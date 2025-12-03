use storage::models::File;
use sqlx::SqlitePool;
use std::fs;
use std::io::Read;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ExtractedMetadata {
    pub path: PathBuf,
    pub mime: Option<String>,
    pub size: u64,
    pub chunks: Vec<Chunk>,
    pub exif: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub start: usize,
    pub end: usize,
    pub text: String,
    pub hash: String,
}

pub async fn run_extractor(pool: &SqlitePool) -> anyhow::Result<()> {
    let dirty_files =
        sqlx::query_as::<_, File>("SELECT f.* FROM files f JOIN dirty d ON f.path = d.path")
            .fetch_all(pool)
            .await?;

    for file in dirty_files {
        let path = PathBuf::from(&file.path);
        let extracted = extract(&path).await?;

        // Clear old data and insert new
        let mut tx = pool.begin().await?;

        sqlx::query!("DELETE FROM chunks WHERE file_id = ?", file.id)
            .execute(&mut *tx)
            .await?;
        sqlx::query!("DELETE FROM metadata WHERE file_id = ?", file.id)
            .execute(&mut *tx)
            .await?;

        for chunk in extracted.chunks {
            sqlx::query!(
                "INSERT INTO chunks (file_id, hash, start, end, text_preview) VALUES (?, ?, ?, ?, ?)",
                file.id,
                chunk.hash,
                chunk.start,
                chunk.end,
                chunk.text
            )
            .execute(&mut *tx)
            .await?;
        }

        if let Some(exif_data) = extracted.exif {
            for (key, value) in exif_data {
                sqlx::query!(
                    "INSERT INTO metadata (file_id, key, value, source) VALUES (?, ?, ?, 'exif')",
                    file.id,
                    key,
                    value
                )
                .execute(&mut *tx)
                .await?;
            }
        }

        // TODO: Update mime type on files table
        // sqlx::query!("UPDATE files SET mime = ? WHERE id = ?", extracted.mime, file.id);

        sqlx::query!("DELETE FROM dirty WHERE path = ?", file.path)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
    }

    Ok(())
}

pub async fn extract(path: &PathBuf) -> anyhow::Result<ExtractedMetadata> {
    let meta = fs::metadata(&path)?;
    let size = meta.len();
    let mime = guess_mime(&path);
    let exif_meta = if cfg!(feature = "exif") {
        extract_exif(&path)
    } else {
        None
    };
    let chunks = if is_texty(&mime) {
        read_text_chunks(&path, 64 * 1024, 2048)?
    } else if cfg!(feature = "pdf") && mime.as_deref() == Some("application/pdf") {
        pdf_text(&path)?
    } else {
        Vec::new()
    };

    Ok(ExtractedMetadata {
        path: path.clone(),
        mime,
        size,
        chunks,
        exif: exif_meta,
    })
}

fn guess_mime(path: &PathBuf) -> Option<String> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| match ext.to_lowercase().as_str() {
            "txt" | "md" | "log" => "text/plain",
            "rs" | "py" | "js" | "ts" | "json" | "toml" | "yaml" | "yml" => "text/plain",
            "pdf" => "application/pdf",
            "doc" | "docx" => "application/msword",
            "jpg" | "jpeg" => "image/jpeg",
            "png" => "image/png",
            _ => "application/octet-stream",
        })
        .map(|s| s.to_string())
}

fn is_texty(mime: &Option<String>) -> bool {
    mime.as_deref()
        .map(|m| m.starts_with("text/") || m.contains("json") || m.contains("yaml"))
        .unwrap_or(false)
}

fn read_text_chunks(
    path: &PathBuf,
    max_bytes: usize,
    chunk_size: usize,
) -> anyhow::Result<Vec<Chunk>> {
    let mut file = fs::File::open(path)?;
    let mut buf = vec![0u8; max_bytes];
    let n = file.read(&mut buf)?;
    let text = String::from_utf8_lossy(&buf[..n]).to_string();
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let end = (start + chunk_size).min(text.len());
        let slice = text[start..end].to_string();
        let hash = blake3::hash(slice.as_bytes()).to_hex().to_string();
        chunks.push(Chunk {
            start,
            end,
            text: slice,
            hash,
        });
        start = end;
    }
    Ok(chunks)
}

#[cfg(feature = "pdf")]
fn pdf_text(path: &PathBuf) -> anyhow::Result<Vec<Chunk>> {
    let content = pdf_extract::extract_text(path)?;
    let mut chunks = Vec::new();
    let mut start = 0;
    let chunk_size = 2048;
    while start < content.len() {
        let end = (start + chunk_size).min(content.len());
        let slice = content[start..end].to_string();
        let hash = blake3::hash(slice.as_bytes()).to_hex().to_string();
        chunks.push(Chunk {
            start,
            end,
            text: slice,
            hash,
        });
        start = end;
    }
    Ok(chunks)
}

#[cfg(not(feature = "pdf"))]
fn pdf_text(_path: &PathBuf) -> anyhow::Result<Vec<Chunk>> {
    Ok(Vec::new())
}

#[cfg(feature = "exif")]
fn extract_exif(path: &PathBuf) -> Option<std::collections::HashMap<String, String>> {
    use kamadak_exif as exif;
    let file = std::fs::File::open(path).ok()?;
    let mut bufreader = std::io::BufReader::new(file);
    let exifreader = exif::Reader::new();
    let exif = exifreader.read_from_container(&mut bufreader).ok()?;
    let mut map = std::collections::HashMap::new();
    for f in exif.fields() {
        map.insert(format!("{}", f.tag), f.display_value().to_string());
    }
    Some(map)
}

#[cfg(not(feature = "exif"))]
fn extract_exif(_path: &PathBuf) -> Option<std::collections::HashMap<String, String>> {
    None
}
