use crate::config::ParserConfig;
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
    pub image_meta: Option<ImageMeta>,
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub start: usize,
    pub end: usize,
    pub text: String,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct ImageMeta {
    pub width: u32,
    pub height: u32,
    pub format: Option<String>,
}

pub async fn run_extractor(pool: &SqlitePool, parsers: &ParserConfig) -> anyhow::Result<()> {
    let dirty_files =
        sqlx::query_as::<_, File>("SELECT f.* FROM files f JOIN dirty d ON f.path = d.path")
            .fetch_all(pool)
            .await?;

    for file in dirty_files {
        let path = PathBuf::from(&file.path);
        // Short-circuit if file hash matches current fast/full hash (from scanner).
        let stored_full = file.full_hash.as_ref().or(file.hash.as_ref());
        let stored_fast = file.fast_hash.as_ref().or(file.hash.as_ref());
        if let Some(stored) = stored_full.or(stored_fast) {
            let current = if stored_full.is_some() {
                full_hash(&path)
            } else {
                fast_hash(&path)
            };
            if let Ok(h) = current {
                if &h == stored {
                    let mut tx = pool.begin().await?;
                    let _ = sqlx::query("DELETE FROM dirty WHERE path = ?")
                        .bind(&file.path)
                        .execute(&mut *tx)
                        .await;
                    tx.commit().await?;
                    continue;
                }
            }
        }

        let extracted = extract(&path, parsers).await?;

        // Compare chunks to skip unchanged ones.
        let mut tx = pool.begin().await?;
        let existing_hashes: Vec<String> = sqlx::query_scalar(
            "SELECT hash FROM chunks WHERE file_id = ?",
        )
        .bind(file.id)
        .fetch_all(&mut *tx)
        .await
        .unwrap_or_default();
        let existing: std::collections::HashSet<String> =
            existing_hashes.into_iter().collect();

        let mut changed = false;

        // Remove chunks that are no longer present.
        let new_hashes: std::collections::HashSet<String> =
            extracted.chunks.iter().map(|c| c.hash.clone()).collect();
        let to_delete: Vec<String> = existing
            .difference(&new_hashes)
            .cloned()
            .collect();
        if !to_delete.is_empty() {
            let mut qb =
                sqlx::QueryBuilder::new("DELETE FROM chunks WHERE file_id = ");
            qb.push_bind(file.id);
            qb.push(" AND hash IN (");
            let mut sep = qb.separated(", ");
            for h in &to_delete {
                sep.push_bind(h);
            }
            sep.push_unseparated(")");
            let _ = qb.build().execute(&mut *tx).await;
            changed = true;
        }

        // Insert new chunks only (keep existing unchanged).
        for chunk in &extracted.chunks {
            if existing.contains(&chunk.hash) {
                continue;
            }
            sqlx::query(
                "INSERT INTO chunks (file_id, hash, start, end, text_preview) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(file.id)
            .bind(&chunk.hash)
            .bind(chunk.start as i64)
            .bind(chunk.end as i64)
            .bind(&chunk.text)
            .execute(&mut *tx)
            .await?;
            changed = true;
        }

        if let Some(exif_data) = extracted.exif {
            for (key, value) in exif_data {
                sqlx::query(
                    "INSERT INTO metadata (file_id, key, value, source) VALUES (?, ?, ?, 'exif')",
                )
                .bind(file.id)
                .bind(key)
                .bind(value)
                .execute(&mut *tx)
                .await?;
            }
        }

        if let Some(img) = extracted.image_meta {
            sqlx::query(
                "INSERT INTO metadata (file_id, key, value, source) VALUES (?, ?, ?, 'image')",
            )
            .bind(file.id)
            .bind("width")
            .bind(img.width.to_string())
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO metadata (file_id, key, value, source) VALUES (?, ?, ?, 'image')",
            )
            .bind(file.id)
            .bind("height")
            .bind(img.height.to_string())
            .execute(&mut *tx)
            .await?;
            if let Some(fmt) = img.format {
                sqlx::query(
                    "INSERT INTO metadata (file_id, key, value, source) VALUES (?, ?, ?, 'image')",
                )
                .bind(file.id)
                .bind("format")
                .bind(fmt)
                .execute(&mut *tx)
                .await?;
            }
        }

        // Update file row with derived mime/hash
        // Keep existing hashes if already set; else backfill fast_hash/hash with current fast hash.
        let current_fast = fast_hash(&path).ok();
        let mut q = sqlx::QueryBuilder::new("UPDATE files SET mime = ");
        q.push_bind(&extracted.mime);
        if let Some(h) = &current_fast {
            q.push(", fast_hash = COALESCE(fast_hash, ");
            q.push_bind(h);
            q.push(")");
            q.push(", hash = COALESCE(hash, ");
            q.push_bind(h);
            q.push(")");
        }
        q.push(" WHERE id = ");
        q.push_bind(file.id);
        let _ = q.build().execute(&mut *tx).await?;

        // Mark clean after processing; downstream embed/classify already skip unchanged chunks.
        if changed || !extracted.chunks.is_empty() {
            let _ = sqlx::query("DELETE FROM dirty WHERE path = ?")
                .bind(&file.path)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
    }

    Ok(())
}

pub async fn extract(path: &PathBuf, parsers: &ParserConfig) -> anyhow::Result<ExtractedMetadata> {
    let meta = fs::metadata(&path)?;
    let size = meta.len();
    let mime = guess_mime(&path);
    let exif_meta = if cfg!(feature = "exif") {
        extract_exif(&path)
    } else {
        None
    };
    // Skip extremely large files for now to avoid heavy extraction.
    const MAX_BYTES: u64 = 10 * 1024 * 1024; // 10MB cap for text extraction
    if size > MAX_BYTES && !is_texty(&mime) {
        return Ok(ExtractedMetadata {
            path: path.clone(),
            mime,
            size,
            chunks: Vec::new(),
            exif: None,
            image_meta: None,
        });
    }
    let mut image_meta = None;
    let chunks = if is_texty(&mime) {
        read_text_chunks(&path, 64 * 1024, 2048)?
    } else if cfg!(feature = "pdf") && parsers.pdf && mime.as_deref() == Some("application/pdf") {
        pdf_text(&path)?
    } else if cfg!(feature = "office") && parsers.office && is_office(&mime) {
        office_text(&path)?
    } else if mime.as_deref().map(|m| m.starts_with("image/")).unwrap_or(false) {
        if cfg!(feature = "image-meta") && parsers.image_meta {
            image_meta = read_image_meta(&path, parsers.max_image_bytes);
        }
        let chunks_from_ocr = {
            #[cfg(feature = "ocr")]
            {
                let within_cap = parsers
                    .max_ocr_bytes
                    .map(|limit| size <= limit)
                    .unwrap_or(true);
                if parsers.ocr && within_cap {
                    if let Some(text) = ocr_image_text(&path) {
                        chunk_text(&text, 2048)
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            }
            #[cfg(not(feature = "ocr"))]
            {
                Vec::new()
            }
        };
        chunks_from_ocr
    } else {
        Vec::new()
    };

    Ok(ExtractedMetadata {
        path: path.clone(),
        mime,
        size,
        chunks,
        exif: exif_meta,
        image_meta,
    })
}

fn guess_mime(path: &PathBuf) -> Option<String> {
    // Try content sniffing first (up to 8KB).
    if let Ok(mut file) = fs::File::open(path) {
        let mut buf = vec![0u8; 8192];
        if let Ok(n) = file.read(&mut buf) {
            if let Some(kind) = infer::get(&buf[..n]) {
                return Some(kind.mime_type().to_string());
            }
        }
    }
    // Fallback to extension-based guess.
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| match ext.to_lowercase().as_str() {
            "txt" | "md" | "log" => "text/plain",
            "rs" | "py" | "js" | "ts" | "json" | "toml" | "yaml" | "yml" => "text/plain",
            "pdf" => "application/pdf",
            "doc" | "docx" => "application/msword",
            "ppt" | "pptx" => "application/vnd.ms-powerpoint",
            "xls" | "xlsx" => "application/vnd.ms-excel",
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

#[cfg(feature = "office")]
fn is_office(mime: &Option<String>) -> bool {
    matches!(
        mime.as_deref(),
        Some("application/msword")
            | Some("application/vnd.ms-powerpoint")
            | Some("application/vnd.ms-excel")
            | Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            | Some("application/vnd.openxmlformats-officedocument.presentationml.presentation")
            | Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    )
}

#[cfg(not(feature = "office"))]
fn is_office(_mime: &Option<String>) -> bool {
    false
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
    Ok(chunk_text(&text, chunk_size))
}

fn fast_hash(path: &PathBuf) -> anyhow::Result<String> {
    use std::io::Read;
    const BYTES: usize = 64 * 1024;
    let mut file = fs::File::open(path)?;
    let mut buf = vec![0u8; BYTES];
    let n = file.read(&mut buf)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&buf[..n]);
    Ok(hasher.finalize().to_hex().to_string())
}

fn full_hash(path: &PathBuf) -> anyhow::Result<String> {
    use std::io::Read;
    let mut file = fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(feature = "pdf")]
fn pdf_text(path: &PathBuf) -> anyhow::Result<Vec<Chunk>> {
    let content = pdf_extract::extract_text(path)?;
    Ok(chunk_text(&content, 2048))
}

#[cfg(not(feature = "pdf"))]
fn pdf_text(_path: &PathBuf) -> anyhow::Result<Vec<Chunk>> {
    Ok(Vec::new())
}

#[cfg(feature = "office")]
fn office_text(path: &PathBuf) -> anyhow::Result<Vec<Chunk>> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    if ext == "docx" {
        if let Ok(mut file) = std::fs::File::open(path) {
            use zip::read::ZipArchive;
            if let Ok(mut zip) = ZipArchive::new(&mut file) {
                if let Ok(mut doc) = zip.by_name("word/document.xml") {
                    let text = read_xml_text(&mut doc).unwrap_or_default();
                    return Ok(chunk_text(&text, 2048));
                }
            }
        }
    }
    if ext == "pptx" {
        if let Ok(mut file) = std::fs::File::open(path) {
            use zip::read::ZipArchive;
            if let Ok(mut zip) = ZipArchive::new(&mut file) {
                let mut combined = String::new();
                for i in 1..=50 {
                    let name = format!("ppt/slides/slide{}.xml", i);
                    if let Ok(mut slide) = zip.by_name(&name) {
                        let text = read_xml_text(&mut slide).unwrap_or_default();
                        combined.push_str(&text);
                        combined.push('\n');
                    } else {
                        break;
                    }
                }
                if !combined.is_empty() {
                    return Ok(chunk_text(&combined, 2048));
                }
            }
        }
    }
    if ext == "xlsx" {
        // For spreadsheets, pull sheet names and first rows as text.
        if let Ok(mut workbook) = calamine::open_workbook_auto(path) {
            let mut text = String::new();
            for sheet in workbook.sheet_names().to_owned() {
                if let Ok(range) = workbook.worksheet_range(&sheet) {
                    text.push_str(&sheet);
                    text.push('\n');
                    for row in range.rows().take(10) {
                        for cell in row.iter().take(10) {
                            use calamine::DataType;
                            match cell {
                                DataType::String(s) => {
                                    text.push_str(s);
                                    text.push(' ');
                                }
                                DataType::Float(f) => {
                                    text.push_str(&f.to_string());
                                    text.push(' ');
                                }
                                DataType::Int(i) => {
                                    text.push_str(&i.to_string());
                                    text.push(' ');
                                }
                                _ => {}
                            }
                        }
                        text.push('\n');
                    }
                }
            }
            return Ok(chunk_text(&text, 2048));
        }
    }
    Ok(Vec::new())
}

#[cfg(not(feature = "office"))]
fn office_text(_path: &PathBuf) -> anyhow::Result<Vec<Chunk>> {
    Ok(Vec::new())
}

fn chunk_hash(start: usize, end: usize, data: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&start.to_le_bytes());
    hasher.update(&end.to_le_bytes());
    hasher.update(data);
    hasher.finalize().to_hex().to_string()
}

fn chunk_text(text: &str, max_len: usize) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let slice_end = (start + max_len).min(text.len());
        let slice = &text[start..slice_end];
        // Try to find a boundary before max_len.
        let boundary = slice
            .rmatch_indices(|c: char| c == '.' || c == '!' || c == '?' || c == '\n')
            .next()
            .map(|(idx, ch)| idx + ch.len());
        let boundary = boundary.or_else(|| slice.rfind(' '));
        let end = boundary.map(|b| start + b).unwrap_or(slice_end);
        let chunk_text = text[start..end].to_string();
        let hash = chunk_hash(start, end, chunk_text.as_bytes());
        chunks.push(Chunk {
            start,
            end,
            text: chunk_text,
            hash,
        });
        start = end;
    }
    chunks
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

#[cfg(feature = "ocr")]
fn ocr_image_text(path: &PathBuf) -> Option<String> {
    use leptess::LepTess;
    let mut lt = LepTess::new(None, "eng").ok()?;
    lt.set_image(path).ok()?;
    lt.get_utf8_text().ok()
}

#[cfg(feature = "image-meta")]
fn read_image_meta(path: &PathBuf, max_bytes: Option<u64>) -> Option<ImageMeta> {
    let meta = std::fs::metadata(path).ok()?;
    let cap = max_bytes.unwrap_or(20 * 1024 * 1024);
    if meta.len() > cap {
        return None;
    }
    let img = image::open(path).ok()?;
    let (w, h) = img.dimensions();
    let fmt = match img {
        image::DynamicImage::ImageLuma8(_)
        | image::DynamicImage::ImageLumaA8(_)
        | image::DynamicImage::ImageLuma16(_)
        | image::DynamicImage::ImageLumaA16(_)
        | image::DynamicImage::ImageRgb8(_)
        | image::DynamicImage::ImageRgba8(_)
        | image::DynamicImage::ImageRgb16(_)
        | image::DynamicImage::ImageRgba16(_) => Some("bitmap".to_string()),
        _ => None,
    };
    Some(ImageMeta {
        width: w,
        height: h,
        format: fmt,
    })
}

#[cfg(not(feature = "image-meta"))]
fn read_image_meta(_path: &PathBuf, _max_bytes: Option<u64>) -> Option<ImageMeta> {
    None
}

#[cfg_attr(not(feature = "office"), allow(dead_code))]
fn read_xml_text<R: std::io::BufRead>(reader: R) -> anyhow::Result<String> {
    use quick_xml::events::Event;
    use quick_xml::Reader;
    let mut xml = Reader::from_reader(reader);
    xml.trim_text(true);
    let mut buf = Vec::new();
    let mut out = String::new();
    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Text(t)) => {
                let txt = t
                    .unescape()
                    .unwrap_or_else(|_| std::borrow::Cow::Owned(String::from_utf8_lossy(t.as_ref()).to_string()));
                out.push_str(txt.as_ref());
                out.push(' ');
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(out)
}
