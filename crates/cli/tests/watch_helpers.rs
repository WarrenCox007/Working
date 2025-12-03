use cli::watch::keyword_index_docs_for_paths;
use storage::{connect, migrate};

#[tokio::test]
async fn keyword_index_docs_include_snippets() {
    let db = "sqlite://file:watch_helpers?mode=memory&cache=shared";
    let pool = connect(db).await.unwrap();
    migrate(&pool).await.unwrap();

    // Seed a file and a chunk with text_preview.
    sqlx::query(
        "INSERT INTO files(path, size, mtime, ctime, status, first_seen, last_seen) VALUES (?1, 1, 0, 0, 'new', strftime('%s','now'), strftime('%s','now'))",
    )
    .bind("C:/tmp/test.txt")
    .execute(&pool)
    .await
    .unwrap();
    let file_id: i64 = sqlx::query_scalar("SELECT id FROM files WHERE path=?1")
        .bind("C:/tmp/test.txt")
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO chunks(file_id, hash, start, end, text_preview) VALUES (?1, 'h1', 0, 4, 'hello world')",
    )
    .bind(file_id)
    .execute(&pool)
    .await
    .unwrap();

    let (docs, missing) =
        keyword_index_docs_for_paths(db, &vec!["C:/tmp/test.txt".to_string()], None)
            .await
            .unwrap();
    assert!(missing.is_empty());
    let combined = docs
        .iter()
        .filter(|(p, _)| p == "C:/tmp/test.txt")
        .map(|(_, d)| d.clone())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        combined.contains("hello world"),
        "docs should include chunk text"
    );
}
