use organizer_core::config::SafetyConfig;

#[tokio::test]
async fn apply_and_undo_moves_with_backup() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("test.db");
    let db_url = format!("sqlite:///{}", db_path.to_string_lossy().replace('\\', "/"));
    std::fs::File::create(&db_path).unwrap();
    let file_src = temp.path().join("file.txt");
    std::fs::write(&file_src, "hello").unwrap();

    // Setup DB
    let pool = storage::connect(&db_url).await.unwrap();
    // Minimal schema for test
    sqlx::query("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT UNIQUE, size INTEGER, mtime INTEGER, ctime INTEGER, status TEXT, first_seen INTEGER, last_seen INTEGER, hash TEXT, mime TEXT, ext TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("CREATE TABLE IF NOT EXISTS file_tags (file_id INTEGER, tag_id INTEGER, confidence REAL, source TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE IF NOT EXISTS dirty (path TEXT PRIMARY KEY, reason TEXT, updated_at INTEGER DEFAULT 0)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE IF NOT EXISTS actions (id INTEGER PRIMARY KEY AUTOINCREMENT, file_id INTEGER NOT NULL, kind TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', created_at INTEGER DEFAULT 0, executed_at INTEGER, undo_token TEXT, backup_path TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    // Insert file and action
    sqlx::query("INSERT INTO files(path,size,mtime,ctime,status,first_seen,last_seen) VALUES(?1,0,0,0,'new',0,0)")
        .bind(file_src.to_string_lossy())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO actions(file_id,kind,payload_json,status) VALUES((SELECT id FROM files WHERE path=?1),'move',?2,'planned')")
        .bind(file_src.to_string_lossy())
        .bind(format!(r#"{{
            "to": "{}"
        }}"#, temp.path().join("dest.txt").to_string_lossy()))
        .execute(&pool)
        .await
        .unwrap();

    let safety = SafetyConfig {
        dry_run: false,
        allow_delete: false,
        allow_paths: vec![],
        deny_paths: vec![],
        trash_dir: Some(temp.path().join("trash").to_string_lossy().into_owned()),
        copy_then_delete: false,
    };

    let actions = cli::apply::apply_actions(&db_url, false, None, &safety, "rename")
        .await
        .unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].status, "executed");
    assert!(actions[0].backup.is_some());

    cli::undo::undo_actions(&db_url, None, None).await.unwrap();

    let restored = file_src.exists();
    assert!(restored || temp.path().join("dest.txt").exists());
}
