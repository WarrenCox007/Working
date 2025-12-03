use organizer_core::config::SafetyConfig;
use std::fs;
use sqlx::Row;

#[tokio::test]
async fn apply_and_undo_moves_with_backup() {
    let temp = tempfile::tempdir().unwrap();
    // Use shared in-memory DB so multiple connections see the same data.
    let db_url = "sqlite://file:apply_undo?mode=memory&cache=shared".to_string();
    let file_src = temp.path().join("file.txt");
    let dest_path = temp.path().join("dest.txt");
    fs::write(&file_src, "hello").unwrap();

    // Setup DB
    let pool = storage::connect(&db_url).await.unwrap();
    // Run real migrations instead of creating tables manually
    storage::migrate(&pool).await.unwrap();

    // Insert file and action
    let file_id = sqlx::query("INSERT INTO files(path,size,mtime,ctime,status,first_seen,last_seen) VALUES(?1,5,0,0,'new',0,0) RETURNING id")
        .bind(file_src.to_string_lossy())
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>("id");

    sqlx::query("INSERT INTO actions(file_id,kind,payload_json,status) VALUES(?1, 'move', ?2, 'planned')")
        .bind(file_id)
        .bind(format!(r#"{{"to": "{}"}}"#, dest_path.to_string_lossy()))
        .execute(&pool)
        .await
        .unwrap();

    let safety = SafetyConfig {
        dry_run: false,
        allow_delete: false,
        allow_paths: vec![temp.path().to_string_lossy().into_owned()],
        deny_paths: vec![],
        trash_dir: Some(temp.path().join("trash").to_string_lossy().into_owned()),
        copy_then_delete: false,
        immediate_vector_delete: true,
    };

    let actions = cli::apply::apply_actions(&db_url, false, true, None, &safety, "rename")
        .await
        .unwrap();

    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].status, "executed");
    assert!(actions[0].backup.is_some());
    assert!(!file_src.exists());
    let backup_path = actions[0].backup.as_ref().map(|s| std::path::PathBuf::from(s));
    assert!(
        dest_path.exists() || backup_path.as_ref().map(|p| p.exists()).unwrap_or(false),
        "expected moved file at dest or backup location"
    );

    cli::undo::undo_actions(&db_url, None, None).await.unwrap();

    // Assert that the original file is restored and the destination is gone.
    assert!(file_src.exists());
    assert!(!dest_path.exists());
}
