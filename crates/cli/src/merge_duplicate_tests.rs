#[cfg(test)]
mod tests {
    use organizer_core::config::SafetyConfig;
    use std::fs;

    #[tokio::test]
    async fn merge_duplicate_trashes_duplicate_and_copies_tags() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("test.db");
        let db_url = format!("sqlite:///{}", db_path.to_string_lossy().replace('\\', "/"));
        fs::File::create(&db_path).unwrap();

        let orig = temp.path().join("orig.txt");
        let dup = temp.path().join("dup.txt");
        fs::write(&orig, "hello").unwrap();
        fs::write(&dup, "hello").unwrap();

        let pool = storage::connect(&db_url).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT UNIQUE, size INTEGER, mtime INTEGER, ctime INTEGER, status TEXT, first_seen INTEGER, last_seen INTEGER, hash TEXT, mime TEXT, ext TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS file_tags (file_id INTEGER, tag_id INTEGER, confidence REAL, source TEXT)").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS actions (id INTEGER PRIMARY KEY AUTOINCREMENT, file_id INTEGER NOT NULL, kind TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', created_at INTEGER DEFAULT 0, executed_at INTEGER, undo_token TEXT, backup_path TEXT)").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO files(path,size,mtime,ctime,status,first_seen,last_seen) VALUES(?1,0,0,0,'new',0,0)")
            .bind(orig.to_string_lossy())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO files(path,size,mtime,ctime,status,first_seen,last_seen) VALUES(?1,0,0,0,'new',0,0)")
            .bind(dup.to_string_lossy())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tags(name) VALUES('old')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO file_tags(file_id, tag_id, confidence, source) VALUES ((SELECT id FROM files WHERE path=?1),(SELECT id FROM tags WHERE name='old'),1.0,'test')")
            .bind(dup.to_string_lossy())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO actions(file_id,kind,payload_json,status) VALUES((SELECT id FROM files WHERE path=?1),'merge_duplicate',?2,'planned')")
            .bind(dup.to_string_lossy())
            .bind(format!(r#"{{
                "duplicate_of": "{}",
                "strategy": "trash_duplicate"
            }}"#, orig.to_string_lossy()))
            .execute(&pool)
            .await
            .unwrap();

        let safety = SafetyConfig {
            dry_run: false,
            allow_delete: true,
            allow_paths: vec![],
            deny_paths: vec![],
            trash_dir: Some(temp.path().join("trash").to_string_lossy().into_owned()),
            copy_then_delete: false,
        };

        let actions = crate::apply::apply_actions(&db_url, false, None, &safety, "rename")
            .await
            .unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].status, "executed");
        assert!(!dup.exists());
        let tag_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM file_tags WHERE file_id=(SELECT id FROM files WHERE path=?1)")
            .bind(orig.to_string_lossy())
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
        assert!(tag_rows > 0);
    }

    #[tokio::test]
    async fn merge_duplicate_replace_overwrites_target() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("test2.db");
        let db_url = format!("sqlite:///{}", db_path.to_string_lossy().replace('\\', "/"));
        fs::File::create(&db_path).unwrap();

        let orig = temp.path().join("orig.txt");
        let dup = temp.path().join("dup.txt");
        fs::write(&orig, "old").unwrap();
        fs::write(&dup, "new").unwrap();

        let pool = storage::connect(&db_url).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT UNIQUE, size INTEGER, mtime INTEGER, ctime INTEGER, status TEXT, first_seen INTEGER, last_seen INTEGER, hash TEXT, mime TEXT, ext TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS file_tags (file_id INTEGER, tag_id INTEGER, confidence REAL, source TEXT)").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS actions (id INTEGER PRIMARY KEY AUTOINCREMENT, file_id INTEGER NOT NULL, kind TEXT NOT NULL, payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', created_at INTEGER DEFAULT 0, executed_at INTEGER, undo_token TEXT, backup_path TEXT)").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO files(path,size,mtime,ctime,status,first_seen,last_seen) VALUES(?1,0,0,0,'new',0,0)")
            .bind(orig.to_string_lossy())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO files(path,size,mtime,ctime,status,first_seen,last_seen) VALUES(?1,0,0,0,'new',0,0)")
            .bind(dup.to_string_lossy())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tags(name) VALUES('to-merge')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO file_tags(file_id, tag_id, confidence, source) VALUES ((SELECT id FROM files WHERE path=?1),(SELECT id FROM tags WHERE name='to-merge'),1.0,'test')")
            .bind(dup.to_string_lossy())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO actions(file_id,kind,payload_json,status) VALUES((SELECT id FROM files WHERE path=?1),'merge_duplicate',?2,'planned')")
            .bind(dup.to_string_lossy())
            .bind(format!(r#"{{
                "duplicate_of": "{}",
                "strategy": "replace"
            }}"#, orig.to_string_lossy()))
            .execute(&pool)
            .await
            .unwrap();

        let safety = SafetyConfig {
            dry_run: false,
            allow_delete: true,
            allow_paths: vec![],
            deny_paths: vec![],
            trash_dir: None,
            copy_then_delete: false,
        };

        let actions = crate::apply::apply_actions(&db_url, false, None, &safety, "overwrite")
            .await
            .unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].status, "executed");
        let final_content = fs::read_to_string(&orig).unwrap();
        assert_eq!(final_content, "new");
        assert!(!dup.exists());
        let tag_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM file_tags WHERE file_id=(SELECT id FROM files WHERE path=?1)")
            .bind(orig.to_string_lossy())
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
        assert!(tag_rows > 0);
    }
}
