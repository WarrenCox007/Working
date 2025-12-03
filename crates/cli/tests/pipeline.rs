use organizer_core::config::{AppConfig, DatabaseConfig, RuleConfig, SafetyConfig, ScanPaths, EmbeddingConfig, VectorConfig, ClassificationConfig, Thresholds};
use organizer_core::scanner;
use organizer_core::extractor;
use organizer_core::classifier;
use organizer_core::suggester;
use std::fs;
use tempfile::tempdir;
use sqlx::Row;

#[tokio::test]
async fn test_full_pipeline() {
    // 1. Setup temporary directories and files
    let temp = tempdir().unwrap();
    let src_dir = temp.path().join("src");
    let dest_dir = temp.path().join("dest");
    let trash_dir = temp.path().join("trash");
    let rules_dir = temp.path().join("rules");
    // Use shared in-memory DB so multiple connections see the same data.
    let db_url = "sqlite://file:pipeline_test?mode=memory&cache=shared".to_string();

    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&dest_dir).unwrap();
    fs::create_dir_all(&trash_dir).unwrap();
    fs::create_dir_all(&rules_dir).unwrap();

    fs::write(src_dir.join("doc.txt"), "This is a document.").unwrap();
    fs::write(src_dir.join("image.jpg"), "fake_image_bytes").unwrap();

    let rule_content = r#" 
        name = "Move Text Documents"
        priority = 1
        enabled = true

        [condition]
        type = "tag"
        tag = "text"

        [[actions]]
        type = "move"
        to = "{dest_dir}/doc.txt"
    "#.replace(
        "{dest_dir}",
        &dest_dir
            .to_string_lossy()
            .replace('\\', "/"),
    );
    fs::write(rules_dir.join("move_docs.toml"), rule_content).unwrap();


    // 2. Setup Config and Database
    let cfg = AppConfig {
        database: DatabaseConfig { path: db_url.clone() },
        scan: ScanPaths {
            include: vec![src_dir.to_string_lossy().into_owned()],
            exclude: vec![],
            hash_mode: Some("fast".to_string()),
        },
        rules: RuleConfig {
            path: Some(rules_dir.to_string_lossy().into_owned()),
        },
        safety: SafetyConfig {
            dry_run: false,
            allow_delete: true,
            allow_paths: vec![temp.path().to_string_lossy().into_owned()],
            deny_paths: vec![],
            trash_dir: Some(trash_dir.to_string_lossy().into_owned()),
            copy_then_delete: false,
            immediate_vector_delete: true,
        },
        embeddings: EmbeddingConfig { provider: "noop".to_string(), model: "".to_string(), batch_size: 1 },
        vectors: VectorConfig { provider: "noop".to_string(), url: None, collection: "".to_string() },
        classification: ClassificationConfig { thresholds: Thresholds { accept: 0.5, review: 0.1 } },
        parsers: organizer_core::config::ParserConfig::default(),
    };

    let pool = storage::connect(&cfg.database.path).await.unwrap();
    storage::migrate(&pool).await.unwrap();

    // 3. Run Pipeline
    let roots = cfg.scan.include.iter().map(std::path::PathBuf::from).collect::<Vec<_>>();
    let hash_mode = organizer_core::scanner::HashMode::from(cfg.scan.hash_mode.as_deref().unwrap_or(""));
    scanner::scan(&roots, &cfg.scan.exclude, &hash_mode, &pool).await.unwrap();

    extractor::run_extractor(&pool, &cfg.parsers).await.unwrap();
    
    // We need a provider registry for the classifier. A NoOp provider is fine for this test.
    let registry = organizer_core::pipeline::build_registry(&cfg);
    classifier::run_classifier_no_knn(&pool, &registry).await.unwrap();
    
    // Load rules from the config file path and insert them into the DB for the suggester
    let rules = organizer_core::rules::load_rules_from_dir(&rules_dir).unwrap();
    for rule in rules {
        sqlx::query("INSERT INTO rules (name, priority, enabled, condition_json, action_json) VALUES (?, ?, ?, ?, ?)")
            .bind(rule.name)
            .bind(rule.priority)
            .bind(rule.enabled)
            .bind(serde_json::to_string(&rule.condition).unwrap())
            .bind(serde_json::to_string(&rule.actions).unwrap())
            .execute(&pool)
            .await
            .unwrap();
    }
    
    suggester::run_suggester(&pool).await.unwrap();

    // 4. Verify suggestion exists
    let planned_action = sqlx::query("SELECT id, kind, payload_json FROM actions WHERE status = 'planned'")
        .fetch_one(&pool)
        .await;
    assert!(planned_action.is_ok());
    let action_row = planned_action.unwrap();
    assert_eq!(action_row.get::<String, _>("kind"), "move");

    // 5. Apply actions
    cli::apply::apply_actions(&cfg.database.path, false, true, None, &cfg.safety, "rename").await.unwrap();
    
    // 6. Assert final state
    assert!(!src_dir.join("doc.txt").exists());
    assert!(dest_dir.join("doc.txt").exists());
    assert!(src_dir.join("image.jpg").exists());
    
    let executed_action = sqlx::query("SELECT id FROM actions WHERE status = 'executed'")
        .fetch_one(&pool)
        .await;
    assert!(executed_action.is_ok());
}
