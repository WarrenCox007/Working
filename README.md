# AI File Organizer

Early scaffold for a Rust-based AI-powered file and folder organizer (scanner -> extractor -> embeddings -> classifier -> suggestions). This is a workspace skeleton; logic is stubbed for fast iteration.

## Layout
- `crates/core/` – pipeline and domain modules (stubs).
- `crates/providers/` – provider traits for LLMs/embeddings.
- `crates/storage/` – SQLite pool + migrations placeholder.
- `crates/cli/` – prototype CLI entrypoint.
- `config/` – sample config.
- `prompt_templates/` – prompt stubs.
- `docs/` – design notes.

## Next Steps
1. Fill schemas/migrations in `storage` for files/metadata/chunks/tags/actions/rules/audit.
2. Implement scanner with async walk, include/exclude, hashing.
3. Flesh out extractor for text/pdf/docx/image EXIF; add parsers behind feature flags.
4. Implement embedding provider routing (LM Studio, GPT4All, OpenAI) in `providers` and hook into `core::embeddings`.
5. Add vector DB adapter (Qdrant/Chroma/pgvector) and keyword index (Tantivy) wiring.
6. Build classification ensemble (heuristic + kNN + LLM prompt) and action suggester with dry-run logging.
7. Harden safety: recycle-bin moves, undo log, rate limits.
8. Add integration tests in `tests/` and basic benchmarks in `scripts/`.

## Running (once implemented)
```bash
cargo run -p cli
```

## Notes
- Undo relies on recorded backups (via `trash_dir` copies/backup_path). OS trash restore is not supported with the current trash crate.
- Search/actions/apply now support filters: tag filters (`--tags`), keyword index hybrid search (`--keyword-index`), and output field trimming (`--fields path,score,tags,...`) for lighter JSON/text.
