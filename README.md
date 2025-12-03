# AI File Organizer

Early scaffold for a Rust-based AI-powered file and folder organizer (scanner -> extractor -> embeddings -> classifier -> suggestions). This is a workspace skeleton; logic is stubbed for fast iteration.

## Layout
- `crates/core/` – pipeline and domain modules (stubs).
- `crates/providers/` – provider traits for LLMs/embeddings.
- `crates/storage/` – SQLite pool + migrations placeholder.
- `crates/cli/` – prototype CLI entrypoint.
- `config/` - sample config.
- `prompt_templates/` - prompt stubs.
- `docs/` - design notes.
- `scripts/` - helper scripts (e.g., Qdrant init).

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
- Duplicates: use `actions --show-duplicates` (or `--duplicates-only`) to list dedupe/merge suggestions with `duplicate_of` and snippets; summaries show duplicate counts; merge_duplicate actions can trash or replace a duplicate.
- Watch mode: `cli watch` monitors paths (defaults to `scan.include`) and re-extracts/re-embeds/re-indexes changed files incrementally, marking the keyword index for refresh. Deletes purge DB rows, keyword index docs, and vectors (controlled by `safety.immediate_vector_delete`), and log purge audits.

## Quick usage examples
- Run pipeline: `cargo run -p cli -- scan` then `... classify` then `... suggest`
- List planned actions: `cargo run -p cli -- suggest --list --fields id,path,kind,duplicate_of,snippet`
- Dedupe review: `cargo run -p cli -- actions --show-duplicates --summary` then apply a specific merge: `cargo run -p cli -- apply --ids 5 --fields id,path,status,backup`
- Search with filters: `cargo run -p cli -- search "invoice" --hybrid --tags finance --fields path,score,duplicate_of,snippet`
- Watch for changes: `cargo run -p cli -- watch --debounce-ms 2000`
- Refresh vector payloads (path prefixes) for all/dirty/specific paths: `cargo run -p cli -- rebuild-vectors --dirty-only` or `--paths /path/a,/path/b`
- Rebuild keyword index: `cargo run -p cli -- rebuild-keyword-index --dirty-only`
- Backfill full file hashes: `cargo run -p cli -- backfill-full-hashes`
- Enable parsers via config (if features enabled):
  ```toml
  [parsers]
  pdf = true
  office = true
  image_meta = true
  ocr = false
  # Optional cap for OCR (bytes)
  max_ocr_bytes = 2097152
  # Optional cap for image metadata decode (bytes)
  max_image_bytes = 20971520
  ```

## Dedupe / merge strategies
- `merge_duplicate` payload supports strategies: `trash_duplicate` (default/keep_original), `replace`/`keep_duplicate` (overwrite survivor), `keep_newest` / `keep_oldest` (decided by mtime), and `keep_original`.
- Dedupe apply copies tags from duplicate to survivor, deletes duplicate DB row, and logs an audit. Vectors/keyword docs are purged on delete (see `safety.immediate_vector_delete`).

## Remaining work
- GUI: Tauri shell to surface search/actions/duplicates with approve/merge controls.
- Classification/AI: richer classifier (LLM prompt, kNN), OCR/media extraction, more parsers.
- Search UX: per-result merge hints, snippets in actions UI, optional GUI filters.
- Packaging/CI: binaries/installers, CI for tests/format, watcher/search/dedupe end-to-end tests.
- Vector/index: better reporting of actual delete counts; optional periodic refresh when `immediate_vector_delete` is false.
