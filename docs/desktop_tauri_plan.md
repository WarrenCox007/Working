# Desktop (Tauri) UI plan

## Screens
- **Search / file list**: keyword-index search input, results table (path, size, mime, modified, tags). Pagination and path prefix filter.
- **File detail**: metadata (mime, size, hashes), snippets/chunks, tags, actions (open in OS, copy path).
- **Maintenance**: buttons for `maintain` and `scan` with output/status, option to target a specific path set.
- **Actions**: list planned actions, apply/undo buttons, dry-run toggle.

## Stack
- **Tauri** shell with a lightweight front-end (Vite + React or Svelte). Keeps everything local.
- **Rust commands** call into existing crates (reuse `organizer_core` / `storage` / CLI logic).

## Backend commands to expose
- `run_maintain(paths?: Vec<String>)` → wraps CLI `maintain`.
- `run_scan(paths?: Vec<String>)` → wraps `scan`.
- `search_keyword(query: String, limit: usize, path_prefix: Option<String>)` → uses keyword index.
- `list_recent(limit: usize)` → latest files from DB.
- `apply_actions(ids?: Vec<i64>, dry_run: bool, force: bool)` → wraps `apply`.
- `undo_actions(ids?: Vec<i64>)` → wraps `undo`.

## Proposed folder layout
```
desktop/
  package.json           # front-end deps (Vite + React/Svelte)
  src/                   # UI
  src-tauri/
    Cargo.toml           # Tauri crate depends on organizer-core and storage
    src/main.rs          # Tauri commands -> core APIs
    tauri.conf.json
```

## Notes
- Keep config local: reuse `config/local.toml` or expose a UI form to edit scan/include/exclude.
- Start vectors as `noop`; keyword index is already working. Later we can enable Qdrant/OpenAI/LM Studio.
- Prefer IPC commands to shelling out to the CLI to avoid process management on Windows.
