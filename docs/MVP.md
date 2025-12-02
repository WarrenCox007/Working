# MVP Scope - AI File Organizer

## Must have
- CLI pipeline: scan → extract → embed → classify → suggest; store files/metadata/chunks/tags/actions.
- Search: vector + keyword hybrid, filters (path/mime/date/tags), tags/snippets in results, `--fields` trimming.
- Actions: list/filter (rule/kind/backup/tags/duplicates), apply/undo with safety (allow/deny, conflict policy, trash backups), dedupe/merge actions with `duplicate_of`.
- Dedupe/merge: detect hash duplicates; emit `dedupe` + `merge_duplicate` actions with strategies (trash_duplicate default; replace/keep_duplicate; keep_newest/keep_oldest/keep_original); apply copies tags, deletes duplicate rows, and logs audit.
- Watch: debounced change detection; reprocess changed files; purge deleted files (DB, keyword docs, vectors), audit logs; `immediate_vector_delete` toggle.
- Keyword index: Tantivy hybrid path/keyword search; incremental refresh via dirty markers.
- Config: providers, vectors, safety, rules; OpenAI/LM Studio/noop embeddings; Qdrant adapter.
- Tests: apply/undo/merge basics green; builds without errors.

## Nice to have (not required for MVP)
- GUI (Tauri) for search/actions/duplicates.
- Richer classification (LLM prompt, kNN), OCR/media extraction.
- More extractors (PDF/docx/image EXIF already optional), audio/video.
- Packaging/CI, installers, binaries.
- Expanded tests: watcher, search filters, keyword index, dedupe end-to-end.
- Reporting actual vector/index delete counts (APIs don’t return counts), periodic refresh when immediate vector delete is off.

## Out of scope for MVP
- OS trash restore.
- Cloud file sync, multi-user, auth.
- Advanced UI/visualization.
