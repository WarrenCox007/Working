# Architecture

## Pipeline
- Scan: async recursive walk with include/exclude; detect changes via size/mtime/hash.
- Extract: type detect, parse metadata and text snippets.
- Embed: batch + cache by content hash; send to configured provider.
- Classify: heuristics + kNN + optional LLM; produce label/confidence.
- Suggest: derive moves/tags/renames; default dry-run.
- Index: persist in SQLite; vectors in Qdrant/Chroma/pgvector.

## Modules
- core: pipeline + domain logic.
- providers: LLM/embedding adapters.
- storage: SQLite + migrations; vector adapters planned.
- cli: entrypoint for prototype runs.

## Safety
- Dry-run by default; audit trail; sandbox path allow/deny; no destructive ops without confirmation.
