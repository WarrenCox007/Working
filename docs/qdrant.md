# Qdrant Setup

Minimal steps to prepare a collection for the file organizer:

1. Start Qdrant (docker example):
   ```bash
   docker run -p 6333:6333 -p 6334:6334 -v qdrant_storage:/qdrant/storage qdrant/qdrant
   ```
2. Initialize collection with payload support:
   ```bash
   QDRANT_URL=http://localhost:6333 COLLECTION=organizer_vectors VECTOR_SIZE=1536 \
   ./scripts/qdrant-init.sh
   ```
3. Configure the app (TOML):
   ```toml
   [vectors]
   provider = "qdrant"
   url = "http://localhost:6333"
   collection = "organizer_vectors"
   ```

Payload fields used:
- `path` (string) — file path
- `path_prefixes` (array<string, lowercase>) — cumulative path segments for prefix filtering
- `mime` (string) — mime type
- `ext` (string) — file extension
- `mtime` (int) — last modified timestamp
- `file_id`, `chunk_id` (ints) — DB references

Note: If you change embedding dimensions, set `VECTOR_SIZE` accordingly and align with your embedding model. Remove and recreate the collection if dimension changes.***
