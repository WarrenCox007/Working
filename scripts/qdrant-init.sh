#!/usr/bin/env bash
set -euo pipefail

# Minimal Qdrant collection initializer for this project.
# Usage: QDRANT_URL=http://localhost:6333 COLLECTION=organizer_vectors ./scripts/qdrant-init.sh
# Environment variables:
#   QDRANT_URL   - Qdrant base URL (default: http://localhost:6333)
#   COLLECTION   - collection name (default: organizer_vectors)
#   VECTOR_SIZE  - embedding dimension (default: 1536)
#   QDRANT_API_KEY - optional API key

QDRANT_URL="${QDRANT_URL:-http://localhost:6333}"
COLLECTION="${COLLECTION:-organizer_vectors}"
VECTOR_SIZE="${VECTOR_SIZE:-1536}"

header_auth=()
if [[ -n "${QDRANT_API_KEY:-}" ]]; then
  header_auth=(-H "api-key: ${QDRANT_API_KEY}")
fi

echo "Creating/overwriting collection '${COLLECTION}' at ${QDRANT_URL} (dim=${VECTOR_SIZE})"
curl -sS -X PUT "${QDRANT_URL}/collections/${COLLECTION}" \
  -H "Content-Type: application/json" \
  "${header_auth[@]}" \
  -d @- <<EOF
{
  "vectors": {
    "size": ${VECTOR_SIZE},
    "distance": "Cosine"
  },
  "optimizers_config": {
    "default_segment_number": 1
  },
  "hnsw_config": {
    "m": 16,
    "ef_construct": 100
  },
  "quantization_config": null,
  "wal_config": {
    "wal_capacity_mb": 16,
    "wal_segments_ahead": 2
  },
  "on_disk_payload": true
}
EOF
