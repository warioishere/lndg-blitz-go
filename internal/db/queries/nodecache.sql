-- NodeCache: DB-Backing des Node-Info-Caches (gui/node_cache.py).

-- name: GetNodeCache :one
SELECT pubkey, data, updated_at FROM gui_nodecache WHERE pubkey = $1;

-- name: UpsertNodeCache :exec
-- Spiegelt NodeCache.objects.update_or_create(pubkey, defaults={data, updated_at}).
INSERT INTO gui_nodecache (pubkey, data, updated_at) VALUES ($1, $2, $3)
ON CONFLICT (pubkey) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at;
