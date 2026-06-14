-- LocalSettings: Laufzeit-Settings aus der DB (Keys buchstabengleich zur
-- Python-Version, z.B. AR-Enabled, QR-UpdateHours). Erste Beispiel-Queries;
-- weitere Queries kommen pro Phase, wenn die portierten Funktionen sie brauchen.

-- name: GetLocalSetting :one
SELECT key, value FROM gui_localsettings WHERE key = $1;

-- name: ListLocalSettings :many
SELECT key, value FROM gui_localsettings ORDER BY key;

-- name: UpsertLocalSetting :exec
INSERT INTO gui_localsettings (key, value) VALUES ($1, $2)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- name: GetOrCreateLocalSetting :one
-- Spiegelt Djangos LocalSettings.objects.get_or_create(key, defaults={value}):
-- existiert der Key, wird der vorhandene Wert zurueckgegeben (DO UPDATE auf den
-- bestehenden Wert = wertneutral); sonst wird er mit dem Default angelegt.
INSERT INTO gui_localsettings (key, value) VALUES ($1, $2)
ON CONFLICT (key) DO UPDATE SET value = gui_localsettings.value
RETURNING key, value;

-- name: CreateLocalSettingIfAbsent :exec
-- Spiegelt LocalSettings(key, value).save() nur-wenn-nicht-vorhanden.
INSERT INTO gui_localsettings (key, value) VALUES ($1, $2)
ON CONFLICT (key) DO NOTHING;

-- name: AnyLocalSettingExists :one
-- Spiegelt LocalSettings.objects.filter(key__in=[...]).exists().
SELECT EXISTS (SELECT 1 FROM gui_localsettings WHERE key = ANY($1::varchar[])) AS does_exist;
