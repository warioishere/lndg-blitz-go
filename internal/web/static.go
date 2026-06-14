package web

import "embed"

// staticFS embeds the frontend assets from gui/static/ (W3.CSS + vanilla JS).
// The UI requires no build step — the bundled JS communicates directly with the
// /api/... endpoints.
//
//go:embed static
var staticFS embed.FS
