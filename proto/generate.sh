#!/usr/bin/env bash
# Regeneriert die vendored LND-Protos nach internal/lnd/lnrpc.
#
# Quelle: lightningnetwork/lnd @ Tag v0.21.0-beta (entspricht der LND-Version,
# die der Node laeuft). Die .proto-Dateien sind unter proto/lnrpc/ vendored,
# damit die Regenerierung nicht von einem externen lnd-Checkout abhaengt.
#
# Voraussetzungen (alle via `go install`, kein protoc/sudo noetig):
#   go install github.com/bufbuild/buf/cmd/buf@latest
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#   PATH muss $(go env GOPATH)/bin enthalten.
set -euo pipefail
cd "$(dirname "$0")/lnrpc"
buf generate \
  --path lightning.proto \
  --path routerrpc/router.proto \
  --path signrpc/signer.proto \
  --path walletrpc/walletkit.proto \
  --path wtclientrpc/wtclient.proto
echo "Protos regeneriert nach internal/lnd/lnrpc."
