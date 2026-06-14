package jobs

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// nodeCacheQ is the DB subset required by lnd.GetNodeInfoCached.
type nodeCacheQ interface {
	GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error)
	GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error)
	UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error
}

// nodeInfoClient is the LND subset used for node cache lookups.
type nodeInfoClient interface {
	GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error)
}

// nodeAlias returns the alias for the given pubkey, or an empty string on error.
func nodeAlias(ctx context.Context, q nodeCacheQ, client nodeInfoClient, pubkey string) string {
	info, err := lnd.GetNodeInfoCached(ctx, q, client, pubkey)
	if err != nil || info.GetNode() == nil {
		return ""
	}
	return info.GetNode().GetAlias()
}

// roundTo rounds x to d decimal places using half-to-even (banker's) rounding.
func roundTo(x float64, d int) float64 {
	p := math.Pow(10, float64(d))
	return math.RoundToEven(x*p) / p
}

// policyClient is the LND subset required by fee-management jobs.
type policyClient interface {
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	UpdateChannelPolicy(ctx context.Context, in *lnrpc.PolicyUpdateRequest, opts ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error)
}

// settingsQuerier bundles the local-settings DB access used across jobs.
type settingsQuerier interface {
	GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error)
	GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error)
}

func ts(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }

// channelPoint constructs an lnrpc.ChannelPoint from a funding txid string and output index.
func channelPoint(fundingTxid string, outputIndex int32) *lnrpc.ChannelPoint {
	return &lnrpc.ChannelPoint{
		FundingTxid: &lnrpc.ChannelPoint_FundingTxidStr{FundingTxidStr: fundingTxid},
		OutputIndex: uint32(outputIndex),
	}
}

// versionFloat parses the first four characters of a LND version string as a float
// (e.g. "0.21.0-beta" -> 0.21).
func versionFloat(version string) float64 {
	s := version
	if len(s) > 4 {
		s = s[:4]
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// settingGateEnabled reads or creates a boolean gate setting. Returns true when
// the stored value parses to a non-zero integer; creates the key with value "0"
// if it does not exist.
func settingGateEnabled(ctx context.Context, q settingsQuerier, key string) (bool, error) {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: "0"})
	if err != nil {
		return false, err
	}
	n, perr := strconv.Atoi(row.Value)
	if perr != nil {
		return false, perr
	}
	return n != 0, nil
}

// getRequiredInt returns the integer value of an existing setting key.
// Returns an error if the key is missing or the value cannot be parsed.
func getRequiredInt(ctx context.Context, q settingsQuerier, key string) (int, error) {
	row, err := q.GetLocalSetting(ctx, key)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(row.Value)
}

func getRequiredFloat(ctx context.Context, q settingsQuerier, key string) (float64, error) {
	row, err := q.GetLocalSetting(ctx, key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(row.Value, 64)
}

// getOrCreateInt reads the setting key, creating it with defStr if absent, then
// returns the value parsed as an integer. A parse error is propagated.
func getOrCreateInt(ctx context.Context, q settingsQuerier, key, defStr string) (int, error) {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: defStr})
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(row.Value)
}

func getOrCreateFloat(ctx context.Context, q settingsQuerier, key, defStr string) (float64, error) {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: defStr})
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(row.Value, 64)
}

// isNoRows reports whether err is a pgx.ErrNoRows error.
func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// grpcErrMsg extracts the message from a gRPC status error.
// For non-gRPC errors it returns the full error string.
func grpcErrMsg(err error) string {
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

// pyFloat formats a float64 as a string: integers are rendered as "N.0",
// other values use the shortest round-trip representation.
func pyFloat(v float64) string {
	if v == math.Trunc(v) && !math.IsInf(v, 0) && math.Abs(v) < 1e16 {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// settingEqualsNoCreate returns true when the setting key exists and its value
// equals val. Returns false without error when the key is absent.
func settingEqualsNoCreate(ctx context.Context, q settingsQuerier, key, val string) (bool, error) {
	row, err := q.GetLocalSetting(ctx, key)
	if isNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return row.Value == val, nil
}

// getOptionalInt returns the integer value of a setting key, or def when the key
// is missing or its value cannot be parsed. The key is never created.
func getOptionalInt(ctx context.Context, q settingsQuerier, key string, def int) (int, error) {
	row, err := q.GetLocalSetting(ctx, key)
	if isNoRows(err) {
		return def, nil
	}
	if err != nil {
		return def, nil
	}
	n, perr := strconv.Atoi(row.Value)
	if perr != nil {
		return def, nil
	}
	return n, nil
}
