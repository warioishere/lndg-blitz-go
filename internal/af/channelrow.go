package af

import "time"

// ChannelFeeRow holds all per-channel data for one AF evaluation pass.
// Fields are grouped by the stage at which they are populated.
type ChannelFeeRow struct {
	// --- inputs (from channel record) ---
	ChanID               string
	RemotePubkey         string
	Capacity             int64
	LocalBalance         int64 // adjusted by pending_outbound
	RemoteBalance        int64 // adjusted by pending_inbound
	PendingOutbound      int64
	PendingInbound       int64
	LocalFeeRate         int // ppm
	LocalInboundFeeRate  int // ppm (may be negative)
	RemoteFeeRate        int
	RemoteInboundFeeRate int
	ArInTarget           int
	ArMaxCost            int
	AutoRebalance        bool
	FeesUpdated          time.Time
	FlpEnabled           bool
	FlpSafety            int

	// --- computed per-channel metrics ---
	AvgRebalanceCost       *int // nil when no rebalance history is available
	AmtRoutedIn1day        int
	AmtRoutedIn7day        int
	AmtRoutedOut7day       int
	AmtRoutedIn4h          int
	AmtRoutedOut4h         int
	NetRouted7day          float64 // rounded to 1 decimal place
	OutPercent             int
	InPercent              int
	Eligible               bool
	LastForwardOut         *time.Time
	LastForwardIn          *time.Time
	LastForward            *time.Time
	HoursSinceLastForward  float64 // 99999 when no forward exists
	FailedOut1day          int
	FailedOutBoostInterval int
	RevenueAssist7day      float64
	Revenue7day            float64

	// --- merged from peer group aggregation ---
	OverallOutPercent      float64
	GroupNetRouted7day     float64
	TotalFailedOut1day     int
	TotalAmtRoutedIn1day   int
	TotalAmtRoutedIn7day   int
	TotalAmtRoutedOut7day  int
	TotalAmtRoutedIn4h     int
	TotalAmtRoutedOut4h    int
	TotalRevenue7day       float64
	TotalRevenueAssist7day float64
	InboundAdjustment      float64 // group produces int; stored as float for final rate arithmetic

	// --- results ---
	Adjustment            float64
	NewRate               float64
	NewRateBeforeFloor    float64
	AdjustmentBeforeFloor float64
	CostFloor             float64
	NewInboundRate        float64
}

// groupRow holds the aggregated values for all channels belonging to one remote peer.
type groupRow struct {
	RemotePubkey           string
	TotalLocalBalance      int64
	TotalCapacity          int64
	TotalFailedOut1day     int
	TotalAmtRoutedIn1day   int
	TotalAmtRoutedIn7day   int
	TotalAmtRoutedOut7day  int
	TotalAmtRoutedIn4h     int
	TotalAmtRoutedOut4h    int
	TotalRevenue7day       float64
	TotalRevenueAssist7day float64
	OverallOutPercent      float64 // total_local_balance / total_capacity * 100
	GroupNetRouted7day     float64 // net routed normalised by total capacity
	PeerOutTarget          int     // 100 - min(ar_in_target) across the group
	InboundAdjustment      int     // curve or legacy inbound adjustment
}
