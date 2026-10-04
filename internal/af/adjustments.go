package af

import "math"

// clampFlow clamps val to the [-maxNetFlow, maxNetFlow] range.
func clampFlow(val float64) float64 {
	if val > maxNetFlow {
		return maxNetFlow
	}
	if val < -maxNetFlow {
		return -maxNetFlow
	}
	return val
}

// clampStep clamps val to [-MaxStep, MaxStep] and truncates toward zero on conversion.
func (s *Settings) clampStep(val float64) int {
	if val > float64(s.MaxStep) {
		return s.MaxStep
	}
	if val < float64(-s.MaxStep) {
		return -s.MaxStep
	}
	return int(val)
}

// sign returns 1, -1, or 0 depending on the sign of d.
func sign(d float64) float64 {
	if d > 0 {
		return 1
	}
	if d < 0 {
		return -1
	}
	return 0
}

// computeCurveOutboundAdjustment calculates the curve-mode outbound fee adjustment
// for a single channel row.
func (s *Settings) computeCurveOutboundAdjustment(r *ChannelFeeRow) int {
	chTarget := 100 - r.ArInTarget
	deviation := float64(chTarget-r.OutPercent) / 100.0
	adj := float64(s.Intensity) * sign(deviation) * math.Pow(math.Abs(deviation), s.Exponent)

	// Suppress fee increases when peer oRate is too high.
	if s.PeerRateCheck && s.PeerRateLimit > 0 && adj > 0 {
		if r.RemoteFeeRate >= s.PeerRateLimit {
			return 0
		}
	}
	// Scale fee decreases.
	if adj < 0 && s.DownScale != 1.0 {
		adj *= s.DownScale
	}
	if s.FlowWeight > 0 && r.NetRouted7day != 0 {
		netFlowRatio := r.NetRouted7day / maxNetFlow
		netFlowRatio = math.Max(-1.0, math.Min(1.0, netFlowRatio))
		if (adj > 0 && netFlowRatio > 0) || (adj < 0 && netFlowRatio < 0) {
			adj *= 1 + s.FlowWeight*math.Abs(netFlowRatio)
		}
	}
	clamped := math.Max(float64(-s.MaxStep), math.Min(float64(s.MaxStep), adj))
	return int(math.RoundToEven(clamped))
}

// computeCurveInboundAdjustment calculates the curve-mode inbound fee adjustment
// for a peer group row.
func (s *Settings) computeCurveInboundAdjustment(g *groupRow) int {
	peerTgt := float64(g.PeerOutTarget)
	deviation := (peerTgt - g.OverallOutPercent) / 100.0
	adj := float64(s.InboundIntensity) * sign(deviation) * math.Pow(math.Abs(deviation), s.Exponent)
	clamped := math.Max(float64(-s.MaxStep), math.Min(float64(s.MaxStep), adj))
	return int(math.RoundToEven(clamped))
}

// computeInboundAdjustment calculates the legacy-mode inbound fee adjustment
// for a peer group row.
func (s *Settings) computeInboundAdjustment(g *groupRow) int {
	var adj float64
	switch {
	case g.OverallOutPercent <= float64(s.LowLiqLimit):
		adj = 0
	case g.OverallOutPercent < float64(s.ExcessLimit):
		if g.TotalAmtRoutedIn7day+g.TotalAmtRoutedOut7day == 0 {
			adj = float64(7 * s.Multiplier)
		} else if g.GroupNetRouted7day > 1 {
			flow := clampFlow(g.GroupNetRouted7day)
			scale := 1 + flow*s.FlowScale
			adj = (-5 * float64(s.Multiplier) * highFlowFactor) * scale
		} else {
			adj = 0
		}
	default:
		if g.TotalAmtRoutedIn7day+g.TotalAmtRoutedOut7day == 0 {
			adj = float64(12 * s.Multiplier)
		} else if g.GroupNetRouted7day < -1 && g.TotalRevenueAssist7day > g.TotalRevenue7day*10 {
			flow := math.Abs(clampFlow(g.GroupNetRouted7day))
			scale := 1 + flow*s.FlowScale
			adj = 12 * float64(s.Multiplier) * highFlowFactor * scale
		} else {
			adj = 0
		}
	}
	return s.clampStep(adj)
}

// computeOutboundAdjustment calculates the legacy-mode outbound fee adjustment
// for a single channel row.
func (s *Settings) computeOutboundAdjustment(r *ChannelFeeRow) int {
	// When HTLC boost conditions are met, skip normal AF logic.
	if s.HtlcBoostAmount > 0 &&
		r.OutPercent <= s.LowLiqLimit &&
		r.FailedOutBoostInterval >= s.HtlcBoostThreshold {
		return s.HtlcBoostAmount
	}

	if r.OutPercent <= s.LowLiqLimit {
		if s.PeerRateCheck && s.PeerRateLimit > 0 && r.RemoteFeeRate >= s.PeerRateLimit {
			hasHTLCConditions := s.HtlcBoostAmount > 0 && r.FailedOutBoostInterval >= s.HtlcBoostThreshold
			if !(s.BypassPeerRateOnHTLC && hasHTLCConditions) {
				return 0
			}
		}
		boost := 0.0
		if s.BoostArOnly && r.AutoRebalance {
			deficit := s.LowLiqLimit - r.OutPercent
			if deficit < 0 {
				deficit = 0
			}
			boost = float64(deficit) / float64(max1(s.LowLiqLimit)) * s.LowLiqBoost
		}
		// clamp_step(max(1, int(multiplier * boost)))
		v := int(float64(s.Multiplier) * boost) // truncates toward zero
		if v < 1 {
			v = 1
		}
		return s.clampStep(float64(v))
	}

	// Gradually decrease fees when no flow is detected.
	if float64(s.LowLiqLimit) < r.OverallOutPercent && r.OverallOutPercent < float64(s.ExcessLimit) {
		hoursIdle := r.HoursSinceLastForward
		if r.LastForward != nil && r.FeesUpdated.Before(*r.LastForward) {
			if hoursIdle >= 2 {
				return s.clampStep(-2)
			}
		} else if hoursIdle >= 6 {
			return s.clampStep(-2)
		}
	}

	switch {
	case r.OverallOutPercent <= float64(s.LowLiqLimit):
		return 0
	case r.OverallOutPercent >= float64(s.ExcessLimit):
		// Don't reduce fees if peer's inbound fee rate is positive.
		if r.RemoteInboundFeeRate > 0 {
			return 0
		}
		adj := -1.0
		if s.ExcessBoostEnabled {
			adj = math.Trunc(adj * s.ExcessBoost)
		}
		return s.clampStep(adj)
	default:
		var adj float64
		if r.TotalAmtRoutedIn7day+r.TotalAmtRoutedOut7day == 0 {
			adj = -3 * float64(s.Multiplier)
			if s.ExcessBoostEnabled {
				adj = math.Trunc(adj * s.ExcessBoost)
			}
		} else if math.Abs(r.GroupNetRouted7day) > 1 {
			flow := clampFlow(r.GroupNetRouted7day)
			scale := 1 + math.Abs(flow)*s.FlowScale
			var base float64
			if flow > 0 {
				base = (2 * float64(s.Multiplier)) * highFlowFactor
			} else {
				base = (-5 * float64(s.Multiplier)) * highFlowFactor
			}
			adj = base * scale
		} else {
			adj = 0
		}
		return s.clampStep(adj)
	}
}

// max1 returns x if x > 1, otherwise 1.
func max1(x int) int {
	if x > 1 {
		return x
	}
	return 1
}
