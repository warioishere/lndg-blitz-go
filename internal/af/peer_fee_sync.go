// Package af implements the auto-fees engine and the peer fee sync helper.
package af

// MirrorPeerFeeTargets propagates the fee targets of the channel with the
// lowest out_percent for each peer onto all other channels of that peer.
// It operates in-place on the supplied rows and returns them.
func MirrorPeerFeeTargets(rows []*ChannelFeeRow) []*ChannelFeeRow {
	if len(rows) == 0 {
		return rows
	}

	// Group channels by remote_pubkey, preserving input order within each group.
	order := make([]string, 0)
	groups := make(map[string][]*ChannelFeeRow)
	for _, r := range rows {
		if _, ok := groups[r.RemotePubkey]; !ok {
			order = append(order, r.RemotePubkey)
		}
		groups[r.RemotePubkey] = append(groups[r.RemotePubkey], r)
	}

	for _, pubkey := range order {
		group := groups[pubkey]
		if len(group) <= 1 {
			continue
		}

		// Find the channel with the minimum out_percent (first occurrence on ties).
		controller := group[0]
		for _, r := range group[1:] {
			if r.OutPercent < controller.OutPercent {
				controller = r
			}
		}

		// Guard: ensure there is at least one follower channel to update.
		followerCount := 0
		for _, r := range group {
			if r != controller {
				followerCount++
			}
		}
		if followerCount == 0 {
			continue
		}

		// Capture controller values before writing to any follower.
		controllerNewRate := controller.NewRate
		controllerNewInboundRate := controller.NewInboundRate
		controllerEligible := controller.Eligible

		for _, f := range group {
			if f == controller {
				continue
			}
			f.NewRate = controllerNewRate
			f.Adjustment = controllerNewRate - float64(f.LocalFeeRate)
			f.NewInboundRate = controllerNewInboundRate
			f.InboundAdjustment = controllerNewInboundRate - float64(f.LocalInboundFeeRate)
		}

		// Propagate the controller's eligibility to the entire group.
		for _, m := range group {
			m.Eligible = controllerEligible
		}
	}

	return rows
}
