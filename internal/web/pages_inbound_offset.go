package web

import (
	"fmt"
	"net/http"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// handleInboundOffsetPost switches dynamic inbound fees on or off per channel.
// The form lists every shown channel in "channels" and the checked ones in
// "dynamic". inbound_offset != 0 marks a channel as dynamic (the column of the
// Django schema; its value is not used). Switching on applies the dynamic rate
// at once, switching off sets the inbound rate to 0; both are logged.
func (s *Server) handleInboundOffsetPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()

	want := map[string]bool{}
	for _, id := range r.PostForm["dynamic"] {
		want[id] = true
	}
	rows, err := s.db.Query(ctx, `SELECT `+ffaColumns+` FROM gui_channels WHERE chan_id = ANY($1) ORDER BY chan_id`, r.PostForm["channels"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	channels, err := scanFFAChannels(rows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		f.add("Error checking LND version: " + err.Error())
		s.redirect(w, r, "/inbound-offset/", f)
		return
	}
	if !versionAtLeast(info.GetVersion(), 0.18) {
		f.add("LND version too low to set inbound fees, update to v0.18+")
		s.redirect(w, r, "/inbound-offset/", f)
		return
	}

	on, off := 0, 0
	for _, ch := range channels {
		dynamic := want[ch.chanID]
		if dynamic == (ch.inboundOffset != 0) {
			continue
		}
		var flag, rate int32
		setting := "Dynamic Off"
		if dynamic {
			flag, setting = -1, "Dynamic On"
			ch.inboundOffset = flag
			rate, _ = s.inboundDynamicRate(ctx, ch, int(ch.localFeeRate))
		}
		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
			Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
			BaseFeeMsat:   int64(ch.localBaseFee),
			FeeRate:       float64(ch.localFeeRate) / 1000000,
			TimeLockDelta: uint32(ch.localCltv),
			InboundFee:    &lnrpc.InboundFee{BaseFeeMsat: ch.localInboundBaseFee, FeeRatePpm: rate},
		}); err != nil {
			f.add("Error updating " + ch.alias + ": " + err.Error())
			continue
		}
		now := time.Now()
		if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET inbound_offset=$2, local_inbound_fee_rate=$3, offset_updated=$4 WHERE chan_id=$1`,
			ch.chanID, flag, rate, now); err != nil {
			f.add("Error updating " + ch.alias + ": " + err.Error())
			continue
		}
		if rate != ch.localInboundFeeRate {
			if _, err := s.db.Exec(ctx, `INSERT INTO gui_inboundfeelog (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
				now, ch.chanID, ch.alias, setting, ch.localInboundFeeRate, rate); err != nil {
				f.add("Error updating " + ch.alias + ": " + err.Error())
				continue
			}
		}
		if dynamic {
			on++
		} else {
			off++
		}
	}
	f.add(fmt.Sprintf("Dynamic inbound fees switched on for %d and off for %d channel(s).", on, off))
	s.redirect(w, r, "/inbound-offset/", f)
}
