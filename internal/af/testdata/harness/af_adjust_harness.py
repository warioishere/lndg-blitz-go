"""Differential harness: replicates the af.py adjustment functions verbatim with
explicit settings, runs them over an input grid, emits JSON for the Go test to
compare. Bodies copied 1:1 from af.py (closures parameterized as args)."""
import json, itertools

MAX_NET_FLOW = 3
HIGH_FLOW_FACTOR = 0.25


def clamp_flow(val):
    if val > MAX_NET_FLOW:
        return MAX_NET_FLOW
    if val < -MAX_NET_FLOW:
        return -MAX_NET_FLOW
    return val


def clamp_step(val, max_step):
    if val > max_step:
        return max_step
    if val < -max_step:
        return -max_step
    return int(val)


def compute_curve_outbound_adjustment(row, S):
    intensity = S['intensity']; exponent = S['exponent']; max_step = S['max_step']
    peer_rate_check = S['peer_rate_check']; peer_rate_limit = S['peer_rate_limit']
    downscale = S['downscale']; flow_weight = S['flow_weight']
    ch_target = 100 - row.get('ar_in_target', 90)
    deviation = (ch_target - row['out_percent']) / 100.0
    sign = 1 if deviation > 0 else (-1 if deviation < 0 else 0)
    adj = intensity * sign * abs(deviation) ** exponent
    if peer_rate_check and peer_rate_limit > 0 and adj > 0:
        if row.get('remote_fee_rate', 0) >= peer_rate_limit:
            return 0
    if adj < 0 and downscale != 1.0:
        adj *= downscale
    if flow_weight > 0 and row['net_routed_7day'] != 0:
        net_flow_ratio = row['net_routed_7day'] / MAX_NET_FLOW
        net_flow_ratio = max(-1.0, min(1.0, net_flow_ratio))
        if (adj > 0 and net_flow_ratio > 0) or (adj < 0 and net_flow_ratio < 0):
            adj *= (1 + flow_weight * abs(net_flow_ratio))
    return int(round(max(-max_step, min(max_step, adj))))


def compute_curve_inbound_adjustment(row, S):
    inbound_intensity = S['inbound_intensity']; exponent = S['exponent']; max_step = S['max_step']
    peer_tgt = row.get('peer_out_target', 10)
    deviation = (peer_tgt - row['overall_out_percent']) / 100.0
    sign = 1 if deviation > 0 else (-1 if deviation < 0 else 0)
    adj = inbound_intensity * sign * abs(deviation) ** exponent
    return int(round(max(-max_step, min(max_step, adj))))


def compute_inbound_adjustment(row, S):
    lowliq_limit = S['lowliq_limit']; excess_limit = S['excess_limit']; multiplier = S['multiplier']
    flow_scale = S['flow_scale']; max_step = S['max_step']
    if row['overall_out_percent'] <= lowliq_limit:
        adj = 0
    elif row['overall_out_percent'] < excess_limit:
        if row['total_amt_routed_in_7day'] + row['total_amt_routed_out_7day'] == 0:
            adj = 7 * multiplier
        elif row['group_net_routed_7day'] > 1:
            flow = clamp_flow(row['group_net_routed_7day'])
            scale = 1 + flow * flow_scale
            adj = (-5 * multiplier * HIGH_FLOW_FACTOR) * scale
        else:
            adj = 0
    else:
        if row['total_amt_routed_in_7day'] + row['total_amt_routed_out_7day'] == 0:
            adj = 12 * multiplier
        elif (row['group_net_routed_7day'] < -1
              and row['total_revenue_assist_7day'] > row['total_revenue_7day'] * 10):
            flow = abs(clamp_flow(row['group_net_routed_7day']))
            scale = 1 + flow * flow_scale
            adj = 12 * multiplier * HIGH_FLOW_FACTOR * scale
        else:
            adj = 0
    return clamp_step(adj, max_step)


def compute_outbound_adjustment(row, S):
    htlc_boost_amount = S['htlc_boost_amount']; lowliq_limit = S['lowliq_limit']
    htlc_boost_threshold = S['htlc_boost_threshold']; peer_rate_check = S['peer_rate_check']
    peer_rate_limit = S['peer_rate_limit']; bypass_peer_rate_on_htlc = S['bypass_peer_rate_on_htlc']
    boost_ar_only = S['boost_ar_only']; lowliq_boost = S['lowliq_boost']; multiplier = S['multiplier']
    excess_limit = S['excess_limit']; excess_boost_enabled = S['excess_boost_enabled']
    excess_boost = S['excess_boost']; flow_scale = S['flow_scale']; max_step = S['max_step']

    if (htlc_boost_amount > 0
        and row['out_percent'] <= lowliq_limit
        and row.get('failed_out_boost_interval', 0) >= htlc_boost_threshold):
        return htlc_boost_amount

    if row['out_percent'] <= lowliq_limit:
        if peer_rate_check and peer_rate_limit > 0 and row['remote_fee_rate'] >= peer_rate_limit:
            has_htlc_conditions = (htlc_boost_amount > 0 and
                                   row.get('failed_out_boost_interval', 0) >= htlc_boost_threshold)
            if not (bypass_peer_rate_on_htlc and has_htlc_conditions):
                return 0
        boost = 0
        if boost_ar_only and row.get('auto_rebalance'):
            deficit = max(0, lowliq_limit - row['out_percent'])
            boost = deficit / max(lowliq_limit, 1) * lowliq_boost
        return clamp_step(max(1, int(multiplier * boost)), max_step)

    if lowliq_limit < row['overall_out_percent'] < excess_limit:
        hours_idle = row.get('hours_since_last_forward', 0)
        if row['fees_updated'] < row.get('last_forward'):
            if hours_idle >= 2:
                return clamp_step(-2, max_step)
        elif hours_idle >= 6:
            return clamp_step(-2, max_step)

    if row['overall_out_percent'] <= lowliq_limit:
        return 0
    elif row['overall_out_percent'] >= excess_limit:
        if row.get('remote_inbound_fee_rate', 0) > 0:
            return 0
        adj = -1
        if excess_boost_enabled:
            adj = int(adj * excess_boost)
        return clamp_step(adj, max_step)
    elif row['overall_out_percent'] < excess_limit:
        if row['total_amt_routed_in_7day'] + row['total_amt_routed_out_7day'] == 0:
            adj = -3 * multiplier
            if excess_boost_enabled:
                adj = int(adj * excess_boost)
        elif abs(row['group_net_routed_7day']) > 1:
            flow = clamp_flow(row['group_net_routed_7day'])
            scale = 1 + abs(flow) * flow_scale
            base = (2 * multiplier if flow > 0 else -5 * multiplier) * HIGH_FLOW_FACTOR
            adj = base * scale
        else:
            adj = 0
        return clamp_step(adj, max_step)
    else:
        if row['total_amt_routed_in_7day'] + row['total_amt_routed_out_7day'] == 0:
            adj = -5 * multiplier
            if excess_boost_enabled:
                adj = int(adj * excess_boost)
        elif (row['group_net_routed_7day'] < -1
              and row['total_revenue_assist_7day'] > row['total_revenue_7day'] * 10):
            flow = abs(clamp_flow(row['group_net_routed_7day']))
            scale = 1 + flow * flow_scale
            adj = -5 * multiplier * HIGH_FLOW_FACTOR
            if excess_boost_enabled:
                adj = int(adj * excess_boost)
            adj *= scale
        else:
            adj = 0
        return clamp_step(adj, max_step)


# fixed reference clock for fees_updated/last_forward comparison.
# Use pandas Timestamp/NaT to faithfully reproduce af.py's df.apply semantics:
# a missing last_forward is NaT, and `Timestamp < NaT` is False (not a TypeError).
import datetime as _dt
import pandas as _pd
T0 = _pd.Timestamp('2025-01-01 00:00:00')


def build_cases():
    cases = []
    settings_grid = [
        dict(intensity=50, exponent=2.0, max_step=100, peer_rate_check=False, peer_rate_limit=0,
             downscale=1.0, flow_weight=0.5, inbound_intensity=20, lowliq_limit=5, excess_limit=95,
             multiplier=5, flow_scale=1.0, htlc_boost_amount=0, htlc_boost_threshold=5,
             bypass_peer_rate_on_htlc=False, boost_ar_only=False, lowliq_boost=1.0,
             excess_boost_enabled=False, excess_boost=1.0),
        dict(intensity=80, exponent=1.5, max_step=50, peer_rate_check=True, peer_rate_limit=300,
             downscale=0.5, flow_weight=0.8, inbound_intensity=40, lowliq_limit=10, excess_limit=90,
             multiplier=8, flow_scale=2.0, htlc_boost_amount=25, htlc_boost_threshold=3,
             bypass_peer_rate_on_htlc=True, boost_ar_only=True, lowliq_boost=2.0,
             excess_boost_enabled=True, excess_boost=1.5),
        dict(intensity=30, exponent=3.0, max_step=20, peer_rate_check=True, peer_rate_limit=100,
             downscale=1.0, flow_weight=0.0, inbound_intensity=10, lowliq_limit=3, excess_limit=97,
             multiplier=3, flow_scale=0.5, htlc_boost_amount=10, htlc_boost_threshold=2,
             bypass_peer_rate_on_htlc=False, boost_ar_only=True, lowliq_boost=0.5,
             excess_boost_enabled=True, excess_boost=2.0),
    ]
    out_percents = [0, 3, 5, 10, 50, 90, 95, 100]
    overall = [0.0, 5.0, 47.5, 50.0, 90.0, 95.0, 99.9]
    nets = [-3.5, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 3.5]
    ar_in = [10, 50, 90]
    peer_out = [10, 50, 90]
    revenues = [(0, 0), (100, 5), (5, 100)]
    routed = [(0, 0), (1000, 2000), (5000, 1000)]
    idle = [0.0, 3.0, 7.0]
    last_fwd_before = [True, False, None]
    rfr = [0, 200, 400]
    rifr = [0, 5]

    idx = 0
    for S in settings_grid:
        for op in out_percents:
            for oop in overall:
                for nr in nets:
                    for ai in ar_in:
                        idx += 1
                        # cycle through secondary params to keep grid bounded
                        po = peer_out[idx % len(peer_out)]
                        rin, rout = routed[idx % len(routed)]
                        rev_a, rev_7 = revenues[idx % len(revenues)]
                        hi = idle[idx % len(idle)]
                        lfb = last_fwd_before[idx % len(last_fwd_before)]
                        remote_fr = rfr[idx % len(rfr)]
                        remote_ifr = rifr[idx % len(rifr)]
                        auto_rb = bool(idx % 2)

                        if lfb is None:
                            last_forward = _pd.NaT
                            fees_updated = T0
                        elif lfb:
                            last_forward = T0 + _pd.Timedelta(hours=1)
                            fees_updated = T0
                        else:
                            last_forward = T0
                            fees_updated = T0 + _pd.Timedelta(hours=1)

                        ch_row = {
                            'ar_in_target': ai, 'out_percent': op, 'net_routed_7day': nr,
                            'remote_fee_rate': remote_fr, 'remote_inbound_fee_rate': remote_ifr,
                            'overall_out_percent': oop, 'failed_out_boost_interval': 4,
                            'auto_rebalance': auto_rb, 'hours_since_last_forward': hi,
                            'fees_updated': fees_updated, 'last_forward': last_forward,
                            'total_amt_routed_in_7day': rin, 'total_amt_routed_out_7day': rout,
                            'total_revenue_assist_7day': rev_a, 'total_revenue_7day': rev_7,
                            'group_net_routed_7day': nr,
                        }
                        grp_row = {
                            'peer_out_target': po, 'overall_out_percent': oop,
                            'total_amt_routed_in_7day': rin, 'total_amt_routed_out_7day': rout,
                            'group_net_routed_7day': nr, 'total_revenue_assist_7day': rev_a,
                            'total_revenue_7day': rev_7,
                        }
                        cases.append({
                            'settings': S,
                            'ch': {
                                'ar_in_target': ai, 'out_percent': op, 'net_routed_7day': nr,
                                'remote_fee_rate': remote_fr, 'remote_inbound_fee_rate': remote_ifr,
                                'overall_out_percent': oop, 'failed_out_boost_interval': 4,
                                'auto_rebalance': auto_rb, 'hours_since_last_forward': hi,
                                'last_forward_before': lfb,
                                'total_amt_routed_in_7day': rin, 'total_amt_routed_out_7day': rout,
                                'total_revenue_assist_7day': rev_a, 'total_revenue_7day': rev_7,
                                'group_net_routed_7day': nr,
                            },
                            'grp': grp_row,
                            'curve_out': compute_curve_outbound_adjustment(ch_row, S),
                            'curve_in': compute_curve_inbound_adjustment(grp_row, S),
                            'legacy_in': compute_inbound_adjustment(grp_row, S),
                            'legacy_out': compute_outbound_adjustment(ch_row, S),
                        })
    return cases


if __name__ == '__main__':
    cases = build_cases()
    print(json.dumps(cases))
