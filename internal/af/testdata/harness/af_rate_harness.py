"""Differential harness for af.py's rate pipeline (lines 420-482), run via the
exact pandas vectorized ops over a grid, emitting per-row results as JSON."""
import json
import pandas as pd
from pandas import isna


def run(settings, rows):
    flp_enabled_global = settings['flp_enabled_global']
    flp_safety_global = settings['flp_safety_global']
    increment = settings['increment']
    min_rate = settings['min_rate']
    max_rate = settings['max_rate']

    channels_df = pd.DataFrame.from_records(rows)

    # af.py:420-422
    channels_df['new_rate'] = channels_df['local_fee_rate'] + channels_df['adjustment']
    channels_df['new_rate'] = (channels_df['new_rate'] / increment).round(0) * increment
    channels_df['new_rate'] = channels_df['new_rate'].clip(min_rate, max_rate)
    # af.py:424-425
    channels_df['new_rate_before_floor'] = channels_df['new_rate']
    channels_df['adjustment_before_floor'] = channels_df['adjustment']

    def compute_cost_floor(row):
        if not flp_enabled_global:
            return 0
        flp_enabled = row.get('flp_enabled', False)
        if isna(flp_enabled):
            flp_enabled = False
        if not bool(flp_enabled):
            return 0
        avg_cost = row.get('avg_rebalance_cost')
        if avg_cost is None or isna(avg_cost):
            return 0
        channel_safety = row.get('flp_safety', 0)
        if isna(channel_safety):
            channel_safety = 0
        safety = flp_safety_global + channel_safety
        current_rate = row.get('local_fee_rate')
        if current_rate is None or isna(current_rate):
            current_rate = 0
        floor_value = max(avg_cost + safety, 0)
        if current_rate > 0:
            floor_value = min(floor_value, current_rate)
        return int(round(floor_value))

    channels_df['cost_floor'] = (
        channels_df.apply(compute_cost_floor, axis=1)
        .round(0)
        .clip(upper=max_rate)
        .fillna(0)
    )

    def enforce_cost_floor(row):
        proposed = row['new_rate']
        current = row['local_fee_rate']
        floor = row['cost_floor']
        if proposed < current:
            if floor > current:
                return current
            if floor > proposed:
                return floor
        return proposed

    channels_df['new_rate'] = channels_df.apply(enforce_cost_floor, axis=1)
    channels_df['adjustment'] = channels_df['new_rate'] - channels_df['local_fee_rate']

    # af.py:476-482
    if 'ar_max_cost' not in channels_df.columns:
        channels_df['ar_max_cost'] = 0
    channels_df['new_inbound_rate'] = channels_df['local_inbound_fee_rate'] + channels_df['inbound_adjustment']
    channels_df['new_inbound_rate'] = (channels_df['new_inbound_rate'] / increment).round(0) * increment
    channels_df['new_inbound_rate'] = channels_df['new_inbound_rate'].clip(
        -((channels_df['ar_max_cost'] / 100) * channels_df['local_fee_rate']), 0)
    channels_df['inbound_adjustment'] = channels_df['new_inbound_rate'] - channels_df['local_inbound_fee_rate']

    return channels_df


def build():
    settings_grid = [
        dict(flp_enabled_global=False, flp_safety_global=0, increment=5, min_rate=0, max_rate=2500),
        dict(flp_enabled_global=True, flp_safety_global=10, increment=5, min_rate=0, max_rate=2500),
        dict(flp_enabled_global=True, flp_safety_global=0, increment=1, min_rate=50, max_rate=1000),
        dict(flp_enabled_global=True, flp_safety_global=25, increment=10, min_rate=0, max_rate=500),
    ]
    local_rates = [0, 1, 50, 137, 500, 2600]
    adjustments = [-300, -53, -5, -1, 0, 1, 3, 47, 250]
    inbound_adjs = [-40, -7, 0, 7, 25]
    ar_max_costs = [0, 50, 100, 200]
    local_inbound = [-20, 0, 10]
    flp_enabled = [True, False]
    flp_safety = [0, 15]
    avg_costs = [None, 0, 80, 300]

    out = []
    for S in settings_grid:
        rows = []
        meta = []
        idx = 0
        for lr in local_rates:
            for adj in adjustments:
                for iadj in inbound_adjs:
                    for amc in ar_max_costs:
                        idx += 1
                        lif = local_inbound[idx % len(local_inbound)]
                        fe = flp_enabled[idx % len(flp_enabled)]
                        fs = flp_safety[idx % len(flp_safety)]
                        ac = avg_costs[idx % len(avg_costs)]
                        row = dict(local_fee_rate=lr, adjustment=adj, inbound_adjustment=iadj,
                                   ar_max_cost=amc, local_inbound_fee_rate=lif,
                                   flp_enabled=fe, flp_safety=fs, avg_rebalance_cost=ac)
                        rows.append(row)
                        meta.append(dict(settings=S, **row))
        df = run(S, rows)
        for i in range(len(df)):
            r = df.iloc[i]
            out.append({
                'settings': S,
                'in': meta[i],
                'new_rate': float(r['new_rate']),
                'adjustment': float(r['adjustment']),
                'cost_floor': float(r['cost_floor']),
                'new_inbound_rate': float(r['new_inbound_rate']),
                'inbound_adjustment': float(r['inbound_adjustment']),
            })
    return out


if __name__ == '__main__':
    print(json.dumps(build()))
