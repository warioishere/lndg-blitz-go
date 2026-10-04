"""End-to-end side-by-side harness: seeds a scenario into the lndg DB via Django
ORM, runs af.main, and emits the resulting per-channel fee targets as JSON.
The Go integration test reads the same DB, runs af.Main, and compares.

Env:
  AF_MODE = 'curve' | 'legacy'   (controls AF-CurveMode seeding)
"""
import os, sys, json
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'lndg.settings')

import django  # noqa
django.setup()
from datetime import datetime, timedelta
from gui.models import Channels, Forwards, Payments, FailedHTLCs, LocalSettings

import af  # triggers its own django.setup(); models already loaded


def clear():
    Channels.objects.all().delete()
    Forwards.objects.all().delete()
    Payments.objects.all().delete()
    FailedHTLCs.objects.all().delete()
    LocalSettings.objects.all().delete()


def seed():
    now = datetime.now()
    mode = os.environ.get('AF_MODE', 'legacy')
    if mode == 'curve':
        LocalSettings(key='AF-CurveMode', value='1').save()
    else:
        LocalSettings(key='AF-CurveMode', value='0').save()
    # Enable FLP globally to exercise the cost floor.
    LocalSettings(key='FLP-Enabled', value='1').save()
    # 1 ppm steps (as on the node): a +1 adjustment is not rounded away
    LocalSettings(key='AF-Increment', value='1').save()

    def ch(chan_id, pubkey, cap, local, remote, **kw):
        defaults = dict(
            remote_pubkey=pubkey, chan_id=chan_id, capacity=cap,
            local_balance=local, remote_balance=remote,
            pending_outbound=0, pending_inbound=0,
            local_fee_rate=kw.get('local_fee_rate', 200),
            local_inbound_fee_rate=kw.get('local_inbound_fee_rate', 0),
            remote_fee_rate=kw.get('remote_fee_rate', 150),
            remote_inbound_fee_rate=kw.get('remote_inbound_fee_rate', 0),
            ar_in_target=kw.get('ar_in_target', 30),
            ar_max_cost=kw.get('ar_max_cost', 50),
            auto_rebalance=kw.get('auto_rebalance', True),
            fees_updated=now - timedelta(days=30),
            flp_enabled=kw.get('flp_enabled', False),
            flp_safety=kw.get('flp_safety', 0),
            is_open=True, is_active=True, alias=kw.get('alias', chan_id),
            # remaining NOT NULL columns without model defaults:
            short_chan_id='', funding_txid='', output_index=0,
            unsettled_balance=0, local_commit=0, local_chan_reserve=0,
            num_updates=0, initiator=True, total_sent=0, total_received=0,
            private=False, htlc_count=0, local_base_fee=0,
            local_inbound_base_fee=0, local_disabled=False, local_cltv=40,
            local_min_htlc_msat=1000, local_max_htlc_msat=cap * 1000,
            remote_base_fee=0, remote_inbound_base_fee=0, remote_disabled=False,
            remote_cltv=40, remote_min_htlc_msat=1000,
            remote_max_htlc_msat=cap * 1000, push_amt=0, close_address='',
            last_update=now,
        )
        c = Channels(**defaults)
        c.save()
        return c

    # Peer A: 2 channels (multi-channel -> mirror). ch100 low-liq, ch101 mid.
    ch('100', 'A'*66, 1000000, 50000, 950000, ar_in_target=30, local_fee_rate=200,
       flp_enabled=True, flp_safety=5)
    ch('101', 'A'*66, 2000000, 1000000, 1000000, ar_in_target=50, local_fee_rate=300)
    # Peer B: single channel, excess liquidity.
    ch('200', 'B'*66, 1000000, 970000, 30000, ar_in_target=40, local_fee_rate=500,
       remote_inbound_fee_rate=0)
    # Peer C: single channel, mid-range, with forwards.
    ch('300', 'C'*66, 4000000, 2000000, 2000000, ar_in_target=20, local_fee_rate=100)
    # Peer D: 2 channels, ch400 carries a manual positive inbound fee (AF must leave it
    # alone, also after the peer mirror), ch401 is the low-liquidity mirror controller.
    ch('400', 'D'*66, 1000000, 600000, 400000, local_fee_rate=250, local_inbound_fee_rate=50)
    ch('401', 'D'*66, 1000000, 100000, 900000, local_fee_rate=250, local_inbound_fee_rate=-20)
    # Peer E: depleted below a 10% target: curve mode must still rise by 1 ppm.
    ch('500', 'E'*66, 1000000, 10000, 990000, ar_in_target=90, local_fee_rate=120, remote_fee_rate=10)

    # Forwards: recent (within 4h) and older (within 7d). amt_out_msat >= 1e6.
    def fwd(cin, cout, amt_msat, fee, ago):
        Forwards(forward_date=now - ago, chan_id_in=cin, chan_id_out=cout,
                 amt_in_msat=amt_msat + 1000, amt_out_msat=amt_msat, fee=fee,
                 inbound_fee=0).save()

    # within 1h (counts in 4h/1d/7d)
    fwd('100', '300', 5000000, 50.0, timedelta(hours=1))
    fwd('300', '100', 2000000, 20.0, timedelta(hours=1))
    fwd('101', '300', 3000000, 30.0, timedelta(hours=1))
    # ~3 days ago (counts in 7d only)
    fwd('300', '200', 8000000, 80.0, timedelta(days=3))
    fwd('200', '300', 1500000, 15.0, timedelta(days=3))

    # Rebalance payments for ch100 (status=2 succeeded), for avg cost / cost floor.
    def pay(phash, chan_id, fee, value, ago, chan_out=None, source_fee_rate=None):
        Payments(creation_date=now - ago, payment_hash=phash, value=value, fee=fee,
                 status=2, index=0, rebal_chan=chan_id, cleaned=False,
                 chan_out=chan_out, source_fee_rate=source_fee_rate).save()

    pay('h1', '100', 50.0, 100000.0, timedelta(hours=2))
    pay('h2', '100', 60.0, 120000.0, timedelta(hours=3))
    pay('h3', '100', 0.0, 0.0, timedelta(hours=4))  # value 0 -> skipped
    # opportunity cost: stored source fee, fallback to the source's current fee, MPP -> 0
    pay('h4', '100', 30.0, 100000.0, timedelta(hours=5), chan_out='300', source_fee_rate=120)
    pay('h5', '100', 30.0, 100000.0, timedelta(hours=6), chan_out='300')
    pay('h6', '100', 30.0, 100000.0, timedelta(hours=7), chan_out='MPP')

    # Failed HTLCs: wire_failure=15, failure_detail=6, amount > liq+pending.
    def fail(phash_id, cout, amount, liq, pending, ago):
        FailedHTLCs(timestamp=now - ago, amount=amount, chan_id_in='x', chan_id_out=cout,
                    wire_failure=15, failure_detail=6, missed_fee=0,
                    chan_out_liq=liq, chan_out_pending=pending).save()

    # 5 min ago (inside htlc_boost_interval=15min and 24h)
    fail(1, '100', 500000, 100000, 0, timedelta(minutes=5))
    fail(2, '100', 600000, 100000, 0, timedelta(minutes=5))
    # 1h ago (inside 24h, outside 15min)
    fail(3, '300', 700000, 100000, 0, timedelta(hours=1))
    # NULL liq -> excluded
    fail(4, '300', 700000, None, None, timedelta(hours=1))


def run():
    clear()
    seed()
    df = af.main(Channels.objects.filter(is_open=True))
    out = []
    for _, row in df.iterrows():
        out.append({
            'chan_id': row['chan_id'],
            'new_rate': float(row['new_rate']),
            'adjustment': float(row['adjustment']),
            'new_inbound_rate': float(row['new_inbound_rate']),
            'inbound_adjustment': float(row['inbound_adjustment']),
            'out_percent': int(row['out_percent']),
            'avg_rebalance_cost': None if row['avg_rebalance_cost'] is None or row['avg_rebalance_cost'] != row['avg_rebalance_cost'] else float(row['avg_rebalance_cost']),
            'cost_floor': float(row['cost_floor']),
        })
    out.sort(key=lambda r: r['chan_id'])
    print(json.dumps(out))


if __name__ == '__main__':
    run()
