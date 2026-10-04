"""Side-by-side harness for the read API: seeds every API table through the Django
ORM, then prints the Python API's answers (status + body) for the list, detail,
no-format and unknown-pk routes of each ViewSet as one JSON object on stdout.
The Go test serves the same database and compares response by response.
"""
import os, sys, json, decimal
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'lndg.settings')
import django; django.setup()
from django.conf import settings as djs
djs.ALLOWED_HOSTS = ['*']
from django.db import models as m
from django.test import Client
from datetime import datetime
from gui import models as gm

PREFIXES = ['payments','paymenthops','invoices','forwards','onchain','closures','resolutions','peers','channels',
            'rebalancer','settings','pendinghtlcs','failedhtlcs','peerevents','feelog','inboundfeelog',
            'rebalanceroutes','nodereputation','graphevents','probelogs','graphprobelogs']
MODELS = [gm.Payments, gm.PaymentHops, gm.Invoices, gm.Forwards, gm.Onchain, gm.Closures, gm.Resolutions, gm.Peers,
          gm.Channels, gm.Rebalancer, gm.LocalSettings, gm.PendingHTLCs, gm.FailedHTLCs, gm.PeerEvents, gm.Autofees,
          gm.InboundFeeLog, gm.RebalanceRoute, gm.NodeReputation, gm.GraphEvent, gm.ProbeLog, gm.GraphProbeLog]

def value_for(f, i, model):
    t = f.get_internal_type()
    if f.name == 'short_chan_id': return f'80000{i}x1x0'
    if model is gm.PeerEvents and f.name == 'chan_id': return 'C1'
    if f.null and i == 1 and not f.primary_key: return None
    if t in ('CharField', 'TextField'):
        n = f.max_length or 20
        return (f'{f.name[:6]}{model.__name__[:4]}{i}')[:n]
    if t in ('IntegerField', 'BigIntegerField', 'PositiveIntegerField', 'SmallIntegerField'): return 7 + i
    if t == 'FloatField': return 2.5 if i == 0 else 3.0  # whole floats render as 3.0
    if t == 'BooleanField': return i == 0
    if t == 'DateTimeField': return datetime(2026, 3, 4, 5, 6, 7, 123456 if i == 0 else 0)
    if t == 'JSONField': return [{'k': i}]
    if t == 'DecimalField': return decimal.Decimal('1.5')
    raise Exception(f'unhandled {model.__name__}.{f.name} {t}')

for model in reversed(MODELS): model.objects.all().delete()
for model in MODELS:
    for i in range(2):
        kw = {}
        for f in model._meta.concrete_fields:
            if f.auto_created and f.primary_key: continue
            if isinstance(f, m.ForeignKey):
                kw[f.name] = f.related_model.objects.order_by('pk')[i]
                continue
            v = value_for(f, i, model)
            if f.primary_key or f.unique: v = f'C{i+1}' if model is gm.Channels and f.name == 'chan_id' else v
            kw[f.name] = v
        model(**kw).save()

c = Client()
out = {}
for p in PREFIXES:
    r = c.get(f'/api/{p}/?format=json&limit=1')
    out[f'/api/{p}/?format=json&limit=1'] = [r.status_code, r.content.decode()]
    r = c.get(f'/api/{p}/?format=json')
    out[f'/api/{p}/?format=json'] = [r.status_code, r.content.decode()]
    try:
        _j = json.loads(r.content); first = (_j['results'] if isinstance(_j, dict) else _j)[0]
        pk = first['url'].split('?')[0].rstrip('/').split('/')[-1] if 'url' in first else None
    except Exception:
        pk = None
    if pk:
        r = c.get(f'/api/{p}/{pk}/?format=json')
        out[f'/api/{p}/{pk}/?format=json'] = [r.status_code, r.content.decode()]
        r = c.get(f'/api/{p}/{pk}/')
        out[f'/api/{p}/{pk}/'] = [r.status_code, r.content.decode()]
        r = c.get(f'/api/{p}/999999/')
        out[f'/api/{p}/999999/'] = [r.status_code, r.content.decode()]
print(json.dumps(out))
