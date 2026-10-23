"""Live dashboard: per-service window metrics, alerts, dead-letter and late counts, Flink job state.

    uvicorn dashboard.app:app --host 127.0.0.1 --port 19080
"""

from __future__ import annotations

import httpx
import psycopg
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from infra import stack

app = FastAPI(title="stream-anomaly-dashboard")


@app.get("/api/state")
def state() -> dict:
    with psycopg.connect(stack.DSN, autocommit=True) as conn:
        latest = conn.execute(
            "SELECT DISTINCT ON (service) service, window_start_ms, request_count, error_count, latency_p95_ms, "
            "queue_depth_max FROM windows ORDER BY service, window_start_ms DESC").fetchall()
        series = conn.execute(
            "SELECT service, window_start_ms, latency_p95_ms, error_count::float / request_count, queue_depth_max "
            "FROM windows ORDER BY window_start_ms").fetchall()
        alerts = conn.execute("SELECT alert_id, service, opened_window_ms, closed_window_ms, metrics, z_at_open "
                              "FROM alerts ORDER BY opened_window_ms DESC LIMIT 20").fetchall()
        counts = conn.execute("SELECT (SELECT count(*) FROM windows), (SELECT count(*) FROM late_events), "
                              "(SELECT count(*) FROM dlq), (SELECT count(*) FROM window_operator_late)").fetchone()
        dlq = conn.execute("SELECT reason, count(*) FROM dlq GROUP BY reason ORDER BY 2 DESC").fetchall()
    jobs = []
    try:
        overview = httpx.get(f"{stack.FLINK_URL}/jobs/overview", timeout=2).json()["jobs"]
        for j in sorted(overview, key=lambda j: j["start-time"], reverse=True)[:3]:  # newest first
            ck = httpx.get(f"{stack.FLINK_URL}/jobs/{j['jid']}/checkpoints", timeout=2).json()
            jobs.append({"name": j["name"], "state": j["state"], "checkpoints_completed": ck["counts"]["completed"]})
    except httpx.HTTPError:
        jobs = [{"name": "flink unreachable"}]
    return {
        "latest": [dict(zip(("service", "window_start_ms", "requests", "errors", "p95_ms", "queue_max"), r)) for r in latest],
        "series": [dict(zip(("service", "t", "p95_ms", "error_rate", "queue_max"), r)) for r in series],
        "alerts": [dict(zip(("alert_id", "service", "opened_ms", "closed_ms", "metrics", "z"), r)) for r in alerts],
        "counts": dict(zip(("windows", "late_dropped", "dead_lettered", "window_operator_late"), counts)),
        "dlq_reasons": dict(dlq), "jobs": jobs,
    }


PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stream anomalies</title>
<style>
:root{--bg:#fafaf9;--fg:#1c1917;--muted:#78716c;--card:#fff;--line:#e7e5e4;--bad:#b91c1c;--ok:#15803d}
@media (prefers-color-scheme:dark){:root{--bg:#1c1917;--fg:#f5f5f4;--muted:#a8a29e;--card:#292524;--line:#44403c;--bad:#f87171;--ok:#4ade80}}
body{background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;margin:0;padding:16px;max-width:1100px;margin:auto}
h1{font-size:18px;margin:0 0 12px} h2{font-size:14px;margin:18px 0 6px;color:var(--muted);font-weight:600}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:10px}
.k{color:var(--muted);font-size:12px} .v{font-size:20px;font-variant-numeric:tabular-nums}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);border-radius:8px;overflow:hidden}
td,th{padding:6px 8px;border-bottom:1px solid var(--line);text-align:left;font-variant-numeric:tabular-nums} th{color:var(--muted);font-weight:600}
.bad{color:var(--bad)} .ok{color:var(--ok)} .wrap{overflow-x:auto}
</style></head><body>
<h1>Streaming anomaly pipeline</h1>
<div class="grid" id="counts"></div>
<h2>Latest window per service</h2><div class="wrap"><table id="latest"></table></div>
<h2>Alerts</h2><div class="wrap"><table id="alerts"></table></div>
<h2>Dead-letter reasons &middot; Flink</h2><div class="grid" id="misc"></div>
<script>
const T0=1767225600000, s=ms=>((ms-T0)/1000).toFixed(0)+'s';
function table(el,cols,rows){el.innerHTML='<tr>'+cols.map(c=>'<th>'+c[0]+'</th>').join('')+'</tr>'+
 rows.map(r=>'<tr>'+cols.map(c=>'<td>'+c[1](r)+'</td>').join('')+'</tr>').join('')}
async function tick(){try{const d=await (await fetch('api/state')).json();
 const c=d.counts; document.getElementById('counts').innerHTML=[['windows',c.windows],['late-dropped',c.late_dropped],
 ['dead-lettered',c.dead_lettered],['tripwire (must be 0)',c.window_operator_late,c.window_operator_late?'bad':'ok']]
 .map(([k,v,cl])=>`<div class="card"><div class="k">${k}</div><div class="v ${cl||''}">${v}</div></div>`).join('');
 table(document.getElementById('latest'),[['service',r=>r.service],['window',r=>s(r.window_start_ms)],['requests',r=>r.requests],
  ['errors',r=>r.errors],['p95 ms',r=>r.p95_ms.toFixed(1)],['queue max',r=>r.queue_max]],d.latest);
 table(document.getElementById('alerts'),[['service',r=>r.service],['opened',r=>s(r.opened_ms)],
  ['closed',r=>r.closed_ms?s(r.closed_ms):'<span class="bad">open</span>'],['metrics',r=>r.metrics],['z',r=>r.z.toFixed(1)]],d.alerts);
 document.getElementById('misc').innerHTML=Object.entries(d.dlq_reasons).map(([k,v])=>`<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`).join('')+
  d.jobs.map(j=>`<div class="card"><div class="k">${j.name}</div><div class="v">${j.state||''}</div><div class="k">checkpoints: ${j.checkpoints_completed??''}</div></div>`).join('');
}catch(e){}}
tick(); setInterval(tick,2000);
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE
