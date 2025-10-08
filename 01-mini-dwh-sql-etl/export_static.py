from pathlib import Path
import sqlite3, json, pandas as pd

BASE = Path(__file__).resolve().parent
DB_PATH = BASE / "mini_dwh.sqlite"
DOCS = BASE.parent / "docs"
DOCS.mkdir(parents=True, exist_ok=True)


# 👇 root/docs (one level UP from 01-mini-dwh-sql-etl/)


# --- read from SQLite views (already created by your ETL)
con = sqlite3.connect(DB_PATH)

monthly = pd.read_sql("""
  SELECT month, revenue, orders, aov
  FROM v_monthly_kpis
  ORDER BY month
""", con)

top = pd.read_sql("""
  SELECT product_id, category, subcategory, revenue
  FROM v_top_products
  ORDER BY revenue DESC
  LIMIT 10
""", con)

cat = pd.read_sql("""
  SELECT category, revenue, pct
  FROM v_category_contribution
  ORDER BY revenue DESC
""", con)

dq_null = pd.read_sql("SELECT COUNT(*) AS issues FROM dq_nulls;", con)["issues"].iloc[0]
dq_neg  = pd.read_sql("SELECT COUNT(*) AS issues FROM dq_negative_qty;", con)["issues"].iloc[0]

tot_rev = float(monthly["revenue"].sum())
tot_orders = int(monthly["orders"].sum())
aov = float(tot_rev / max(tot_orders, 1))
payload = {
    "kpis": {
        "revenue": round(tot_rev, 2),
        "orders": tot_orders,
        "aov": round(aov, 2),
        "dq_issues": int(dq_null + dq_neg)
    },
    "monthly": monthly.to_dict(orient="list"),
    "top_products": top.to_dict(orient="records"),
    "category": cat.to_dict(orient="records")
}

# --- write JSON
(DOCS / "data.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

# --- write a static HTML dashboard that loads data.json and draws charts
html = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>E-commerce KPIs (Static)</title>
<link rel="preconnect" href="https://cdn.plot.ly">
<style>
  :root { --blue:#1f77b4; --bg:#f8fafc; --card:#ffffff; --text:#0f172a; --muted:#64748b; }
  body { margin:0; font:16px/1.5 system-ui, -apple-system, Segoe UI, Roboto, Arial; background:var(--bg); color:var(--text);}
  .wrap { max-width:1100px; margin:auto; padding:24px; }
  h1 { text-align:center; color:var(--blue); font-size:34px; margin:6px 0 12px; }
  .sub { text-align:center; color:var(--muted); margin-bottom:18px; }
  .grid { display:grid; gap:16px; }
  .kpis { grid-template-columns: repeat(4, 1fr); }
  .card { background:var(--card); border:1px solid #e5e7eb; border-radius:14px; padding:14px; }
  .label { color:var(--muted); font-size:13px; }
  .value { font-size:24px; font-weight:800; }
  .two { grid-template-columns:1fr 1fr; }
  footer { margin-top:24px; text-align:center; color:var(--muted); font-size:14px; }
  @media (max-width:900px) {
    .kpis { grid-template-columns:1fr 1fr; }
    .two  { grid-template-columns:1fr; }
  }
</style>
</head>
<body>
<div class="wrap">
  <h1>Mini Data Warehouse — E-commerce KPIs</h1>
  <div class="sub">Static snapshot (instant load). Built from CSV/API → SQLite views.</div>

  <div class="grid kpis">
    <div class="card"><div class="label">Revenue</div><div class="value" id="rev">–</div></div>
    <div class="card"><div class="label">Orders</div><div class="value" id="ord">–</div></div>
    <div class="card"><div class="label">Average Order Value</div><div class="value" id="aov">–</div></div>
    <div class="card"><div class="label">DQ Issues</div><div class="value" id="dq">–</div></div>
  </div>

  <div class="grid two" style="margin-top:16px;">
    <div class="card"><div id="monthly" style="height:360px;"></div></div>
    <div class="card"><div id="top" style="height:360px;"></div></div>
  </div>

  <div class="card" style="margin-top:16px;"><div id="cat" style="height:360px;"></div></div>

  <footer>Repo: GitHub → <a href="../" target="_blank">code</a> · Interactive app (may be slower): add link here</footer>
</div>

<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<script>
fetch('data.json').then(r => r.json()).then(d => {
  // KPI cards
  const fmt = (n)=> Number(n).toLocaleString();
  document.getElementById('rev').textContent = fmt(Math.round(d.kpis.revenue));
  document.getElementById('ord').textContent = fmt(d.kpis.orders);
  document.getElementById('aov').textContent = fmt(Math.round(d.kpis.aov));
  document.getElementById('dq').textContent  = fmt(d.kpis.dq_issues);

  // Monthly line (revenue + orders, twin axes)
  const months = d.monthly.month, rev = d.monthly.revenue, ord = d.monthly.orders;
  Plotly.newPlot('monthly', [
    {x: months, y: rev, name:'Revenue', mode:'lines+markers'},
    {x: months, y: ord, name:'Orders', mode:'lines+markers', yaxis:'y2'}
  ], {
    title:'Monthly KPIs', margin:{t:30,r:40,l:50,b:40},
    yaxis:{title:'Revenue'}, yaxis2:{title:'Orders', overlaying:'y', side:'right'}
  }, {displayModeBar:false, responsive:true});

  // Top products bar
  const tp = d.top_products, px = tp.map(t=>t.product_id), py = tp.map(t=>t.revenue);
  Plotly.newPlot('top', [{x:px, y:py, type:'bar', name:'Revenue'}],
    {title:'Top 10 Products by Revenue', margin:{t:30,r:10,l:40,b:80}, xaxis:{tickangle:-45}},
    {displayModeBar:false, responsive:true});

  // Category share pie
  const cx = d.category.map(c=>c.category), cy = d.category.map(c=>c.revenue);
  Plotly.newPlot('cat', [{labels: cx, values: cy, type:'pie', textinfo:'label+percent'}],
    {title:'Category Contribution', margin:{t:30,r:10,l:10,b:10}},
    {displayModeBar:false, responsive:true});
});
</script>
</body>
</html>
"""
(DOCS / "index.html").write_text(html, encoding="utf-8")
print(f"✅ Wrote {DOCS/'index.html'} and {DOCS/'data.json'}")
