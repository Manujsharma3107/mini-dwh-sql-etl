import os, sqlite3, pandas as pd, streamlit as st
from pathlib import Path

# --- Robust DB path (absolute, next to this file) ---
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str(BASE_DIR / "mini_dwh.sqlite")


# --- Helper: does the DB already have our tables? ---
def _db_has_schema(db_path: str) -> bool:
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
              AND name IN ('fact_sales','dim_product','dim_customer')
            """
        )
        rows = cur.fetchall()
        con.close()
        # Expecting at least these 3 tables after ETL
        return len(rows) >= 3
    except Exception:
        return False


# Build DB on first run (or when schema is missing)
if not _db_has_schema(DB_PATH):
    import etl_pipeline as etl

    etl.make_synthetic()
    etl.fetch_api_sample()
    etl.load_to_sqlite()
    # Clear any cached queries and rerun once so the new tables are visible
    try:
        st.cache_data.clear()
    except Exception:
        pass
    try:
        st.rerun()
    except Exception:
        pass


@st.cache_data
def sql_df(query: str) -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, con)
    con.close()
    return df


# ============ PREMIUM CONFIGURATION ============
st.set_page_config(
    page_title="E-Commerce Analytics | Data Warehouse",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============ MODERN STYLING ============
st.markdown("""
<style>
    /* Import modern font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap');
    
    /* Global styles */
    * { font-family: 'Inter', sans-serif; }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Main container styling */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }
    
    /* Hero section */
    .hero-section {
        background: linear-gradient(to right, #f8fafc 0%, #e2e8f0 100%);
        padding: 2.5rem 2.5rem;
        border-radius: 12px;
        border-left: 6px solid #3b82f6;
        margin-bottom: 2.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
    }
    
    .hero-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        letter-spacing: -0.02em;
        color: #1e293b;
    }
    
    .hero-subtitle {
        font-size: 1rem;
        font-weight: 400;
        color: #64748b;
    }
    
    /* Premium metric cards */
    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
        border-radius: 16px;
        padding: 1.75rem 1.5rem;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
        border: 1px solid rgba(0, 0, 0, 0.05);
        transition: all 0.3s ease;
        height: 100%;
    }
    
    .metric-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.12);
    }
    
    .metric-label {
        font-size: 0.875rem;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.75rem;
    }
    
    .metric-value {
        font-size: 2.25rem;
        font-weight: 800;
        color: #0f172a;
        line-height: 1;
    }
    
    .metric-icon {
        width: 48px;
        height: 48px;
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.5rem;
        margin-bottom: 1rem;
    }
    
    .metric-revenue { background: linear-gradient(135deg, #10b981 0%, #059669 100%); }
    .metric-orders { background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); }
    .metric-aov { background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); }
    .metric-quality { background: linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%); }
    
    /* Section headers */
    .section-header {
        font-size: 1.5rem;
        font-weight: 700;
        color: #0f172a;
        margin: 2.5rem 0 1.25rem 0;
        padding-bottom: 0.75rem;
        border-bottom: 3px solid #e2e8f0;
    }
    
    /* Filter section */
    .filter-section {
        background: #f8fafc;
        padding: 1.5rem;
        border-radius: 16px;
        margin-bottom: 2rem;
        border: 1px solid #e2e8f0;
    }
    
    /* Info boxes */
    .info-box {
        background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
        border-left: 4px solid #3b82f6;
        padding: 1rem 1.25rem;
        border-radius: 8px;
        margin: 1rem 0;
        font-size: 0.95rem;
        line-height: 1.6;
    }
    
    .success-box {
        background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
        border-left: 4px solid #10b981;
        padding: 1.25rem 1.5rem;
        border-radius: 12px;
        margin: 1.5rem 0;
        font-size: 1rem;
        font-weight: 500;
        color: #065f46;
    }
    
    /* Chart containers */
    .chart-container {
        background: white;
        padding: 1.5rem;
        border-radius: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.06);
        margin-bottom: 1.5rem;
        border: 1px solid #e2e8f0;
    }
    
    .chart-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1e293b;
        margin-bottom: 1rem;
    }
    
    /* Download buttons */
    .stDownloadButton button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.625rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        width: 100%;
    }
    
    .stDownloadButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(102, 126, 234, 0.4);
    }
    
    /* Dataframe styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 2px 12px rgba(0, 0, 0, 0.05);
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background: #f8fafc;
        border-radius: 10px;
        font-weight: 600;
        color: #334155;
    }
    
    /* Architecture boxes */
    .arch-box {
        background: white;
        border-radius: 12px;
        padding: 1.25rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border: 2px solid #e2e8f0;
        height: 100%;
    }
    
    .arch-title {
        font-weight: 700;
        font-size: 1rem;
        color: #1e293b;
        margin-bottom: 0.75rem;
    }
    
    .arch-content {
        font-size: 0.875rem;
        color: #475569;
        line-height: 1.8;
    }
</style>
""", unsafe_allow_html=True)

# ============ HERO SECTION ============
st.markdown("""
<div class="hero-section">
    <div class="hero-title">E-Commerce Analytics Dashboard</div>
    <div class="hero-subtitle">Real-time insights from your data warehouse | ETL Pipeline → Star Schema → KPI Views</div>
</div>
""", unsafe_allow_html=True)

# ============ OVERVIEW EXPANDER ============
with st.expander("📊 Architecture Overview", expanded=False):
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="arch-box">
            <div class="arch-title">1️⃣ Data Sources</div>
            <div class="arch-content">
                • CSV files (orders, products)<br>
                • REST API samples<br>
                • Synthetic data generation
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="arch-box">
            <div class="arch-title">2️⃣ ETL Pipeline</div>
            <div class="arch-content">
                • Data cleaning & validation<br>
                • Type conversion<br>
                • Quality checks (DQ views)
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="arch-box">
            <div class="arch-title">3️⃣ Star Schema</div>
            <div class="arch-content">
                • dim_customer, dim_product<br>
                • dim_date, fact_sales<br>
                • Optimized for analytics
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="arch-box">
            <div class="arch-title">4️⃣ Analytics Layer</div>
            <div class="arch-content">
                • SQL views for KPIs<br>
                • Interactive dashboards<br>
                • Data quality monitoring
            </div>
        </div>
        """, unsafe_allow_html=True)

# ============ FILTERS ============
st.markdown('<div class="filter-section">', unsafe_allow_html=True)
st.markdown("### Filters")

months_df = sql_df("SELECT DISTINCT substr(order_date,1,7) AS month FROM fact_sales ORDER BY 1;")
cats_df = sql_df("SELECT DISTINCT category FROM dim_product ORDER BY 1;")
pay_df = sql_df("SELECT DISTINCT payment_method FROM fact_sales ORDER BY 1;")

fcol1, fcol2, fcol3 = st.columns(3)

with fcol1:
    options = months_df["month"].tolist()
    if len(options) >= 2:
        sel_range = st.select_slider("Month Range", options=options, value=(options[0], options[-1]))
    else:
        sel_range = (options[0], options[0]) if options else ("", "")

with fcol2:
    sel_cats = st.multiselect("Categories", cats_df["category"].tolist(), default=cats_df["category"].tolist())

with fcol3:
    sel_pmts = st.multiselect("Payment Methods", pay_df["payment_method"].tolist(), default=pay_df["payment_method"].tolist())

st.markdown('</div>', unsafe_allow_html=True)

# ============ DATA PROCESSING ============
base = sql_df("""
    SELECT fs.*, dp.category, dp.subcategory
    FROM fact_sales fs
    JOIN dim_product dp ON dp.product_id = fs.product_id
    WHERE fs.status IN ('completed','shipped')
""")

base = base[base["category"].isin(sel_cats) & base["payment_method"].isin(sel_pmts)]
base["month"] = base["order_date"].str[:7]
base = base[(base["month"] >= sel_range[0]) & (base["month"] <= sel_range[1])]

# KPI calculations
total_revenue = base["revenue"].sum()
total_orders = base["order_id"].nunique()
avg_order_value = total_revenue / max(total_orders, 1)

dq_nulls = sql_df("SELECT COUNT(*) AS issues FROM dq_nulls")
dq_neg = sql_df("SELECT COUNT(*) AS issues FROM dq_negative_qty")
dq_total = int(dq_nulls["issues"].iloc[0] + dq_neg["issues"].iloc[0])

# ============ METRIC CARDS ============
st.markdown("### 📈 Key Performance Indicators")
m1, m2, m3, m4 = st.columns(4)

metrics = [
    (m1, "💰", "Total Revenue", f"${total_revenue:,.0f}", "metric-revenue"),
    (m2, "🛒", "Total Orders", f"{total_orders:,}", "metric-orders"),
    (m3, "📊", "Avg Order Value", f"${avg_order_value:,.0f}", "metric-aov"),
    (m4, "✅", "Data Quality", f"{dq_total} issues", "metric-quality")
]

for col, icon, label, value, css_class in metrics:
    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-icon {css_class}">{icon}</div>
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
    </div>
    """, unsafe_allow_html=True)

# ============ VISUALIZATIONS ============
st.markdown('<div class="section-header">📊 Revenue & Order Trends</div>', unsafe_allow_html=True)

# Monthly trend
kpi_f = base.groupby("month").agg(
    orders=("order_id", "nunique"),
    revenue=("revenue", "sum")
).reset_index()
kpi_f["aov"] = (kpi_f["revenue"] / kpi_f["orders"]).round(2)

if not kpi_f.empty:
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Monthly Revenue & Orders</div>', unsafe_allow_html=True)
    
    # Show the data table with better styling
    display_kpi = kpi_f.copy()
    display_kpi["revenue"] = display_kpi["revenue"].apply(lambda x: f"${x:,.0f}")
    display_kpi["aov"] = display_kpi["aov"].apply(lambda x: f"${x:,.0f}")
    st.dataframe(display_kpi, use_container_width=True, height=300)
    
    # Line chart for revenue
    st.line_chart(kpi_f.set_index("month")["revenue"], use_container_width=True, height=300)
    st.markdown('</div>', unsafe_allow_html=True)

# ============ TOP PRODUCTS ============
st.markdown('<div class="section-header">🏆 Top Performing Products</div>', unsafe_allow_html=True)

top_f = (
    base.groupby(["product_id", "category", "subcategory"])
    .agg(revenue=("revenue", "sum"), orders=("order_id", "nunique"))
    .reset_index()
    .nlargest(10, "revenue")
)

if not top_f.empty:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Top 10 Products by Revenue</div>', unsafe_allow_html=True)
        st.bar_chart(top_f.set_index("product_id")["revenue"], use_container_width=True, height=400)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Product Details</div>', unsafe_allow_html=True)
        display_top = top_f[["product_id", "category", "revenue"]].copy()
        display_top["revenue"] = display_top["revenue"].apply(lambda x: f"${x:,.0f}")
        st.dataframe(display_top, use_container_width=True, height=400)
        st.markdown('</div>', unsafe_allow_html=True)

# ============ CATEGORY ANALYSIS ============
st.markdown('<div class="section-header">🎯 Category Performance</div>', unsafe_allow_html=True)

cat_f = base.groupby("category").agg(
    revenue=("revenue", "sum"),
    orders=("order_id", "nunique")
).reset_index().sort_values("revenue", ascending=False)

if not cat_f.empty:
    cat_f["pct"] = (100 * cat_f["revenue"] / cat_f["revenue"].sum()).round(2)
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Revenue by Category</div>', unsafe_allow_html=True)
        st.bar_chart(cat_f.set_index("category")["revenue"], use_container_width=True, height=350)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with c2:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Category Breakdown</div>', unsafe_allow_html=True)
        display_cat = cat_f.copy()
        display_cat["revenue"] = display_cat["revenue"].apply(lambda x: f"${x:,.0f}")
        display_cat["pct"] = display_cat["pct"].apply(lambda x: f"{x}%")
        st.dataframe(display_cat, use_container_width=True, height=350)
        st.markdown('</div>', unsafe_allow_html=True)

# ============ INSIGHTS ============
if not kpi_f.empty and not cat_f.empty:
    best_month = kpi_f.loc[kpi_f["revenue"].idxmax(), "month"]
    best_month_rev = kpi_f["revenue"].max()
    top_cat = cat_f.iloc[0]["category"]
    top_cat_rev = cat_f.iloc[0]["revenue"]
    
    st.markdown(f"""
    <div class="success-box">
        <strong>💡 Key Insights:</strong><br>
        • Highest revenue month: <strong>{best_month}</strong> (${best_month_rev:,.0f})<br>
        • Top category: <strong>{top_cat}</strong> contributing ${top_cat_rev:,.0f} ({100*top_cat_rev/total_revenue:.1f}%)<br>
        • Average order value: <strong>${avg_order_value:,.2f}</strong> across {total_orders:,} orders
    </div>
    """, unsafe_allow_html=True)

# ============ DATA TABLES & DOWNLOADS ============
st.markdown('<div class="section-header">📥 Export Data</div>', unsafe_allow_html=True)

d1, d2 = st.columns(2)
with d1:
    st.download_button("📊 Download Monthly KPIs (CSV)", kpi_f.to_csv(index=False), "kpis.csv", "text/csv")
with d2:
    st.download_button("🏆 Download Top Products (CSV)", top_f.to_csv(index=False), "top_products.csv", "text/csv")

# ============ RAW DATA ============

DATA_DIR = BASE_DIR / "data"

with st.expander("🔍 View Raw Data Samples"):
    tab1, tab2, tab3 = st.tabs(["📦 Orders", "📋 Order Items", "📊 Fact Table"])
    
    # --- Orders ---
    with tab1:
        orders_path = DATA_DIR / "orders.csv"
        if orders_path.exists():
            raw_orders = pd.read_csv(orders_path, nrows=20, parse_dates=["order_date"])
            st.dataframe(raw_orders, use_container_width=True, height=400)
            st.download_button(
                "Download full orders.csv",
                orders_path.open("rb"),
                "orders.csv",
                "text/csv"
            )
        else:
            st.error(f"File not found: {orders_path}")

    # --- Order Items ---
    with tab2:
        items_path = DATA_DIR / "order_items.csv"
        if items_path.exists():
            raw_items = pd.read_csv(items_path, nrows=20)
            st.dataframe(raw_items, use_container_width=True, height=400)
            st.download_button(
                "Download full order_items.csv",
                items_path.open("rb"),
                "order_items.csv",
                "text/csv"
            )
        else:
            st.error(f"File not found: {items_path}")

    # --- Fact Table ---
    with tab3:
        try:
            fact_sample = sql_df("SELECT * FROM fact_sales LIMIT 20;")
            st.dataframe(fact_sample, use_container_width=True, height=400)
        except Exception as e:
            st.error(f"Error fetching fact table: {e}")


# ============ DATA QUALITY ============
with st.expander("✅ Data Quality Monitoring"):
    st.markdown("""
    <div class="info-box">
        <strong>Data Quality Checks:</strong> Automated views monitor data integrity throughout the pipeline.
    </div>
    """, unsafe_allow_html=True)
    
    qcol1, qcol2, qcol3 = st.columns(3)
    qcol1.metric("Null Value Issues", int(dq_nulls["issues"].iloc[0]))
    qcol2.metric("Negative Quantity Issues", int(dq_neg["issues"].iloc[0]))
    qcol3.metric("Total Quality Flags", dq_total)
    
    if dq_total == 0:
        st.success("✅ All data quality checks passed!")
    else:
        st.warning(f"⚠️ Found {dq_total} data quality issues that need attention.")

