import sqlite3, pandas as pd, requests
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
ASSETS = Path(__file__).parent / "assets"
DB_PATH = Path(__file__).parent / "mini_dwh.sqlite"

DATA_DIR.mkdir(parents=True, exist_ok=True)
ASSETS.mkdir(parents=True, exist_ok=True)

def make_synthetic():
    import numpy as np
    rng = np.random.default_rng(42)

    products = pd.DataFrame({
        "product_id": range(1, 51),
        "category": rng.choice(["Beverages","Snacks","Electronics","Home"], 50),
        "subcategory": rng.choice(["Premium","Budget","Organic","Standard"], 50),
        "price": rng.integers(50, 2000, 50).astype(float)
    }).drop_duplicates(subset=["product_id"])
    products.to_csv(DATA_DIR/"products.csv", index=False)

    customers = pd.DataFrame({
        "customer_id": range(1, 401),
        "signup_date": pd.date_range("2024-01-01", periods=400, freq="D"),
        "city": rng.choice(["Delhi","Mumbai","Bengaluru","Chandigarh","Jaipur"], 400),
        "state": rng.choice(["DL","MH","KA","PB","RJ"], 400)
    })
    customers.to_csv(DATA_DIR/"customers.csv", index=False)

    orders, items = [], []
    order_id = 1
    for cid in customers["customer_id"]:
        n_orders = int(rng.integers(0, 5))
        for _ in range(n_orders):
            od = pd.Timestamp("2024-01-01") + pd.Timedelta(days=int(rng.integers(0, 270)))
            status = rng.choice(["completed","shipped","cancelled"], p=[0.7,0.2,0.1])
            payment = rng.choice(["UPI","Card","COD"])
            orders.append([order_id, cid, od, status, payment])

            n_items = int(rng.integers(1, 6))
            for _ in range(n_items):
                pid = int(rng.integers(1, 51))
                qty = int(rng.integers(1, 4))
                price = float(products.loc[products.product_id==pid, "price"].sample(1, random_state=42).iloc[0])
                disc = float(rng.choice([0,0.05,0.1,0.2], p=[0.5,0.2,0.2,0.1]))
                items.append([order_id, pid, qty, price, disc])
            order_id += 1

    pd.DataFrame(orders, columns=["order_id","customer_id","order_date","status","payment_method"]) \
      .to_csv(DATA_DIR/"orders.csv", index=False)
    pd.DataFrame(items, columns=["order_id","product_id","quantity","unit_price","discount"]) \
      .to_csv(DATA_DIR/"order_items.csv", index=False)

def fetch_api_sample():
    # tiny API example -> creates a small CSV
    try:
        j = requests.get("https://httpbin.org/json", timeout=5).json()
        pd.DataFrame([{"sample_title": j.get("slideshow",{}).get("title","demo")}]) \
          .to_csv(DATA_DIR/"api_sample.csv", index=False)
    except Exception:
        pd.DataFrame([{"sample_title":"demo"}]).to_csv(DATA_DIR/"api_sample.csv", index=False)

def load_to_sqlite():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
      PRAGMA foreign_keys = ON;
      DROP TABLE IF EXISTS fact_sales;
      DROP TABLE IF EXISTS dim_product;
      DROP TABLE IF EXISTS dim_customer;
      DROP TABLE IF EXISTS dim_date;

      CREATE TABLE dim_product(
        product_id INTEGER PRIMARY KEY,
        category TEXT, subcategory TEXT, price REAL
      );
      CREATE TABLE dim_customer(
        customer_id INTEGER PRIMARY KEY,
        signup_date TEXT, city TEXT, state TEXT
      );
      CREATE TABLE dim_date(
        date_key TEXT PRIMARY KEY,
        year INTEGER, month INTEGER, day INTEGER
      );
      CREATE TABLE fact_sales(
        order_id INTEGER,
        order_date TEXT,
        customer_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        unit_price REAL,
        discount REAL,
        status TEXT,
        payment_method TEXT,
        revenue REAL
      );
    """)

    pd.read_csv(DATA_DIR/"products.csv").to_sql("dim_product", con, if_exists="append", index=False)
    pd.read_csv(DATA_DIR/"customers.csv").to_sql("dim_customer", con, if_exists="append", index=False)

    orders = pd.read_csv(DATA_DIR/"orders.csv", parse_dates=["order_date"])
    items  = pd.read_csv(DATA_DIR/"order_items.csv")
    orders.to_sql("stg_orders", con, if_exists="replace", index=False)
    items.to_sql("stg_order_items", con, if_exists="replace", index=False)

    dates = pd.DataFrame({"date_key": orders["order_date"].dt.date.astype(str)}).drop_duplicates().sort_values("date_key")
    dates["year"] = pd.to_datetime(dates["date_key"]).dt.year
    dates["month"] = pd.to_datetime(dates["date_key"]).dt.month
    dates["day"] = pd.to_datetime(dates["date_key"]).dt.day
    dates.to_sql("dim_date", con, if_exists="append", index=False)

    fact = items.merge(orders, on="order_id", how="left")
    fact["revenue"] = fact["quantity"] * fact["unit_price"] * (1 - fact["discount"].fillna(0.0))
    fact.to_sql("fact_sales", con, if_exists="append", index=False)

    cur.executescript("""
      DROP VIEW IF EXISTS dq_nulls;
      DROP VIEW IF EXISTS dq_negative_qty;
      DROP VIEW IF EXISTS v_monthly_kpis;
      DROP VIEW IF EXISTS v_top_products;
      DROP VIEW IF EXISTS v_category_contribution;

      CREATE VIEW dq_nulls AS
        SELECT * FROM fact_sales
        WHERE order_id IS NULL OR customer_id IS NULL OR product_id IS NULL;

      CREATE VIEW dq_negative_qty AS
        SELECT * FROM fact_sales WHERE quantity < 0 OR unit_price < 0;

      CREATE VIEW v_monthly_kpis AS
        SELECT substr(order_date,1,7) AS month,
               COUNT(DISTINCT order_id) AS orders,
               ROUND(SUM(revenue),2) AS revenue,
               ROUND(SUM(revenue)/NULLIF(COUNT(DISTINCT order_id),0),2) AS aov
        FROM fact_sales
        WHERE status IN ('completed','shipped')
        GROUP BY 1 ORDER BY 1;

      CREATE VIEW v_top_products AS
        SELECT dp.product_id, dp.category, dp.subcategory,
               ROUND(SUM(fs.revenue),2) AS revenue
        FROM fact_sales fs
        JOIN dim_product dp ON dp.product_id = fs.product_id
        WHERE fs.status IN ('completed','shipped')
        GROUP BY 1,2,3
        ORDER BY revenue DESC LIMIT 10;

      CREATE VIEW v_category_contribution AS
        SELECT dp.category,
               ROUND(SUM(fs.revenue),2) AS revenue,
               ROUND(100.0 * SUM(fs.revenue) /
                     (SELECT SUM(revenue) FROM fact_sales WHERE status IN ('completed','shipped')), 2) AS pct
        FROM fact_sales fs
        JOIN dim_product dp ON dp.product_id = fs.product_id
        WHERE fs.status IN ('completed','shipped')
        GROUP BY dp.category
        ORDER BY revenue DESC;
    """)

    con.commit(); con.close()

if __name__ == "__main__":
    make_synthetic()
    fetch_api_sample()
    load_to_sqlite()
    print("✅ Mini DWH built at", DB_PATH)
