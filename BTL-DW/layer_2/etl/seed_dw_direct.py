import random
from datetime import date
import pyodbc
from faker import Faker

# ============================================================
# CONFIG
# ============================================================
SERVER      = "localhost"
DW_DATABASE = "DATA_WAREHOUSE"

RANDOM_SEED = 42
fake = Faker('vi_VN')
random.seed(RANDOM_SEED)

# TARGET
NUM_BRANCHES   = 63
NUM_STORES     = 252
NUM_PRODUCTS   = 99
NUM_CUSTOMERS  = 500
NUM_MONTHS     = 61

TARGET_TONKHO  = 1_216_778
TARGET_BANHANG = 991_180

# ============================================================
# CONNECTION
# ============================================================
def get_connection():
    return pyodbc.connect(
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={SERVER};Database={DW_DATABASE};Trusted_Connection=yes;"
    )

# ============================================================
# TIME
# ============================================================
def build_month_keys(n):
    today = date.today()
    y, m = today.year, today.month

    rows = []
    for i in range(n-1, -1, -1):
        total = y * 12 + (m - 1) - i
        yy = total // 12
        mm = total % 12 + 1
        key = yy * 100 + mm
        quy = ((mm-1)//3)+1
        rows.append((key, mm, quy, yy))
    return rows

# ============================================================
# DIMENSIONS
# ============================================================
def get_branches():
    rows = []
    for i in range(NUM_BRANCHES):
        rows.append((
            f"TP{i+1:03d}",
            fake.city(),
            fake.address().replace("\n", ", "),
            random.choice(["Mien Bac", "Mien Trung", "Mien Nam"])
        ))
    return rows


def get_stores(branches):
    rows = []
    for i in range(NUM_STORES):
        rows.append((
            f"CH{i+1:04d}",
            random.choice(branches)[0],
            f"0{random.randint(3,9)}{random.randint(10000000,99999999)}"
        ))
    return rows


def get_products():
    rows = []
    for i in range(NUM_PRODUCTS):
        rows.append((
            f"MH{i+1:03d}",
            fake.word() + " " + fake.word(),
            random.choice(["Small","Medium","Large"]),
            round(random.uniform(0.05, 2.5),2),
            random.randint(10_000, 1_000_000)
        ))
    return rows


def get_customers(branches):
    ma_tp_list = [b[0] for b in branches]

    n_dl  = int(NUM_CUSTOMERS * 0.4)
    n_bd  = int(NUM_CUSTOMERS * 0.4)
    n_ca2 = NUM_CUSTOMERS - n_dl - n_bd

    types = ['DL']*n_dl + ['BD']*n_bd + ['CA2']*n_ca2
    random.shuffle(types)

    rows = []
    for i, t in enumerate(types):
        ma_kh = f"KH{i+1:04d}"
        ten   = fake.name()
        ma_tp = random.choice(ma_tp_list)

        if t == 'DL':
            loai, hdv, dc = 1, fake.name(), None
        elif t == 'BD':
            loai, hdv, dc = 2, None, fake.address().replace("\n", ", ")
        else:
            loai, hdv, dc = 3, fake.name(), fake.address().replace("\n", ", ")

        rows.append((ma_kh, ten, ma_tp, loai, hdv, dc))
    return rows

# ============================================================
# FACTS
# ============================================================
def build_fact_tonkho(months, stores, products):
    price = {p[0]: p[4] for p in products}
    rows = []

    for m, *_ in months:
        for ch, *_ in stores:
            for mh, *_ in products:

                if len(rows) >= TARGET_TONKHO:
                    return rows

                qty = random.randint(0, 200)
                val = qty * price[mh]

                rows.append((m, ch, mh, qty, val))

    return rows


def build_fact_banhang(months, customers, products):
    price = {p[0]: p[4] for p in products}
    rows = []

    for m, *_ in months:
        for kh, *_ in customers:
            for mh, *_ in products:

                if len(rows) >= TARGET_BANHANG:
                    return rows

                sl = random.randint(1, 10)
                dt = sl * price[mh] * random.uniform(0.85, 1.0)

                rows.append((m, mh, kh, dt, sl))

    return rows
# ============================================================
# LOAD
# ============================================================
def clear_tables(cur):
    tables = [
        'Fact_BanHang','Fact_TonKho',
        'Dim_KhachHang','Dim_CuaHang','Dim_MatHang',
        'Dim_VanPhongDaiDien','Dim_ThoiGian'
    ]
    for t in tables:
        cur.execute(f"DELETE FROM dbo.{t}")


def insert_batch(cur, query, data, batch=1000):
    for i in range(0, len(data), batch):
        cur.executemany(query, data[i:i+batch])


# ============================================================
# MAIN
# ============================================================
def main():
    print("Generating data...")

    months    = build_month_keys(NUM_MONTHS)
    branches  = get_branches()
    stores    = get_stores(branches)
    products  = get_products()
    customers = get_customers(branches)

    fact_tk = build_fact_tonkho(months, stores, products)
    fact_bh = build_fact_banhang(months, customers, products)

    print("Done build:")
    print(len(fact_tk), "TonKho")
    print(len(fact_bh), "BanHang")

    conn = get_connection()
    cur  = conn.cursor()

    clear_tables(cur)

    insert_batch(cur,
        "INSERT INTO Dim_ThoiGian VALUES (?,?,?,?)",
        months)

    insert_batch(cur,
        "INSERT INTO Dim_VanPhongDaiDien VALUES (?,?,?,?)",
        branches)

    insert_batch(cur,
        "INSERT INTO Dim_CuaHang VALUES (?,?,?)",
        stores)

    insert_batch(cur,
        "INSERT INTO Dim_MatHang VALUES (?,?,?,?,?)",
        products)

    insert_batch(cur,
        """INSERT INTO Dim_KhachHang
        VALUES (?,?,?,?,?,?)""",
        customers)

    insert_batch(cur,
        "INSERT INTO Fact_TonKho VALUES (?,?,?,?,?)",
        fact_tk)

    insert_batch(cur,
        "INSERT INTO Fact_BanHang VALUES (?,?,?,?,?)",
        fact_bh)

    conn.commit()
    conn.close()

    print("DONE ✅")

if __name__ == "__main__":
    main()