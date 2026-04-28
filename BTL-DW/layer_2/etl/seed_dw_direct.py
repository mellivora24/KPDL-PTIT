"""
Seed data directly into DW (without loading from IDB).

Target model:
- Dim_ThoiGian: month level (YYYYMM)
- Dim_VanPhongDaiDien, Dim_CuaHang, Dim_MatHang, Dim_KhachHang
- Fact_TonKho: 1 row = 1 product at 1 store in 1 month
- Fact_BanHang: 1 row = 1 customer buys 1 product in 1 month
"""

import random
from datetime import date
import pyodbc

# ============================================================
# CONFIG
# ============================================================
SERVER = "localhost"
DW_DATABASE = "DATA_WAREHOUSE"
TRUNCATE_BEFORE_SEED = True
MONTHS_TO_SEED = 60
RANDOM_SEED = 42


# ============================================================
# CONNECTION
# ============================================================
def get_connection(db_name: str) -> pyodbc.Connection:
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=DATA_WAREHOUSE;"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


# ============================================================
# DATE HELPERS
# ============================================================
def shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + delta
    new_year = total // 12
    new_month = (total % 12) + 1
    return new_year, new_month


def build_month_keys(months_to_seed: int) -> list[tuple[int, int, int, int]]:
    """
    Build month-level keys: (MaThoiGian, Thang, Quy, Nam)
    Includes current month and previous N-1 months.
    """
    today = date.today()
    current_y = today.year
    current_m = today.month

    rows = []
    for i in range(months_to_seed - 1, -1, -1):
        y, m = shift_month(current_y, current_m, -i)
        key = y * 100 + m
        quy = ((m - 1) // 3) + 1
        rows.append((key, m, quy, y))
    return rows


# ============================================================
# STATIC DIMENSION DATA
# ============================================================
def get_branches() -> list[tuple[str, str, str, str]]:
    return [
        ("TP001", "TP. Ho Chi Minh", "123 Nguyen Hue, Q1", "Ho Chi Minh"),
        ("TP002", "TP. Ha Noi", "456 Ba Trieu, Hoan Kiem", "Ha Noi"),
        ("TP003", "TP. Da Nang", "789 Bach Dang, Hai Chau", "Da Nang"),
    ]


def get_stores() -> list[tuple[str, str, str]]:
    return [
        ("CH001", "TP001", "0123456789"),
        ("CH002", "TP001", "0987654321"),
        ("CH003", "TP002", "0111111111"),
        ("CH004", "TP002", "0222222222"),
        ("CH005", "TP003", "0333333333"),
    ]


def get_products() -> list[tuple[str, str, str, float, float]]:
    return [
        ("MH001", "Ao so mi nam", "M,L,XL", 0.30, 150000),
        ("MH002", "Quan jean nam", "M,L,XL", 0.60, 250000),
        ("MH003", "Ao phong nu", "S,M,L", 0.20, 100000),
        ("MH004", "Vay midi nu", "S,M,L,XL", 0.40, 300000),
        ("MH005", "Giay the thao nam", "37-44", 0.40, 400000),
        ("MH006", "Giay cao got nu", "35-41", 0.30, 500000),
        ("MH007", "Tui xach nu", "One Size", 0.80, 600000),
        ("MH008", "Vi da nam", "One Size", 0.20, 200000),
        ("MH009", "Mu luoi trai", "One Size", 0.15, 80000),
        ("MH010", "Khan quang co", "One Size", 0.10, 50000),
        ("MH011", "Day nit da nam", "One Size", 0.20, 120000),
        ("MH012", "Vong tay thoi trang", "One Size", 0.05, 60000),
    ]


def get_customers() -> list[tuple[str, str, str, int, str | None, str | None]]:
    # (MaKhachHang, TenKhachHang, MaThanhPho, LoaiKhachHang, HuongDanVienDuLich, DiaChiBuuDien)
    return [
        ("KH001", "Nguyen Van A", "TP001", 1, "Tran Xuan Phuc", None),
        ("KH002", "Tran Thi B", "TP001", 2, None, "123 Nguyen Hue, Q1"),
        ("KH003", "Pham Van C", "TP002", 1, "Le Quoc Hung", None),
        ("KH004", "Le Thi D", "TP002", 2, None, "456 Ba Trieu, HK"),
        ("KH005", "Hoang Van E", "TP003", 1, "Pham Minh Tuan", None),
        ("KH006", "Dang Thi F", "TP003", 2, None, "789 Bach Dang, DN"),
        ("KH007", "Vu Van G", "TP001", 3, "Ngo Thanh Cong", "321 Ly Thuong Kiet, Q10"),
        ("KH008", "Bui Thi H", "TP002", 3, "Dinh Van Hoa", "654 Tran Phu, Q5"),
        ("KH009", "Duong Van I", "TP003", 1, "Vo Quang Minh", None),
        ("KH010", "To Thi K", "TP001", 2, None, "987 Nguyen Trai, Q1"),
    ]


# ============================================================
# FACT BUILDERS
# ============================================================
def build_fact_tonkho(
    month_rows: list[tuple[int, int, int, int]],
    stores: list[tuple[str, str, str]],
    products: list[tuple[str, str, str, float, float]],
) -> list[tuple[int, str, str, int, float]]:
    """
    Build Fact_TonKho at monthly snapshot grain.
    """
    price_map = {p[0]: p[4] for p in products}

    # Initial stock by (store, product)
    current_stock = {}
    for store, _, _ in stores:
        for product, *_ in products:
            current_stock[(store, product)] = random.randint(40, 180)

    rows = []
    for ma_thoi_gian, _thang, _quy, _nam in month_rows:
        for store, _, _ in stores:
            for product, *_ in products:
                key = (store, product)

                # Monthly movement with occasional restock.
                delta = random.randint(-30, 20)
                if random.random() < 0.25:
                    delta += random.randint(10, 40)

                qty = max(0, current_stock[key] + delta)
                current_stock[key] = qty
                value = round(qty * price_map[product], 2)

                rows.append((ma_thoi_gian, store, product, qty, value))
    return rows


def build_fact_banhang(
    month_rows: list[tuple[int, int, int, int]],
    customers: list[tuple[str, str, str, int, str | None, str | None]],
    products: list[tuple[str, str, str, float, float]],
) -> list[tuple[int, str, str, float, int]]:
    """
    Build Fact_BanHang at monthly aggregated grain.
    1 row/customer/product/month, matching ETL grain.
    """
    product_ids = [p[0] for p in products]
    price_map = {p[0]: p[4] for p in products}

    rows = []
    for ma_thoi_gian, _thang, _quy, _nam in month_rows:
        for customer, *_ in customers:
            buy_count = random.randint(1, 3)
            picked_products = random.sample(product_ids, buy_count)

            for product in picked_products:
                so_luong = random.randint(1, 12)
                gia_thuc_te = price_map[product] * random.uniform(0.80, 1.00)
                doanh_thu = round(so_luong * gia_thuc_te, 2)
                rows.append((ma_thoi_gian, product, customer, doanh_thu, so_luong))
    return rows


# ============================================================
# LOADERS
# ============================================================
def clear_tables(cur: pyodbc.Cursor) -> None:
    # Delete in FK-safe order.
    cur.execute("DELETE FROM dbo.Fact_BanHang;")
    cur.execute("DELETE FROM dbo.Fact_TonKho;")
    cur.execute("DELETE FROM dbo.Dim_KhachHang;")
    cur.execute("DELETE FROM dbo.Dim_CuaHang;")
    cur.execute("DELETE FROM dbo.Dim_MatHang;")
    cur.execute("DELETE FROM dbo.Dim_VanPhongDaiDien;")
    cur.execute("DELETE FROM dbo.Dim_ThoiGian;")


def seed_dimensions(cur: pyodbc.Cursor, month_rows, branches, stores, products, customers) -> None:
    cur.executemany(
        "INSERT INTO dbo.Dim_ThoiGian (MaThoiGian, Thang, Quy, Nam) VALUES (?, ?, ?, ?);",
        month_rows,
    )
    cur.executemany(
        "INSERT INTO dbo.Dim_VanPhongDaiDien (MaThanhPho, TenThanhPho, DiaChiVP, Bang) VALUES (?, ?, ?, ?);",
        branches,
    )
    cur.executemany(
        "INSERT INTO dbo.Dim_CuaHang (MaCuaHang, MaThanhPho, SDT) VALUES (?, ?, ?);",
        stores,
    )
    cur.executemany(
        "INSERT INTO dbo.Dim_MatHang (MaMatHang, MoTa, KichThuoc, TrongLuong, Gia) VALUES (?, ?, ?, ?, ?);",
        products,
    )
    cur.executemany(
        """
        INSERT INTO dbo.Dim_KhachHang
            (MaKhachHang, TenKhachHang, MaThanhPho, LoaiKhachHang, HuongDanVienDuLich, DiaChiBuuDien)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        customers,
    )


def seed_facts(cur: pyodbc.Cursor, fact_tonkho_rows, fact_banhang_rows) -> None:
    cur.executemany(
        """
        INSERT INTO dbo.Fact_TonKho (MaThoiGian, MaCuaHang, MaMatHang, SoLuongTonKho, GiaTriTonKho)
        VALUES (?, ?, ?, ?, ?);
        """,
        fact_tonkho_rows,
    )
    cur.executemany(
        """
        INSERT INTO dbo.Fact_BanHang (MaThoiGian, MaMatHang, MaKhachHang, DoanhThu, SoLuongBan)
        VALUES (?, ?, ?, ?, ?);
        """,
        fact_banhang_rows,
    )


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    random.seed(RANDOM_SEED)

    month_rows = build_month_keys(MONTHS_TO_SEED)
    branches = get_branches()
    stores = get_stores()
    products = get_products()
    customers = get_customers()

    fact_tonkho_rows = build_fact_tonkho(month_rows, stores, products)
    fact_banhang_rows = build_fact_banhang(month_rows, customers, products)

    conn = get_connection(DW_DATABASE)
    try:
        cur = conn.cursor()

        if TRUNCATE_BEFORE_SEED:
            clear_tables(cur)

        seed_dimensions(cur, month_rows, branches, stores, products, customers)
        seed_facts(cur, fact_tonkho_rows, fact_banhang_rows)

        conn.commit()

        print("Seed DW completed.")
        print(f"Dim_ThoiGian: {len(month_rows)}")
        print(f"Dim_VanPhongDaiDien: {len(branches)}")
        print(f"Dim_CuaHang: {len(stores)}")
        print(f"Dim_MatHang: {len(products)}")
        print(f"Dim_KhachHang: {len(customers)}")
        print(f"Fact_TonKho: {len(fact_tonkho_rows)}")
        print(f"Fact_BanHang: {len(fact_banhang_rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
