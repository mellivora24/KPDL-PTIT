"""
Sinh du lieu truc tiep vao DW

Quy mo:
  - 10 Van phong dai dien (10 thanh pho, 3 mien)
  - 30 Cua hang (3 CH / thanh pho)
  - 20 Mat hang
  - 200 Khach hang (40% du lich, 40% buu dien, 20% ca hai)
  - Fact_TonKho : 60 thang x 30 CH x 20 MH = 36,000 ban ghi
  - Fact_BanHang: ~60 thang x 200 KH x 1-3 MH = ~24,000 ban ghi

Yeu cau:
  pip install faker pyodbc
"""

import random
from datetime import date
import pyodbc
from faker import Faker

# ============================================================
# CONFIG
# ============================================================
SERVER              = "localhost"   # ← sua thanh instance cua ban
DW_DATABASE         = "DATA_WAREHOUSE"
TRUNCATE_BEFORE_SEED = True
MONTHS_TO_SEED      = 60     # 2021-01 → 2026-04
RANDOM_SEED         = 42
NUM_KHACHHANG       = 200    # 40% DL, 40% BD, 20% CA2

fake = Faker('vi_VN')
random.seed(RANDOM_SEED)

# ============================================================
# CONNECTION
# ============================================================
def get_connection() -> pyodbc.Connection:
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={SERVER};"
        f"Database={DW_DATABASE};"
        f"Trusted_Connection=yes;"
    )
    try:
        conn = pyodbc.connect(conn_str)
        print(f"  Ket noi thanh cong: {SERVER}/{DW_DATABASE}")
        return conn
    except Exception as e:
        print(f"  LOI ket noi: {e}")
        raise

# ============================================================
# DATE HELPERS
# ============================================================
def shift_month(year: int, month: int, delta: int) -> tuple:
    total    = year * 12 + (month - 1) + delta
    new_year = total // 12
    new_month = (total % 12) + 1
    return new_year, new_month


def build_month_keys(months_to_seed: int) -> list:
    """
    Sinh cac thang tu 2021-01 den thang hien tai
    Returns: [(MaThoiGian, Thang, Quy, Nam), ...]
    """
    # Bat dau tu 60 thang truoc
    today     = date.today()
    current_y = today.year
    current_m = today.month

    rows = []
    for i in range(months_to_seed - 1, -1, -1):
        y, m = shift_month(current_y, current_m, -i)
        key  = y * 100 + m
        quy  = ((m - 1) // 3) + 1
        rows.append((key, m, quy, y))
    return rows

# ============================================================
# STATIC DIMENSION DATA
# ============================================================
THANH_PHOS = [
    # (MaThanhPho, TenThanhPho, DiaChiVP, Bang)
    ("TP001", "Ha Noi",      "456 Ba Trieu, Hoan Kiem, Ha Noi",        "Mien Bac"),
    ("TP002", "Hai Phong",   "123 Tran Phu, Hong Bang, Hai Phong",     "Mien Bac"),
    ("TP003", "Quang Ninh",  "789 Le Thanh Tong, Ha Long, Quang Ninh", "Mien Bac"),
    ("TP004", "Nam Dinh",    "321 Tran Hung Dao, TP Nam Dinh",         "Mien Bac"),
    ("TP005", "Da Nang",     "789 Bach Dang, Hai Chau, Da Nang",       "Mien Trung"),
    ("TP006", "Hue",         "12 Le Loi, TP Hue, TT Hue",             "Mien Trung"),
    ("TP007", "Quang Nam",   "45 Hung Vuong, Tam Ky, Quang Nam",      "Mien Trung"),
    ("TP008", "Ho Chi Minh", "123 Nguyen Hue, Q1, TP HCM",            "Mien Nam"),
    ("TP009", "Can Tho",     "456 Hoa Binh, Ninh Kieu, Can Tho",      "Mien Nam"),
    ("TP010", "Dong Nai",    "789 Pham Van Thuan, Bien Hoa, Dong Nai", "Mien Nam"),
]

MAT_HANG_LIST = [
    # (MaMatHang, MoTa, KichThuoc, TrongLuong, Gia)
    ("MH001", "Dien thoai Samsung Galaxy S24", "Large",  0.18, 22000000),
    ("MH002", "Laptop Dell XPS 15",            "Large",  1.86, 35000000),
    ("MH003", "Tai nghe Sony WH-1000XM5",      "Medium", 0.25,  8500000),
    ("MH004", "Man hinh LG 27 inch 4K",        "Large",  5.80, 12000000),
    ("MH005", "Chuot Logitech MX Master 3",    "Small",  0.14,  2500000),
    ("MH006", "Ban phim Keychron K2",          "Medium", 0.88,  2200000),
    ("MH007", "Webcam Logitech C920",          "Small",  0.16,  1800000),
    ("MH008", "Loa JBL Charge 5",             "Medium", 0.96,  3500000),
    ("MH009", "Sac du phong Anker 20000mAh",  "Small",  0.35,   950000),
    ("MH010", "Cap USB-C Anker 2m",           "Small",  0.08,   250000),
    ("MH011", "Dien thoai iPhone 15",         "Medium", 0.17, 25000000),
    ("MH012", "May tinh bang iPad Air",        "Medium", 0.46, 18000000),
    ("MH013", "Dong ho Apple Watch Series 9", "Small",  0.05, 11000000),
    ("MH014", "Loa Harman Kardon Onyx",       "Large",  1.40,  5500000),
    ("MH015", "Balo laptop Targus 15.6",      "Large",  0.60,   850000),
    ("MH016", "Hub USB-C Anker 7-in-1",       "Small",  0.12,   650000),
    ("MH017", "Giay da nam Oxford",           "Medium", 0.90,  1200000),
    ("MH018", "Ao so mi nam Oxford",          "Medium", 0.30,   450000),
    ("MH019", "Quan jeans Levis 501",         "Medium", 0.55,   950000),
    ("MH020", "Giay the thao Nike Air Max",   "Large",  0.85,  3200000),
]


def get_branches() -> list:
    return THANH_PHOS


def get_stores() -> list:
    """30 cua hang, 3 CH / thanh pho"""
    stores = []
    for i, (ma_tp, _, _, _) in enumerate(THANH_PHOS):
        for j in range(3):
            ma_ch = f"CH{i+1:02d}{j+1:02d}"
            sdt   = f"0{random.randint(3,9)}{random.randint(10000000,99999999)}"
            stores.append((ma_ch, ma_tp, sdt))
    return stores


def get_products() -> list:
    return MAT_HANG_LIST


def get_customers() -> list:
    """
    200 KH: 40% du lich, 40% buu dien, 20% ca hai
    Returns: [(MaKhachHang, TenKhachHang, MaThanhPho,
               LoaiKhachHang, HuongDanVienDuLich, DiaChiBuuDien)]
    """
    ma_tp_list = [tp[0] for tp in THANH_PHOS]

    n_dl  = int(NUM_KHACHHANG * 0.40)   # 80 du lich
    n_bd  = int(NUM_KHACHHANG * 0.40)   # 80 buu dien
    n_ca2 = NUM_KHACHHANG - n_dl - n_bd  # 40 ca hai

    types = (['DL'] * n_dl + ['BD'] * n_bd + ['CA2'] * n_ca2)
    random.shuffle(types)

    customers = []
    for i, loai in enumerate(types):
        ma_kh = f"KH{i+1:04d}"
        ten   = fake.name()
        ma_tp = random.choice(ma_tp_list)

        if loai == 'DL':
            loai_int = 1
            hdv      = fake.name()
            dia_chi  = None
        elif loai == 'BD':
            loai_int = 2
            hdv      = None
            dia_chi  = fake.address().replace('\n', ', ')
        else:  # CA2
            loai_int = 3
            hdv      = fake.name()
            dia_chi  = fake.address().replace('\n', ', ')

        customers.append((ma_kh, ten, ma_tp, loai_int, hdv, dia_chi))

    return customers

# ============================================================
# FACT BUILDERS
# ============================================================
def build_fact_tonkho(month_rows, stores, products) -> list:
    """
    Fact_TonKho: Periodic Snapshot cuoi thang
    Grain: 1 dong = 1 mat hang tai 1 cua hang trong 1 thang
    Semi-additive: SoLuongTonKho, GiaTriTonKho
    """
    price_map = {p[0]: p[4] for p in products}

    # Khoi tao ton kho ban dau
    current_stock = {}
    for ma_ch, _, _ in stores:
        for ma_mh, *_ in products:
            current_stock[(ma_ch, ma_mh)] = random.randint(50, 200)

    rows = []
    for ma_thoi_gian, _thang, _quy, _nam in month_rows:
        for ma_ch, _, _ in stores:
            for ma_mh, *_ in products:
                key = (ma_ch, ma_mh)

                # Bien dong ton kho hang thang
                # Co xac suat 25% nhap hang them
                delta = random.randint(-30, 15)
                if random.random() < 0.25:
                    delta += random.randint(20, 60)

                qty = max(0, current_stock[key] + delta)
                current_stock[key] = qty

                gia_tri = round(qty * price_map[ma_mh], 2)
                rows.append((ma_thoi_gian, ma_ch, ma_mh, qty, gia_tri))

    return rows


def build_fact_banhang(month_rows, customers, products) -> list:
    """
    Fact_BanHang: Transaction tong hop theo thang
    Grain: 1 dong = 1 KH mua 1 MH trong 1 thang
    Additive: DoanhThu, SoLuongBan
    GiaDat = GiaNiemYet * (0.85 - 1.00) phan anh khuyen mai
    """
    product_ids = [p[0] for p in products]
    price_map   = {p[0]: p[4] for p in products}

    rows = []
    for ma_thoi_gian, _thang, _quy, _nam in month_rows:
        for ma_kh, *_ in customers:
            # Moi KH mua 1-4 mat hang moi thang
            buy_count      = random.randint(1, 4)
            picked_products = random.sample(product_ids, buy_count)

            for ma_mh in picked_products:
                so_luong   = random.randint(1, 15)
                # GiaDat = gia thuc te tai thoi diem giao dich
                gia_dat    = price_map[ma_mh] * random.uniform(0.85, 1.00)
                doanh_thu  = round(so_luong * gia_dat, 2)
                rows.append((ma_thoi_gian, ma_mh, ma_kh, doanh_thu, so_luong))

    return rows

# ============================================================
# LOADERS
# ============================================================
def clear_tables(cur: pyodbc.Cursor) -> None:
    print("\n[TRUNCATE] Xoa du lieu cu...")

    tables = [
        'Fact_BanHang', 'Fact_TonKho',
        'Dim_KhachHang', 'Dim_CuaHang', 'Dim_MatHang',
        'Dim_VanPhongDaiDien', 'Dim_ThoiGian'
    ]
    for t in tables:
        cur.execute(f"DELETE FROM dbo.{t};")
        print(f"  Cleared: {t}")


def seed_dimensions(cur, month_rows, branches, stores, products, customers) -> None:
    print("\n[DIM] Dang nap Dimensions...")

    cur.executemany(
        "INSERT INTO dbo.Dim_ThoiGian (MaThoiGian, Thang, Quy, Nam) VALUES (?, ?, ?, ?);",
        month_rows
    )
    print(f"  Dim_ThoiGian       : {len(month_rows)} ban ghi")

    cur.executemany(
        "INSERT INTO dbo.Dim_VanPhongDaiDien (MaThanhPho, TenThanhPho, DiaChiVP, Bang) VALUES (?, ?, ?, ?);",
        branches
    )
    print(f"  Dim_VanPhongDaiDien: {len(branches)} ban ghi")

    cur.executemany(
        "INSERT INTO dbo.Dim_CuaHang (MaCuaHang, MaThanhPho, SDT) VALUES (?, ?, ?);",
        stores
    )
    print(f"  Dim_CuaHang        : {len(stores)} ban ghi")

    cur.executemany(
        "INSERT INTO dbo.Dim_MatHang (MaMatHang, MoTa, KichThuoc, TrongLuong, Gia) VALUES (?, ?, ?, ?, ?);",
        products
    )
    print(f"  Dim_MatHang        : {len(products)} ban ghi")

    cur.executemany(
        """INSERT INTO dbo.Dim_KhachHang
            (MaKhachHang, TenKhachHang, MaThanhPho,
             LoaiKhachHang, HuongDanVienDuLich, DiaChiBuuDien)
           VALUES (?, ?, ?, ?, ?, ?);""",
        customers
    )
    print(f"  Dim_KhachHang      : {len(customers)} ban ghi")


def seed_facts(cur, fact_tonkho_rows, fact_banhang_rows) -> None:
    print("\n[FACT] Dang nap Facts...")

    BATCH = 1000
    for i in range(0, len(fact_tonkho_rows), BATCH):
        cur.executemany(
            """INSERT INTO dbo.Fact_TonKho
                (MaThoiGian, MaCuaHang, MaMatHang, SoLuongTonKho, GiaTriTonKho)
               VALUES (?, ?, ?, ?, ?);""",
            fact_tonkho_rows[i:i+BATCH]
        )
    print(f"  Fact_TonKho  : {len(fact_tonkho_rows)} ban ghi")

    for i in range(0, len(fact_banhang_rows), BATCH):
        cur.executemany(
            """INSERT INTO dbo.Fact_BanHang
                (MaThoiGian, MaMatHang, MaKhachHang, DoanhThu, SoLuongBan)
               VALUES (?, ?, ?, ?, ?);""",
            fact_banhang_rows[i:i+BATCH]
        )
    print(f"  Fact_BanHang : {len(fact_banhang_rows)} ban ghi")

# ============================================================
# VALIDATION
# ============================================================
def validate_dw(cur) -> None:
    print("\n" + "="*55)
    print("KIEM TRA DW SAU KHI SEED")
    print("="*55)

    checks = [
        ("Dim_ThoiGian",         "SELECT COUNT(*) FROM dbo.Dim_ThoiGian"),
        ("Dim_VanPhongDaiDien",  "SELECT COUNT(*) FROM dbo.Dim_VanPhongDaiDien"),
        ("Dim_CuaHang",          "SELECT COUNT(*) FROM dbo.Dim_CuaHang"),
        ("Dim_MatHang",          "SELECT COUNT(*) FROM dbo.Dim_MatHang"),
        ("Dim_KhachHang",        "SELECT COUNT(*) FROM dbo.Dim_KhachHang"),
        ("Fact_TonKho",          "SELECT COUNT(*) FROM dbo.Fact_TonKho"),
        ("Fact_BanHang",         "SELECT COUNT(*) FROM dbo.Fact_BanHang"),
    ]

    for ten, query in checks:
        cur.execute(query)
        val = cur.fetchone()[0]
        print(f"  {ten:<25}: {val:>8} ban ghi")

    # Kiem tra Bang trong Dim_VanPhongDaiDien
    print("\n  --- Kiem tra Bang (Mien) ---")
    cur.execute("""
        SELECT Bang, COUNT(*) AS SoTP
        FROM dbo.Dim_VanPhongDaiDien
        GROUP BY Bang ORDER BY Bang
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:<15}: {row[1]} thanh pho")

    # Kiem tra LoaiKhachHang
    print("\n  --- Kiem tra Loai Khach Hang ---")
    cur.execute("""
        SELECT LoaiKhachHang,
               COUNT(*) AS SoKH,
               CAST(COUNT(*)*100.0/SUM(COUNT(*)) OVER() AS DECIMAL(5,1)) AS PhanTram
        FROM dbo.Dim_KhachHang
        GROUP BY LoaiKhachHang
        ORDER BY LoaiKhachHang
    """)
    loai_map = {1: 'Du lich', 2: 'Buu dien', 3: 'Ca hai'}
    for row in cur.fetchall():
        print(f"  Loai {row[0]} ({loai_map.get(row[0],'?'):<8}): {row[1]:>4} KH ({row[2]}%)")

    # Kiem tra FK integrity
    print("\n  --- Kiem tra FK Integrity ---")
    fk_checks = [
        ("Fact_TonKho → Dim_ThoiGian",
         "SELECT COUNT(*) FROM dbo.Fact_TonKho f LEFT JOIN dbo.Dim_ThoiGian d ON f.MaThoiGian=d.MaThoiGian WHERE d.MaThoiGian IS NULL"),
        ("Fact_TonKho → Dim_CuaHang",
         "SELECT COUNT(*) FROM dbo.Fact_TonKho f LEFT JOIN dbo.Dim_CuaHang d ON f.MaCuaHang=d.MaCuaHang WHERE d.MaCuaHang IS NULL"),
        ("Fact_TonKho → Dim_MatHang",
         "SELECT COUNT(*) FROM dbo.Fact_TonKho f LEFT JOIN dbo.Dim_MatHang d ON f.MaMatHang=d.MaMatHang WHERE d.MaMatHang IS NULL"),
        ("Fact_BanHang → Dim_ThoiGian",
         "SELECT COUNT(*) FROM dbo.Fact_BanHang f LEFT JOIN dbo.Dim_ThoiGian d ON f.MaThoiGian=d.MaThoiGian WHERE d.MaThoiGian IS NULL"),
        ("Fact_BanHang → Dim_MatHang",
         "SELECT COUNT(*) FROM dbo.Fact_BanHang f LEFT JOIN dbo.Dim_MatHang d ON f.MaMatHang=d.MaMatHang WHERE d.MaMatHang IS NULL"),
        ("Fact_BanHang → Dim_KhachHang",
         "SELECT COUNT(*) FROM dbo.Fact_BanHang f LEFT JOIN dbo.Dim_KhachHang d ON f.MaKhachHang=d.MaKhachHang WHERE d.MaKhachHang IS NULL"),
        ("Dim_CuaHang → Dim_VPDD",
         "SELECT COUNT(*) FROM dbo.Dim_CuaHang c LEFT JOIN dbo.Dim_VanPhongDaiDien v ON c.MaThanhPho=v.MaThanhPho WHERE v.MaThanhPho IS NULL"),
    ]
    all_ok = True
    for ten, query in fk_checks:
        cur.execute(query)
        val = cur.fetchone()[0]
        status = " - OK" if val == 0 else f"LOI ({val} ban ghi)"
        if val != 0:
            all_ok = False
        print(f"  {ten:<35}: {status}")

    print("\n" + "="*55)
    if all_ok:
        print("   DW SAN SANG CHO SSAS!")
        print("   Co the Deploy va Process Cube duoc roi")
    else:
        print("   DW CON LOI - Can xu ly truoc khi Process Cube")
    print("="*55)

# ============================================================
# MAIN
# ============================================================
def main() -> None:
    print("="*30)
    print("SEED DATA WAREHOUSE")
    print(f"Server  : {SERVER}")
    print(f"Database: {DW_DATABASE}")
    print(f"Thang   : {MONTHS_TO_SEED} thang")
    print(f"KH      : {NUM_KHACHHANG} khach hang")
    print("="*30)

    random.seed(RANDOM_SEED)

    # --- Build data ---
    print("\n[BUILD] Dang tao du lieu...")
    month_rows       = build_month_keys(MONTHS_TO_SEED)
    branches         = get_branches()
    stores           = get_stores()
    products         = get_products()
    customers        = get_customers()
    fact_tonkho_rows = build_fact_tonkho(month_rows, stores, products)
    fact_banhang_rows = build_fact_banhang(month_rows, customers, products)

    print(f"  Months    : {len(month_rows)}")
    print(f"  Branches  : {len(branches)}")
    print(f"  Stores    : {len(stores)}")
    print(f"  Products  : {len(products)}")
    print(f"  Customers : {len(customers)}")
    print(f"  TonKho    : {len(fact_tonkho_rows):,}")
    print(f"  BanHang   : {len(fact_banhang_rows):,}")

    # --- Load vao DB ---
    conn = get_connection()
    try:
        cur = conn.cursor()

        if TRUNCATE_BEFORE_SEED:
            clear_tables(cur)
            conn.commit()

        seed_dimensions(cur, month_rows, branches, stores, products, customers)
        conn.commit()

        seed_facts(cur, fact_tonkho_rows, fact_banhang_rows)
        conn.commit()

        print("\nSeed hoan tat!")

        # --- Validate ---
        validate_dw(cur)

    except Exception as e:
        conn.rollback()
        print(f"\nLOI: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()