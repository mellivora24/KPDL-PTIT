"""
Data Seed for IDB - Integrated Database
Tạo dữ liệu mẫu cho hệ thống kho dữ liệu:
  - VanPhongDaiDien (Branch)
  - CuaHang (Store)
  - MatHang (Product)
  - KhachHang (Customer)
  - DonDatHang + MatHangDuocDat (Orders)
  - MatHangDuocLuuTru (Inventory Snapshot - Monthly)
"""

import pyodbc
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging

# ============================================================
# CONFIG
# ============================================================
SERVER = r'.\SQLEXPRESS'  # SQL Server instance
IDB_DATABASE = 'IDB'
SALES_DB = 'SalesDB'
CUSTOMER_DB = 'CustomerDB'

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# CONNECTION & HELPER
# ============================================================
def get_connection(db_name):
    """Kết nối đến database"""
    conn_str = f'Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={db_name};Trusted_Connection=yes;'
    return pyodbc.connect(conn_str)

def execute_query(conn, query, params=None):
    """Thực thi query"""
    cursor = conn.cursor()
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    conn.commit()
    cursor.close()

# ============================================================
# DATA GENERATION
# ============================================================

class DataGenerator:
    def __init__(self):
        self.conn_idb = get_connection(IDB_DATABASE)
        self.conn_sales = get_connection(SALES_DB)
        self.conn_customer = get_connection(CUSTOMER_DB)
        
    def seed_all(self):
        """Chạy toàn bộ data seed"""
        try:
            logger.info("=== BẮT ĐẦU SEED DỮ LIỆU ===")
            
            self._seed_van_phong_dai_dien()
            self._seed_cua_hang()
            self._seed_mat_hang()
            self._seed_khach_hang()
            self._seed_don_dat_hang()
            self._seed_mat_hang_duoc_luu_tru()
            
            logger.info("=== HOÀN THÀNH SEED DỮ LIỆU ===")
        finally:
            self.conn_idb.close()
            self.conn_sales.close()
            self.conn_customer.close()

    def _seed_van_phong_dai_dien(self):
        """Tạo Van Phong Dai Dien (Branch)"""
        logger.info(">> Tạo Van Phong Dai Dien...")
        
        branches = [
            ('TP001', 'TP. Hồ Chí Minh', '123 Đường Nguyễn Hue, Q.1', 'Thành phố Hồ Chí Minh'),
            ('TP002', 'TP. Hà Nội', '456 Đường Ba Trieu, Q. Hoàn Kiếm', 'Thành phố Hà Nội'),
            ('TP003', 'TP. Đà Nẵng', '789 Đường Bạch Đằng, Q. Hải Châu', 'Thành phố Đà Nẵng'),
        ]
        
        query = """
        INSERT INTO dbo.VanPhongDaiDien (MaThanhPho, TenThanhPho, DiaChiVP, Bang, ThoiGianThanhLap)
        VALUES (?, ?, ?, ?, ?)
        """
        
        for ma, ten, dia_chi, bang in branches:
            try:
                params = (ma, ten, dia_chi, bang, datetime.now())
                execute_query(self.conn_idb, query, params)
                logger.info(f"   ✓ {ma} - {ten}")
            except Exception as e:
                logger.warning(f"   ⚠ {ma} - {str(e)}")

    def _seed_cua_hang(self):
        """Tạo Cua Hang (Store)"""
        logger.info(">> Tạo Cua Hang...")
        
        stores = [
            ('CH001', 'TP001', '0123456789'),
            ('CH002', 'TP001', '0987654321'),
            ('CH003', 'TP002', '0111111111'),
            ('CH004', 'TP002', '0222222222'),
            ('CH005', 'TP003', '0333333333'),
        ]
        
        query = """
        INSERT INTO dbo.CuaHang (MaCuaHang, MaThanhPho, SoDienThoai, ThoiGianMo)
        VALUES (?, ?, ?, ?)
        """
        
        for ma, ma_tp, phone in stores:
            try:
                params = (ma, ma_tp, phone, datetime.now())
                execute_query(self.conn_idb, query, params)
                logger.info(f"   ✓ {ma} - {ma_tp}")
            except Exception as e:
                logger.warning(f"   ⚠ {ma} - {str(e)}")

    def _seed_mat_hang(self):
        """Tạo Mat Hang (Product)"""
        logger.info(">> Tạo Mat Hang...")
        
        products = [
            ('MH001', 'Áo sơ mi nam', 'M, L, XL', 0.3, 150000),
            ('MH002', 'Quần jean nam', 'M, L, XL', 0.6, 250000),
            ('MH003', 'Áo phông nữ', 'S, M, L', 0.2, 100000),
            ('MH004', 'Váy midi nữ', 'S, M, L, XL', 0.4, 300000),
            ('MH005', 'Giày thể thao nam', '37-44', 0.4, 400000),
            ('MH006', 'Giày cao gót nữ', '35-41', 0.3, 500000),
            ('MH007', 'Túi xách nữ', 'One Size', 0.8, 600000),
            ('MH008', 'Ví da nam', 'One Size', 0.2, 200000),
            ('MH009', 'Mũ lưỡi trai', 'One Size', 0.15, 80000),
            ('MH010', 'Khăn quàng cổ', 'One Size', 0.1, 50000),
            ('MH011', 'Dây nịt da nam', 'One Size', 0.2, 120000),
            ('MH012', 'Vòng tay thời trang', 'One Size', 0.05, 60000),
        ]
        
        query = """
        INSERT INTO dbo.MatHang (MaMH, MoTa, KichCo, TrongLuong, Gia, ThoiGianNhap)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        for ma, mo_ta, kich_co, trong_luong, gia in products:
            try:
                params = (ma, mo_ta, kich_co, trong_luong, gia, datetime.now())
                execute_query(self.conn_idb, query, params)
                logger.info(f"   ✓ {ma} - {mo_ta}")
            except Exception as e:
                logger.warning(f"   ⚠ {ma} - {str(e)}")

    def _seed_khach_hang(self):
        """Tạo Khach Hang (Customer)"""
        logger.info(">> Tạo Khach Hang...")
        
        customers = [
            ('KH001', 'Nguyễn Văn A', 'TP001'),
            ('KH002', 'Trần Thị B', 'TP001'),
            ('KH003', 'Phạm Văn C', 'TP002'),
            ('KH004', 'Lê Thị D', 'TP002'),
            ('KH005', 'Hoàng Văn E', 'TP003'),
            ('KH006', 'Đặng Thị F', 'TP003'),
            ('KH007', 'Vũ Văn G', 'TP001'),
            ('KH008', 'Bùi Thị H', 'TP002'),
            ('KH009', 'Dương Văn I', 'TP003'),
            ('KH010', 'Tô Thị K', 'TP001'),
        ]
        
        query = """
        INSERT INTO dbo.KhachHang (MaKH, TenKH, MaThanhPho, NgayDatHangDauTien)
        VALUES (?, ?, ?, ?)
        """
        
        for ma, ten, ma_tp in customers:
            try:
                # Ngày đặt hàng đầu tiên: 12-60 tháng trước
                ngay_dat = datetime.now() - timedelta(days=random.randint(365, 1830))
                params = (ma, ten, ma_tp, ngay_dat.date())
                execute_query(self.conn_idb, query, params)
                logger.info(f"   ✓ {ma} - {ten}")
            except Exception as e:
                logger.warning(f"   ⚠ {ma} - {str(e)}")

        # Thêm một số khách hàng vào loại Du Lich
        query_du_lich = """
        INSERT INTO dbo.KhachHangDuLich (MaKH, HuongDanVienDuLich, ThoiGianThemKH)
        VALUES (?, ?, ?)
        """
        
        du_lich_customers = ['KH001', 'KH003', 'KH005', 'KH007', 'KH009']
        hdv_names = ['Trần Xuân Phúc', 'Lê Quốc Hùng', 'Phạm Minh Tuấn', 'Ngô Thành Công', 'Đinh Văn Hòa']
        
        for kh, hdv in zip(du_lich_customers, hdv_names):
            try:
                params = (kh, hdv, datetime.now())
                execute_query(self.conn_idb, query_du_lich, params)
                logger.info(f"   ✓ {kh} - Du lich (HDV: {hdv})")
            except Exception as e:
                logger.warning(f"   ⚠ {kh} - Du lich - {str(e)}")

        # Thêm một số khách hàng vào loại Buu Dien
        query_buu_dien = """
        INSERT INTO dbo.KhachHangBuuDien (MaKH, DiaChiBuuDien, ThoiGianThemKH)
        VALUES (?, ?, ?)
        """
        
        buu_dien_customers = ['KH002', 'KH004', 'KH006', 'KH008', 'KH010']
        addresses = ['123 Nguyễn Hue, Q1', '456 Ba Trieu, HK', '789 Bạch Đằng, DN', '321 Lý Thường Kiệt, Q10', '654 Trần Phú, Q5']
        
        for kh, addr in zip(buu_dien_customers, addresses):
            try:
                params = (kh, addr, datetime.now())
                execute_query(self.conn_idb, query_buu_dien, params)
                logger.info(f"   ✓ {kh} - Buu dien")
            except Exception as e:
                logger.warning(f"   ⚠ {kh} - Buu dien - {str(e)}")

    def _seed_don_dat_hang(self):
        """Tạo Don Dat Hang (Order)"""
        logger.info(">> Tạo Don Dat Hang...")
        
        # Tạo 200-300 đơn đặt hàng trong 60 tháng
        customers = ['KH001', 'KH002', 'KH003', 'KH004', 'KH005', 'KH006', 'KH007', 'KH008', 'KH009', 'KH010']
        products = ['MH001', 'MH002', 'MH003', 'MH004', 'MH005', 'MH006', 'MH007', 'MH008', 'MH009', 'MH010', 'MH011', 'MH012']
        
        query_don = """
        INSERT INTO dbo.DonDatHang (MaDon, NgayDatHang, MaKH)
        VALUES (?, ?, ?)
        """
        
        query_detail = """
        INSERT INTO dbo.MatHangDuocDat (MaDon, MaMH, SoLuongDat, GiaDat, ThoiGianDuocDat)
        VALUES (?, ?, ?, ?, ?)
        """
        
        # Sinh đơn ngẫu nhiên từ 60 tháng trước
        base_date = datetime.now() - relativedelta(months=60)
        don_count = 0
        
        for i in range(250):
            don_id = f'DON{1001 + i:04d}'
            order_date = base_date + timedelta(days=random.randint(0, 1825))  # 60 tháng ≈ 1825 ngày
            customer = random.choice(customers)
            
            try:
                params = (don_id, order_date, customer)
                execute_query(self.conn_idb, query_don, params)
                don_count += 1
                
                # Thêm 1-4 mặt hàng cho mỗi đơn
                num_items = random.randint(1, 4)
                selected_products = random.sample(products, min(num_items, len(products)))
                
                for product in selected_products:
                    qty = random.randint(1, 5)
                    # Giá gốc là giá danh sách, nhưng có thể có discount
                    price_factor = random.uniform(0.8, 1.0)  # Giảm 20% hoặc bán đúng giá
                    
                    # Lấy giá từ MatHang (hardcoded ở đây)
                    base_prices = {
                        'MH001': 150000, 'MH002': 250000, 'MH003': 100000, 'MH004': 300000,
                        'MH005': 400000, 'MH006': 500000, 'MH007': 600000, 'MH008': 200000,
                        'MH009': 80000, 'MH010': 50000, 'MH011': 120000, 'MH012': 60000,
                    }
                    actual_price = int(base_prices[product] * price_factor)
                    
                    params_detail = (don_id, product, qty, actual_price, order_date)
                    execute_query(self.conn_idb, query_detail, params_detail)
                
                if don_count % 10 == 0:
                    logger.info(f"   ✓ Đã tạo {don_count} đơn")
            except Exception as e:
                logger.warning(f"   ⚠ {don_id} - {str(e)}")
        
        logger.info(f"   ✓ Tổng cộng: {don_count} đơn đặt hàng")

    def _seed_mat_hang_duoc_luu_tru(self):
        """Tạo Mat Hang Duoc Luu Tru - 2-5 updates/tháng trong 60 tháng"""
        logger.info(">> Tạo Mat Hang Duoc Luu Tru (60 tháng, 2-5 lần cập nhật/tháng)...")
        
        stores = ['CH001', 'CH002', 'CH003', 'CH004', 'CH005']
        products = ['MH001', 'MH002', 'MH003', 'MH004', 'MH005', 'MH006', 'MH007', 'MH008', 'MH009', 'MH010', 'MH011', 'MH012']
        
        query = """
        INSERT INTO dbo.MatHangDuocLuuTru (MaCuaHang, MaMH, SoLuongTrongKho, ThoiGianLuuTru)
        VALUES (?, ?, ?, ?)
        """
        
        # Tạo dữ liệu tồn kho cho 60 tháng gần nhất.
        # Mỗi cặp (CuaHang, MatHang) sẽ có 2-5 lần cập nhật trong tháng,
        # và luôn có 1 bản ghi ở cuối tháng để ETL RowNum=1 có ý nghĩa chốt kỳ.
        base_date = datetime.now() - relativedelta(months=60)
        snapshot_count = 0
        
        for month_offset in range(60):
            current_month_start = base_date + relativedelta(months=month_offset)
            next_month = current_month_start + relativedelta(months=1)
            last_day_of_month = next_month - timedelta(days=1)
            
            # Sinh tồn kho cho mỗi cửa hàng - mặt hàng
            for store in stores:
                for product in products:
                    updates_in_month = random.randint(2, 5)

                    # Luôn có một mốc cuối tháng để đại diện trạng thái chốt kỳ.
                    random_days = []
                    if last_day_of_month.day > 1:
                        max_random_days = min(updates_in_month - 1, last_day_of_month.day - 1)
                        random_days = random.sample(range(1, last_day_of_month.day), max_random_days)

                    update_days = sorted(random_days + [last_day_of_month.day])
                    qty = random.randint(20, 120)

                    for day in update_days:
                        if day == last_day_of_month.day:
                            update_time = last_day_of_month.replace(hour=23, minute=59, second=59, microsecond=0)
                        else:
                            update_time = current_month_start.replace(
                                day=day,
                                hour=random.randint(8, 20),
                                minute=random.randint(0, 59),
                                second=random.randint(0, 59),
                                microsecond=0,
                            )

                        qty_change = random.randint(-15, 20)
                        qty = max(0, qty + qty_change)

                        try:
                            params = (store, product, qty, update_time)
                            execute_query(self.conn_idb, query, params)
                            snapshot_count += 1
                        except Exception as e:
                            logger.warning(f"   ⚠ {store}/{product}/{update_time} - {str(e)}")
            
            if (month_offset + 1) % 6 == 0:
                logger.info(f"   ✓ Đã tạo đến tháng {last_day_of_month.strftime('%m/%Y')}")
        
        logger.info(f"   ✓ Tổng cộng: {snapshot_count} records tồn kho (2-5 lần cập nhật/tháng)")

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    try:
        generator = DataGenerator()
        generator.seed_all()
        logger.info("SEED DỮ LIỆU THÀNH CÔNG!")
    except Exception as e:
        logger.error(f"LỖI SEED DỮ LIỆU: {str(e)}", exc_info=True)
