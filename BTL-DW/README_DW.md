# Quy trình đổ dữ liệu từ IDB vào DW


# Indexing bảng Fact_BanHang

<div align="center">

| Index Name          | Columns                                                       | Type        | Purpose                         | Example                                                             | Explain                                                       |
| ------------------- | ------------------------------------------------------------- | ----------- | ------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------- |
| PK_Fact_TonKho      | MaThoiGian, MaCuaHang, MaMatHang                              | Clustered   | uniqueness, truy vấn đúng grain | `WHERE MaThoiGian=202401 AND MaCuaHang='CH01' AND MaMatHang='MH01'` | Dữ liệu được sắp xếp theo key này → truy cập trực tiếp 1 dòng |
| IX_TonKho_ThoiGian  | MaThoiGian                                                    | B-Tree      | filter theo thời gian           | `WHERE MaThoiGian=202401`                                           | Tìm kiếm theo tháng thay vì scan toàn bảng                        |
| IX_TonKho_CuaHang   | MaCuaHang                                                     | B-Tree      | join Dim_CuaHang                | `JOIN Dim_CuaHang ON MaCuaHang`                                     | Tăng tốc join theo store                                      |
| IX_TonKho_MatHang   | MaMatHang                                                     | B-Tree      | join Dim_MatHang                | `JOIN Dim_MatHang ON MaMatHang`                                     | Tăng tốc join theo sản phẩm                                   |
| IX_NCCI_Fact_TonKho | MaThoiGian, MaCuaHang, MaMatHang, SoLuongTonKho, GiaTriTonKho | Columnstore | scan + aggregate OLAP           | `SELECT MaThoiGian, SUM(GiaTriTonKho) GROUP BY MaThoiGian`          | Đọc theo cột → giảm IO, tối ưu SUM/GROUP BY                   |

</div>

# Indexing bảng Fact_TonKho

<div align="center">

| Index Name           | Columns                                                  | Type        | Purpose                         | Example                                                               | Explain                                 |
| -------------------- | -------------------------------------------------------- | ----------- | ------------------------------- | --------------------------------------------------------------------- | --------------------------------------- |
| PK_Fact_BanHang      | MaThoiGian, MaMatHang, MaKhachHang                       | Clustered   | uniqueness, truy vấn đúng grain | `WHERE MaThoiGian=202401 AND MaMatHang='MH01' AND MaKhachHang='KH01'` | Truy cập nhanh theo key đầy đủ          |
| IX_BanHang_ThoiGian  | MaThoiGian                                               | B-Tree      | filter theo thời gian           | `WHERE MaThoiGian=202401`                                             | Tối ưu truy vấn theo tháng              |
| IX_BanHang_MatHang   | MaMatHang                                                | B-Tree      | join Dim_MatHang                | `JOIN Dim_MatHang ON MaMatHang`                                       | Join nhanh với dimension sản phẩm       |
| IX_BanHang_KhachHang | MaKhachHang                                              | B-Tree      | join Dim_KhachHang              | `JOIN Dim_KhachHang ON MaKhachHang`                                   | Join nhanh với dimension khách hàng     |
| IX_NCCI_Fact_BanHang | MaThoiGian, MaMatHang, MaKhachHang, DoanhThu, SoLuongBan | Columnstore | scan + aggregate OLAP           | `SELECT MaThoiGian, SUM(DoanhThu) GROUP BY MaThoiGian`                | Scan theo cột → cực nhanh cho phân tích |

</div>

# Bảng Dimension Table

| Dimension           | Thuộc tính chính                                                                        | Loại  |
| ------------------- | --------------------------------------------------------------------------------------- | ----- |
| Dim_ThoiGian        | MaThoiGian, Thang, Quy, Nam                                                             | SCD 0 |
| Dim_MatHang         | MaMatHang, MoTa, TrongLuong, KichThuoc, Gia                                             | SCD 1 |
| Dim_CuaHang         | MaCuaHang, MaThanhPho (FK), SDT                                                         | SCD 1 |
| Dim_KhachHang       | MaKhachHang, TenKhachHang, MaThanhPho, LoaiKhachHang, HuongDanVienDuLich, DiaChiBuuDien | SCD 1 |
| Dim_VanPhongDaiDien | MaThanhPho, TenThanhPho, DiaChiVP, Bang                                                 | SCD 1 |

=> Dim_ThoiGian (Time Dimension) là dữ liệu tham chiếu bất biến (immutable reference data),  các thuộc tính như tháng, quý, năm không thay đổi theo thời gian nên được thiết kế theo SCD Type 0.

=> Các dimension còn lại là dữ liệu nghiệp vụ (business data), có thể thay đổi trong thực tế (ví dụ: giá sản phẩm, thông tin khách hàng, cửa hàng), nên được thiết kế theo SCD Type 1 để ghi đè và phản ánh trạng thái hiện tại.

# Giải thích SCD

| Loại  | Ý nghĩa                                      |
| ----- | -------------------------------------------- |
| SCD 0 | Không thay đổi sau khi tạo (immutable)       |
| SCD 1 | Ghi đè trực tiếp khi có thay đổi (overwrite) |
