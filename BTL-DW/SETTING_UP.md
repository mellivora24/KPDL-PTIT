# Hướng dẫn Cài đặt và Cấu hình Hệ thống Kho dữ liệu (DW) - PTIT

Tài liệu này tổng hợp quy trình thiết lập môi trường và xử lý lỗi kết nối cho dự án Kho dữ liệu và Khai phá dữ liệu (Mô hình Fact Constellation)[cite: 1].

## 1. Lựa chọn Phiên bản Công cụ (Tool Versions)
Để đảm bảo tính đồng bộ và tránh lỗi "Version Mismatch", nhóm thống nhất sử dụng hệ sinh thái Microsoft 2022:

*   **Hệ quản trị CSDL:** SQL Server 2022 Developer Edition[cite: 1].
    *   *Lưu ý:* Khi cài đặt phải chọn tính năng **Analysis Services** ở chế độ **Multidimensional Mode**[cite: 1].
*   **Công cụ quản lý:** SQL Server Management Studio (SSMS) v20.x[cite: 1].
*   **Môi trường phát triển:** Visual Studio 2022 Community[cite: 1].
    *   *Workload:* **Data storage and processing**[cite: 1].
    *   *Extensions bắt buộc:* SQL Server Integration Services (SSIS) và SQL Server Analysis Services (SSAS) Projects 2022[cite: 1].

## 2. Quy trình Thực hiện Indexing và Nạp dữ liệu
Để tối ưu hiệu năng cho các bảng Fact lớn (Sales, Inventory), quy trình nạp dữ liệu được thực hiện như sau[cite: 1]:

1.  **Thiết lập Metadata:** Định nghĩa cấu trúc bảng, kiểu dữ liệu và ràng buộc trước khi nạp[cite: 1].
2.  **Nạp dữ liệu (ETL):** Thực hiện đổ dữ liệu từ nguồn vào kho qua SSIS[cite: 1].
3.  **Tạo Index:** 
    *   Chỉ tạo **Clustered Index** trước khi nạp để sắp xếp dữ liệu vật lý[cite: 1].
    *   Tạo **Non-clustered Columnstore Index (NCCI)** SAU khi nạp dữ liệu để tránh giảm tốc độ load[cite: 1].

## 3. Xử lý lỗi kết nối Data Source (SSAS)
### Triệu chứng
Khi Process Cube, hệ thống báo lỗi: *“A connection could not be made to the data source with the DataSourceID of 'DATA WAREHOUSE'”*[cite: 1].

### Nguyên nhân
Dịch vụ SQL Server Analysis Services (SSAS) không có quyền truy cập vào Database Engine để đọc dữ liệu từ kho[cite: 1].

### Giải pháp (SQL Script)
Chạy đoạn script sau trên SQL Server Management Studio để cấp quyền cho Service Account của SSAS:

```sql
-- 1. Tạo Login cho Service Account của SSAS từ hệ thống Windows
CREATE LOGIN [NT Service\MSSQLServerOLAPService] FROM WINDOWS;

-- 2. Chuyển sang Database của kho dữ liệu
USE [Tên_DB_Của_Bạn]; 

-- 3. Tạo User cho Login vừa tạo trong Database này
CREATE USER [NT Service\MSSQLServerOLAPService] FOR LOGIN [NT Service\MSSQLServerOLAPService];

-- 4. Cấp quyền đọc dữ liệu cho User này
ALTER ROLE db_datareader ADD MEMBER [NT Service\MSSQLServerOLAPService];
```

## 4. Kiểm tra cuối cùng (Checklist)
*   [ ] SQL Server Engine đã bật giao thức TCP/IP.
*   [ ] SSAS Instance đang chạy ở chế độ Multidimensional[cite: 1].
*   [ ] Data Source trong Visual Studio đã Test Connection thành công[cite: 1].
*   [ ] Project Target Server Version đã chỉnh về SQL Server 2022[cite: 1].
