# BTL-MINING - Ứng dụng Desktop Quản lý Rủi ro Tín dụng (PyQt6)

Ứng dụng desktop theo mô hình **MVC (Model - View - Controller)** dùng để đánh giá rủi ro tín dụng dựa trên bộ dữ liệu **German Credit Dataset**.

---

## 1) Cấu trúc hệ thống

* **app/config**: cấu hình kết nối SQL Server, đường dẫn model và dữ liệu
* **app/models**: định nghĩa thực thể dữ liệu và schema đặc trưng (feature schema)
* **app/services**: xử lý nghiệp vụ gồm:

  * kết nối database
  * xử lý dữ liệu
  * pipeline machine learning (SVM)
* **app/controllers**: điều phối logic:

  * dự đoán (predict)
  * ra quyết định (approve/reject)
  * ghi nhận kết quả (outcome)
  * huấn luyện lại mô hình (retrain)
* **app/views**: giao diện người dùng sử dụng PyQt6
* **scripts/sql_migrations**: các script khởi tạo bảng trên SQL Server

---

## 2) Cài đặt môi trường

### Yêu cầu hệ thống:

* Python 3.10 trở lên
* SQL Server
* ODBC Driver 17 for SQL Server

---

## 3) Chạy ứng dụng desktop

```powershell
Set-Location BTL-MINING
.\.venv\Scripts\Activate.ps1
python -m app.main
```

---

### Lưu ý:

Nếu chưa có mô hình, hệ thống sẽ tự động:

* huấn luyện mô hình SVM ban đầu
* sử dụng dữ liệu từ: `data/raw/german.data`

---

## 4) Luồng nghiệp vụ hệ thống

1. Nhân viên nhập thông tin hồ sơ khách hàng tại tab **Xét duyệt khách hàng**
2. Hệ thống dự đoán rủi ro tín dụng và hiển thị:

   * điểm rủi ro (risk score)
   * biểu đồ xác suất rủi ro
3. Nhân viên thực hiện quyết định:

   * Duyệt khoản vay
   * Từ chối khoản vay
4. Khi đến hạn vay:

   * cập nhật kết quả thực tế tại tab **Cập nhật trả nợ**
5. Hệ thống sử dụng dữ liệu mới để:

   * huấn luyện lại mô hình trong tab **Học từ dữ liệu mới**

---

## 5) Lưu ý kỹ thuật

* Mô hình hiện tại: **SVM**
* Có sử dụng **class weight** để ưu tiên lớp rủi ro cao (bad) theo cost matrix
* Pipeline xử lý dữ liệu gồm:

  * StandardScaler (chuẩn hóa dữ liệu)
  * OneHotEncoder (mã hóa biến phân loại)
* Dữ liệu huấn luyện lại bao gồm:

  * dữ liệu gốc UCI
  * dữ liệu phản hồi thực tế từ database (`actual_outcome`)
* Giao diện sử dụng:

  * PyQt6
  * matplotlib (hiển thị trực quan mức độ rủi ro)

---

## 6) Cơ chế tự động huấn luyện lại (Auto Retrain)

Khi khởi động ứng dụng, hệ thống có thể tự động kiểm tra dữ liệu phản hồi mới và huấn luyện lại mô hình theo chu kỳ.

### Biến môi trường cấu hình:

* `DM_AUTO_RETRAIN_ENABLED=true` → bật/tắt tự động retrain
* `DM_AUTO_RETRAIN_INTERVAL_MIN=10` → chu kỳ kiểm tra (phút)
* `DM_AUTO_RETRAIN_MIN_NEW_FEEDBACK=30` → số lượng feedback tối thiểu để retrain
* `DM_AUTO_RETRAIN_STATE_PATH=./model/auto_retrain_state.json` → file lưu trạng thái

---

### Nguyên tắc hoạt động:

* Mỗi N phút, hệ thống kiểm tra số lượng bản ghi có `actual_outcome`
* Nếu số feedback mới vượt ngưỡng cấu hình:
  → hệ thống tự động huấn luyện lại mô hình
* Trạng thái lần huấn luyện gần nhất được lưu trong file JSON để tránh retrain lặp lại
