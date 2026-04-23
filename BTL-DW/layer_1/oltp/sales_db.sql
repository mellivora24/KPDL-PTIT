-- ============================================================
-- SALES_DB - Co so du lieu Ban hang
-- Mo ta: Quan ly ha tang kinh doanh, danh muc san pham,
--        giao dich dat hang va ton kho
-- ============================================================
-- Phan loai quan he:
--   PR1: Khoa chinh KHONG chua khoa cua quan he khac
--        → Chuyen thanh Thuc the doc lap
--   SR1: Khoa chinh la KET HOP khoa chinh cua cac thuc the khac
--        → Chuyen thanh quan he Nhieu-Nhieu (M:N)
-- ============================================================

-- CREATE DATABASE SalesDB;
-- GO

USE SalesDB;
GO

-- ============================================================
-- DROP (neu can chay lai tu dau)
-- ============================================================
IF OBJECT_ID('dbo.MatHangDuocDat',   'U') IS NOT NULL DROP TABLE dbo.MatHangDuocDat;
IF OBJECT_ID('dbo.MatHangDuocLuuTru','U') IS NOT NULL DROP TABLE dbo.MatHangDuocLuuTru;
IF OBJECT_ID('dbo.DonDatHang',       'U') IS NOT NULL DROP TABLE dbo.DonDatHang;
IF OBJECT_ID('dbo.CuaHang',          'U') IS NOT NULL DROP TABLE dbo.CuaHang;
IF OBJECT_ID('dbo.MatHang',          'U') IS NOT NULL DROP TABLE dbo.MatHang;
IF OBJECT_ID('dbo.VanPhongDaiDien',  'U') IS NOT NULL DROP TABLE dbo.VanPhongDaiDien;
GO

-- ============================================================
-- 1. VANPHONGDAIDIEN
-- Loai quan he: PR1
--   Khoa chinh : MaThanhPho
--   KAP        : (khong co)
--   FKA        : (khong co)
--   NKA        : TenThanhPho, DiaChiVP, Bang, ThoiGianThanhLap
-- Ngu nghia: Quan ly cap thanh pho, moi thanh pho co 1 VPDD
-- ============================================================
CREATE TABLE dbo.VanPhongDaiDien (
    MaThanhPho      VARCHAR(10)   NOT NULL,
    TenThanhPho     NVARCHAR(100) NOT NULL,
    DiaChiVP        NVARCHAR(200) NOT NULL,
    Bang            NVARCHAR(100) NOT NULL,
    ThoiGianThanhLap DATETIME2    NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_VanPhongDaiDien PRIMARY KEY (MaThanhPho)
);
GO

-- ============================================================
-- 2. CUAHANG
-- Loai quan he: PR1
--   Khoa chinh : MaCuaHang
--   KAP        : (khong co)
--   FKA        : MaThanhPho → VanPhongDaiDien (quan he 1:N)
--   NKA        : SoDienThoai, ThoiGianMo
-- Ngu nghia: Diem ban hang vat ly, thuoc quan ly cua 1 VPDD
-- Anh xa FKA: 1 VPDD quan ly N Cua hang (1:N)
-- ============================================================
CREATE TABLE dbo.CuaHang (
    MaCuaHang    VARCHAR(10)  NOT NULL,
    MaThanhPho   VARCHAR(10)  NOT NULL,   -- FKA → VanPhongDaiDien
    SoDienThoai  VARCHAR(20)  NOT NULL,
    ThoiGianMo   DATETIME2    NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_CuaHang PRIMARY KEY (MaCuaHang),
    CONSTRAINT FK_CuaHang_VanPhong
        FOREIGN KEY (MaThanhPho)
        REFERENCES dbo.VanPhongDaiDien (MaThanhPho)
);
GO

-- ============================================================
-- 3. MATHANG
-- Loai quan he: PR1
--   Khoa chinh : MaMH
--   KAP        : (khong co)
--   FKA        : (khong co)
--   NKA        : MoTa, KichCo, TrongLuong, Gia, ThoiGianNhap
-- Ngu nghia: Danh muc san pham kinh doanh
-- ============================================================
CREATE TABLE dbo.MatHang (
    MaMH         VARCHAR(10)   NOT NULL,
    MoTa         NVARCHAR(500) NULL,
    KichCo       NVARCHAR(50)  NULL,
    TrongLuong   DECIMAL(10,2) NULL,
    Gia          DECIMAL(18,2) NOT NULL,
    ThoiGianNhap DATETIME2     NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_MatHang PRIMARY KEY (MaMH),
    CONSTRAINT CK_MatHang_Gia
        CHECK (Gia >= 0)
);
GO

-- ============================================================
-- 4. DONDATHANG
-- Loai quan he: PR1
--   Khoa chinh : MaDon
--   KAP        : (khong co)
--   FKA        : MaKH → CustomerDB.KhachHang (lien DB)
--   NKA        : NgayDatHang
-- Ngu nghia: Thong tin chung ve giao dich mua sam
-- Luu y: MaKH tham chieu sang CustomerDB
--        (khong dat FK cross-database, quan ly o tang ung dung)
-- ============================================================
CREATE TABLE dbo.DonDatHang (
    MaDon       VARCHAR(10) NOT NULL,
    NgayDatHang DATETIME2   NOT NULL DEFAULT GETDATE(),
    MaKH        VARCHAR(10) NOT NULL,   -- FKA → CustomerDB.KhachHang

    CONSTRAINT PK_DonDatHang PRIMARY KEY (MaDon)
    -- NOTE: Khong dat FK cross-database cho MaKH
    --       Tinh toan ven ven du lieu quan ly o tang IDB
);
GO

-- ============================================================
-- 5. MATHANGDUOCLUUTRU
-- Loai quan he: SR1 → Quan he M:N (CuaHang × MatHang)
--   Khoa chinh : (MaCuaHang, MaMH) - composite
--   KAP        : MaCuaHang, MaMH (ca hai deu la khoa ngoai trong PK)
--   FKA        : (khong co them)
--   NKA        : SoLuongTrongKho, ThoiGianLuuTru
-- Ngu nghia: Quan ly muc do ton kho thuc te
--            1 Cua hang luu tru nhieu Mat hang (M:N)
-- ============================================================
CREATE TABLE dbo.MatHangDuocLuuTru (
    MaCuaHang        VARCHAR(10) NOT NULL,   -- KAP → CuaHang
    MaMH             VARCHAR(10) NOT NULL,   -- KAP → MatHang
    SoLuongTrongKho  INT         NOT NULL DEFAULT 0,
    ThoiGianLuuTru   DATETIME2   NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_MatHangDuocLuuTru
        PRIMARY KEY (MaCuaHang, MaMH),
    CONSTRAINT FK_LuuTru_CuaHang
        FOREIGN KEY (MaCuaHang)
        REFERENCES dbo.CuaHang (MaCuaHang),
    CONSTRAINT FK_LuuTru_MatHang
        FOREIGN KEY (MaMH)
        REFERENCES dbo.MatHang (MaMH),
    CONSTRAINT CK_LuuTru_SoLuong
        CHECK (SoLuongTrongKho >= 0)
);
GO

-- ============================================================
-- 6. MATHANGDUOCDAT
-- Loai quan he: SR1 → Quan he M:N (DonDatHang × MatHang)
--   Khoa chinh : (MaDon, MaMH) - composite
--   KAP        : MaDon, MaMH (ca hai deu la khoa ngoai trong PK)
--   FKA        : (khong co them)
--   NKA        : SoLuongDat, GiaDat, ThoiGianDuocDat
-- Ngu nghia: Chi tiet mat hang trong tung don hang
--            Khach hang co the dat nhieu mat hang trong 1 don (M:N)
-- Luu y: GiaDat la gia THUC TE tai thoi diem giao dich
--        (khac voi Gia niem yet trong MatHang)
-- ============================================================
CREATE TABLE dbo.MatHangDuocDat (
    MaDon            VARCHAR(10)   NOT NULL,   -- KAP → DonDatHang
    MaMH             VARCHAR(10)   NOT NULL,   -- KAP → MatHang
    SoLuongDat       INT           NOT NULL,
    GiaDat           DECIMAL(18,2) NOT NULL,   -- Gia thuc te tai thoi diem giao dich
    ThoiGianDuocDat  DATETIME2     NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_MatHangDuocDat
        PRIMARY KEY (MaDon, MaMH),
    CONSTRAINT FK_DuocDat_Don
        FOREIGN KEY (MaDon)
        REFERENCES dbo.DonDatHang (MaDon),
    CONSTRAINT FK_DuocDat_MatHang
        FOREIGN KEY (MaMH)
        REFERENCES dbo.MatHang (MaMH),
    CONSTRAINT CK_DuocDat_SoLuong
        CHECK (SoLuongDat > 0),
    CONSTRAINT CK_DuocDat_Gia
        CHECK (GiaDat >= 0)
);
GO
