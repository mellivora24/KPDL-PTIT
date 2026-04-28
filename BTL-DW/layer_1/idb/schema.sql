-- ============================================================
-- IDB - Co so du lieu tich hop (Integrated Database)
-- Ket qua hop nhat: CustomerDB + SalesDB → IDB
-- ============================================================
-- Quy trinh tich hop:
--   Buoc 1: Giai quyet xung dot (dong nghia, da nghia, khoa)
--   Buoc 2: Tron cac thuc the dung quan he hai ngoi
--   Buoc 3: Tron cac quan he
-- ============================================================
-- Giai quyet xung dot da nghia (doi ten thuoc tinh "Thoi gian"):
--   [VanPhongDaiDien].[Thoi gian] → ThoiGianThanhLap
--   [MatHang].[Thoi gian]         → ThoiGianNhap
--   [CuaHang].[Thoi gian]         → ThoiGianMo
--   [MatHangLuuTru].[Thoi gian]   → ThoiGianLuuTru
--   [MatHangDuocDat].[Thoi gian]  → ThoiGianDuocDat
--   [KhachHangDuLich].[Thoi gian] → ThoiGianThemKH
--   [KhachHangBuuDien].[Thoi gian]→ ThoiGianThemKH
-- ============================================================

CREATE DATABASE IDB;
GO

USE IDB;
GO

-- ============================================================
-- DROP (neu can chay lai tu dau)
-- ============================================================
IF OBJECT_ID('dbo.MatHangDuocDat',   'U') IS NOT NULL DROP TABLE dbo.MatHangDuocDat;
IF OBJECT_ID('dbo.MatHangDuocLuuTru','U') IS NOT NULL DROP TABLE dbo.MatHangDuocLuuTru;
IF OBJECT_ID('dbo.DonDatHang',       'U') IS NOT NULL DROP TABLE dbo.DonDatHang;
IF OBJECT_ID('dbo.KhachHangBuuDien', 'U') IS NOT NULL DROP TABLE dbo.KhachHangBuuDien;
IF OBJECT_ID('dbo.KhachHangDuLich',  'U') IS NOT NULL DROP TABLE dbo.KhachHangDuLich;
IF OBJECT_ID('dbo.KhachHang',        'U') IS NOT NULL DROP TABLE dbo.KhachHang;
IF OBJECT_ID('dbo.CuaHang',          'U') IS NOT NULL DROP TABLE dbo.CuaHang;
IF OBJECT_ID('dbo.MatHang',          'U') IS NOT NULL DROP TABLE dbo.MatHang;
IF OBJECT_ID('dbo.VanPhongDaiDien',  'U') IS NOT NULL DROP TABLE dbo.VanPhongDaiDien;
GO

-- ============================================================
-- 1. VANPHONGDAIDIEN
-- Nguon: SalesDB
-- Quan he: 1:N voi CuaHang
-- ============================================================
CREATE TABLE dbo.VanPhongDaiDien (
    MaThanhPho       VARCHAR(10)   NOT NULL,
    TenThanhPho      NVARCHAR(100) NOT NULL,
    DiaChiVP         NVARCHAR(200) NOT NULL,
    Bang             NVARCHAR(100) NOT NULL,
    ThoiGianThanhLap DATETIME2     NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_VanPhongDaiDien PRIMARY KEY (MaThanhPho)
);
GO

-- ============================================================
-- 2. CUAHANG
-- Nguon: SalesDB
-- Quan he: N:1 voi VanPhongDaiDien (FKA: MaThanhPho)
--          N:M voi MatHang qua MatHangDuocLuuTru
-- ============================================================
CREATE TABLE dbo.CuaHang (
    MaCuaHang   VARCHAR(10) NOT NULL,
    MaThanhPho  VARCHAR(10) NOT NULL,   -- FK → VanPhongDaiDien
    SoDienThoai VARCHAR(20) NOT NULL,
    ThoiGianMo  DATETIME2   NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_CuaHang PRIMARY KEY (MaCuaHang),
    CONSTRAINT FK_CuaHang_VanPhong
        FOREIGN KEY (MaThanhPho)
        REFERENCES dbo.VanPhongDaiDien (MaThanhPho)
);
GO

-- ============================================================
-- 3. MATHANG
-- Nguon: SalesDB
-- Quan he: N:M voi CuaHang qua MatHangDuocLuuTru
--          N:M voi DonDatHang qua MatHangDuocDat
-- ============================================================
CREATE TABLE dbo.MatHang (
    MaMH         VARCHAR(10)   NOT NULL,
    MoTa         NVARCHAR(500) NULL,
    KichCo       NVARCHAR(50)  NULL,
    TrongLuong   DECIMAL(10,2) NULL,
    Gia          DECIMAL(18,2) NOT NULL,
    ThoiGianNhap DATETIME2     NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_MatHang PRIMARY KEY (MaMH),
    CONSTRAINT CK_MatHang_Gia CHECK (Gia >= 0)
);
GO

-- ============================================================
-- 4. KHACHHANG
-- Nguon: CustomerDB
-- Quan he: 1:N voi DonDatHang
--          1:1 voi KhachHangDuLich (neu la KH du lich)
--          1:1 voi KhachHangBuuDien (neu la KH buu dien)
--          Overlap Generalization: co the thuoc ca 2 loai
-- Luu y: MaThanhPho la thong tin dia ly (KHONG FK → VanPhongDaiDien)
--        Vi VPDD quan ly CuaHang, khong quan ly KhachHang
-- ============================================================
CREATE TABLE dbo.KhachHang (
    MaKH               VARCHAR(10)   NOT NULL,
    TenKH              NVARCHAR(100) NOT NULL,
    MaThanhPho         VARCHAR(10)   NOT NULL,  -- dia ly, KHONG FK
    NgayDatHangDauTien DATE          NULL,

    CONSTRAINT PK_KhachHang PRIMARY KEY (MaKH)
);
GO

-- ============================================================
-- 5. KHACHHANGDULICH
-- Nguon: CustomerDB
-- Loai: PR2 - khoa chinh dong thoi la khoa ngoai
-- Overlap Generalization: 1 KH co the vua la DuLich vua la BuuDien
-- ============================================================
CREATE TABLE dbo.KhachHangDuLich (
    MaKH               VARCHAR(10)   NOT NULL,
    HuongDanVienDuLich NVARCHAR(100) NOT NULL,
    ThoiGianThemKH     DATETIME2     NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_KhachHangDuLich PRIMARY KEY (MaKH),
    CONSTRAINT FK_DuLich_KhachHang
        FOREIGN KEY (MaKH)
        REFERENCES dbo.KhachHang (MaKH)
);
GO

-- ============================================================
-- 6. KHACHHANGBUUDIEN
-- Nguon: CustomerDB
-- Loai: PR2 - khoa chinh dong thoi la khoa ngoai
-- Overlap Generalization: 1 KH co the vua la DuLich vua la BuuDien
-- ============================================================
CREATE TABLE dbo.KhachHangBuuDien (
    MaKH           VARCHAR(10)   NOT NULL,
    DiaChiBuuDien  NVARCHAR(200) NOT NULL,
    ThoiGianThemKH DATETIME2     NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_KhachHangBuuDien PRIMARY KEY (MaKH),
    CONSTRAINT FK_BuuDien_KhachHang
        FOREIGN KEY (MaKH)
        REFERENCES dbo.KhachHang (MaKH)
);
GO

-- ============================================================
-- 7. DONDATHANG
-- Nguon: SalesDB
-- Quan he: N:1 voi KhachHang (FKA: MaKH)
--          N:M voi MatHang qua MatHangDuocDat
-- Day la quan he "Dat" ket noi KhachHang (CustomerDB)
-- voi DonDatHang (SalesDB) - buoc tron quan he 2 ngoi
-- ============================================================
CREATE TABLE dbo.DonDatHang (
    MaDon       VARCHAR(10) NOT NULL,
    NgayDatHang DATETIME2   NOT NULL DEFAULT GETDATE(),
    MaKH        VARCHAR(10) NOT NULL,   -- FK → KhachHang

    CONSTRAINT PK_DonDatHang PRIMARY KEY (MaDon),
    CONSTRAINT FK_Don_KhachHang
        FOREIGN KEY (MaKH)
        REFERENCES dbo.KhachHang (MaKH)
);
GO

-- ============================================================
-- 8. MATHANGDUOCLUUTRU
-- Nguon: SalesDB
-- Loai: SR1 → Quan he M:N (CuaHang × MatHang) + Snapshot Thang
--   Khoa chinh composite: (MaCuaHang, MaMH, ThoiGianLuuTru)
--   Luu lai lich su ton kho theo tung thang
--   Moi thang 1 ban ghi snapshot cuoi thang cho moi cap (CH, MH)
-- ============================================================
CREATE TABLE dbo.MatHangDuocLuuTru (
    MaCuaHang       VARCHAR(10) NOT NULL,   -- KAP → CuaHang
    MaMH            VARCHAR(10) NOT NULL,   -- KAP → MatHang
    SoLuongTrongKho INT         NOT NULL DEFAULT 0,
    ThoiGianLuuTru  DATETIME2   NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_MatHangDuocLuuTru
        PRIMARY KEY (MaCuaHang, MaMH, ThoiGianLuuTru),
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
-- 9. MATHANGDUOCDAT
-- Nguon: SalesDB
-- Loai: SR1 → Quan he M:N (DonDatHang × MatHang)
--   Khoa chinh composite: (MaDon, MaMH)
--   Ca hai deu la KAP (khoa ngoai trong PK)
-- Luu y: GiaDat = gia THUC TE tai thoi diem giao dich
--        (khac Gia niem yet trong MatHang)
-- ============================================================
CREATE TABLE dbo.MatHangDuocDat (
    MaDon           VARCHAR(10)   NOT NULL,   -- KAP → DonDatHang
    MaMH            VARCHAR(10)   NOT NULL,   -- KAP → MatHang
    SoLuongDat      INT           NOT NULL,
    GiaDat          DECIMAL(18,2) NOT NULL,
    ThoiGianDuocDat DATETIME2     NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_MatHangDuocDat
        PRIMARY KEY (MaDon, MaMH),
    CONSTRAINT FK_DuocDat_Don
        FOREIGN KEY (MaDon)
        REFERENCES dbo.DonDatHang (MaDon),
    CONSTRAINT FK_DuocDat_MatHang
        FOREIGN KEY (MaMH)
        REFERENCES dbo.MatHang (MaMH),
    CONSTRAINT CK_DuocDat_SoLuong CHECK (SoLuongDat > 0),
    CONSTRAINT CK_DuocDat_Gia     CHECK (GiaDat >= 0)
);
GO
