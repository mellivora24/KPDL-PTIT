-- ============================================================
-- CUSTOMER_DB - Co so du lieu Van phong dai dien
-- Mo ta: Luu tru thong tin khach hang va phan loai
-- ============================================================
-- Phan loai quan he:
--   PR1: Khoa chinh KHONG chua khoa cua quan he khac
--   PR2: Khoa chinh dong thoi la khoa ngoai (tham chieu PR1)
-- ============================================================

-- CREATE DATABASE CustomerDB;
-- GO

USE CustomerDB;
GO

-- ============================================================
-- DROP (optional)
-- ============================================================
IF OBJECT_ID('dbo.KhachHangBuuDien', 'U') IS NOT NULL DROP TABLE dbo.KhachHangBuuDien;
IF OBJECT_ID('dbo.KhachHangDuLich',  'U') IS NOT NULL DROP TABLE dbo.KhachHangDuLich;
IF OBJECT_ID('dbo.KhachHang',        'U') IS NOT NULL DROP TABLE dbo.KhachHang;
GO

-- ============================================================
-- 1. KHACHHANG
-- Loai quan he: PR1
--   Khoa chinh : MaKH (khong chua khoa cua quan he khac)
--   KAP        : (khong co)
--   NKA        : TenKH, MaTP, NgayDatHangDauTien
-- ============================================================
CREATE TABLE dbo.KhachHang (
    MaKH                VARCHAR(10)   NOT NULL,
    TenKH               NVARCHAR(100) NOT NULL,
    MaTP                VARCHAR(10)   NOT NULL,  -- Thanh pho KH sinh song
    NgayDatHangDauTien  DATE          NULL,       -- NULL neu chua dat hang lan nao

    CONSTRAINT PK_KhachHang PRIMARY KEY (MaKH)
);
GO

-- ============================================================
-- 2. KHACHHANGDULICH
-- Loai quan he: PR2
--   Khoa chinh : MaKH (dong thoi la KAP - khoa ngoai tham chieu KhachHang)
--   KAP        : MaKH
--   NKA        : HuongDanVienDuLich, ThoiGianThemKH
-- Ngu nghia: Overlap Generalization
--   → Mot KH co the vua la KhachHangDuLich vua la KhachHangBuuDien
-- ============================================================
CREATE TABLE dbo.KhachHangDuLich (
    MaKH                VARCHAR(10)   NOT NULL,
    HuongDanVienDuLich  NVARCHAR(100) NOT NULL,
    ThoiGianThemKH      DATETIME2     NOT NULL DEFAULT SYSDATETIME(),

    CONSTRAINT PK_KhachHangDuLich PRIMARY KEY (MaKH),
    CONSTRAINT FK_DuLich_KhachHang
        FOREIGN KEY (MaKH)
        REFERENCES dbo.KhachHang (MaKH)
);
GO

-- ============================================================
-- 3. KHACHHANGBUUDIEN
-- Loai quan he: PR2
--   Khoa chinh : MaKH (dong thoi la KAP - khoa ngoai tham chieu KhachHang)
--   KAP        : MaKH
--   NKA        : DiaChiBuuDien, ThoiGianThemKH
-- Ngu nghia: Overlap Generalization
--   → Mot KH co the vua la KhachHangDuLich vua la KhachHangBuuDien
-- ============================================================
CREATE TABLE dbo.KhachHangBuuDien (
    MaKH            VARCHAR(10)   NOT NULL,
    DiaChiBuuDien   NVARCHAR(200) NOT NULL,
    ThoiGianThemKH      DATETIME2     NOT NULL DEFAULT SYSDATETIME(),

    CONSTRAINT PK_KhachHangBuuDien PRIMARY KEY (MaKH),
    CONSTRAINT FK_BuuDien_KhachHang
        FOREIGN KEY (MaKH)
        REFERENCES dbo.KhachHang (MaKH)
);
GO
