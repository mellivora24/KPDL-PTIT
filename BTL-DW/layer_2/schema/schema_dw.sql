-- ============================================================
-- DATA WAREHOUSE SCHEMA - FACT CONSTELLATION
-- Project: BTL Kho Du Lieu & Khai Pha Du Lieu
-- Updated: Bo FK tu Dim_KhachHang -> Dim_VanPhongDaiDien
--          (VPDD khong quan ly khach hang, chi quan ly cua hang)
-- ============================================================
-- CREATE DATABASE DATA_WAREHOUSE;
-- GO

USE DATA_WAREHOUSE;
GO

-- ============================================================
-- DROP (neu can chay lai tu dau)
-- ============================================================
IF OBJECT_ID('dbo.Fact_BanHang',        'U') IS NOT NULL DROP TABLE dbo.Fact_BanHang;
IF OBJECT_ID('dbo.Fact_TonKho',         'U') IS NOT NULL DROP TABLE dbo.Fact_TonKho;
IF OBJECT_ID('dbo.Dim_KhachHang',       'U') IS NOT NULL DROP TABLE dbo.Dim_KhachHang;
IF OBJECT_ID('dbo.Dim_CuaHang',         'U') IS NOT NULL DROP TABLE dbo.Dim_CuaHang;
IF OBJECT_ID('dbo.Dim_MatHang',         'U') IS NOT NULL DROP TABLE dbo.Dim_MatHang;
IF OBJECT_ID('dbo.Dim_VanPhongDaiDien', 'U') IS NOT NULL DROP TABLE dbo.Dim_VanPhongDaiDien;
IF OBJECT_ID('dbo.Dim_ThoiGian',        'U') IS NOT NULL DROP TABLE dbo.Dim_ThoiGian;
GO

-- ============================================================
-- 1. DIM_THOIGIAN
-- SCD Type 0: khong thay doi sau khi nap
-- Hierarchy: Thang -> Quy -> Nam
-- MaThoiGian: YYYYMM (vi du: 202401 = thang 1 nam 2024)
-- ============================================================
CREATE TABLE dbo.Dim_ThoiGian (
    MaThoiGian  INT      NOT NULL,  -- YYYYMM, e.g. 202401
    Thang       TINYINT  NOT NULL,  -- 1-12
    Quy         TINYINT  NOT NULL,  -- 1-4
    Nam         SMALLINT NOT NULL,  -- e.g. 2024

    CONSTRAINT PK_Dim_ThoiGian PRIMARY KEY (MaThoiGian),
    CONSTRAINT CK_Thang CHECK (Thang BETWEEN 1 AND 12),
    CONSTRAINT CK_Quy   CHECK (Quy   BETWEEN 1 AND 4)
);
GO

-- ============================================================
-- 2. DIM_VANPHONGDAIDIEN
-- SCD Type 1: ghi de khi co thay doi
-- Hierarchy: ThanhPho -> Bang
-- Chi quan ly Cua hang, KHONG quan ly Khach hang
-- ============================================================
CREATE TABLE dbo.Dim_VanPhongDaiDien (
    MaThanhPho  VARCHAR(10)   NOT NULL,
    TenThanhPho NVARCHAR(100) NOT NULL,
    DiaChiVP    NVARCHAR(200) NOT NULL,
    Bang        NVARCHAR(100) NOT NULL,

    CONSTRAINT PK_Dim_VanPhongDaiDien PRIMARY KEY (MaThanhPho)
);
GO

-- ============================================================
-- 3. DIM_CUAHANG
-- SCD Type 1: ghi de khi co thay doi
-- Dung cho Fact_TonKho
-- Dim_CuaHang -> Dim_VanPhongDaiDien (VPDD quan ly CuaHang)
-- Hierarchy: CuaHang -> ThanhPho -> Bang
-- ============================================================
CREATE TABLE dbo.Dim_CuaHang (
    MaCuaHang   VARCHAR(10) NOT NULL,
    MaThanhPho  VARCHAR(10) NOT NULL,  -- FK den VPDD
    SDT         VARCHAR(20) NOT NULL,

    CONSTRAINT PK_Dim_CuaHang PRIMARY KEY (MaCuaHang),
    CONSTRAINT FK_CuaHang_VanPhong
        FOREIGN KEY (MaThanhPho)
        REFERENCES dbo.Dim_VanPhongDaiDien (MaThanhPho)
);
GO

-- ============================================================
-- 4. DIM_MATHANG
-- SCD Type 1: ghi de khi co thay doi (gia, trong luong, ...)
-- Shared dimension: dung cho ca Fact_TonKho va Fact_BanHang
-- ============================================================
CREATE TABLE dbo.Dim_MatHang (
    MaMatHang  VARCHAR(10)   NOT NULL,
    MoTa       NVARCHAR(500) NULL,
    KichThuoc  NVARCHAR(50)  NULL,
    TrongLuong DECIMAL(10,2) NULL,
    Gia        DECIMAL(18,2) NOT NULL,

    CONSTRAINT PK_Dim_MatHang PRIMARY KEY (MaMatHang)
);
GO

-- ============================================================
-- 5. DIM_KHACHHANG
-- SCD Type 1: ghi de khi co thay doi
-- Dung cho Fact_BanHang
-- MaThanhPho: thong tin dia ly cua khach hang
--             KHONG co FK -> Dim_VanPhongDaiDien
--             (VPDD khong quan ly khach hang)
-- LoaiKhachHang:
--   1 = Khach du lich  : HuongDanVienDuLich NOT NULL
--   2 = Khach buu dien : DiaChiBuuDien NOT NULL
--   3 = Ca hai loai    : ca hai NOT NULL
-- ============================================================
CREATE TABLE dbo.Dim_KhachHang (
    MaKhachHang        VARCHAR(10)   NOT NULL,
    TenKhachHang       NVARCHAR(100) NOT NULL,
    MaThanhPho         VARCHAR(10)   NOT NULL,  -- thong tin dia ly, KHONG FK

    -- 1: Du lich | 2: Buu dien | 3: Ca hai
    LoaiKhachHang      TINYINT       NOT NULL,

    -- NULL khi khach khong thuoc loai tuong ung
    HuongDanVienDuLich NVARCHAR(100) NULL,  -- co gia tri khi LoaiKhachHang IN (1,3)
    DiaChiBuuDien      NVARCHAR(200) NULL,  -- co gia tri khi LoaiKhachHang IN (2,3)

    CONSTRAINT PK_Dim_KhachHang PRIMARY KEY (MaKhachHang),
    CONSTRAINT CK_LoaiKhachHang
        CHECK (LoaiKhachHang IN (1, 2, 3)),
    CONSTRAINT CK_KhachHang_DuLich
        CHECK (LoaiKhachHang NOT IN (1, 3) OR HuongDanVienDuLich IS NOT NULL),
    CONSTRAINT CK_KhachHang_BuuDien
        CHECK (LoaiKhachHang NOT IN (2, 3) OR DiaChiBuuDien IS NOT NULL)
    -- NOTE: Khong co FK MaThanhPho -> Dim_VanPhongDaiDien
    --       Vi VPDD quan ly Cua hang, khong quan ly Khach hang
);
GO

-- ============================================================
-- 6. FACT_TONKHO
-- Loai: Periodic Snapshot (chot so cuoi thang)
-- Grain: 1 dong = 1 mat hang tai 1 cua hang trong 1 thang
-- Measures:
--   SoLuongTonKho : semi-additive (khong cong theo ThoiGian)
--   GiaTriTonKho  : semi-additive (khong cong theo ThoiGian)
-- Dimensions: ThoiGian, CuaHang, MatHang
-- ============================================================
CREATE TABLE dbo.Fact_TonKho (
    MaThoiGian    INT           NOT NULL,
    MaCuaHang     VARCHAR(10)   NOT NULL,
    MaMatHang     VARCHAR(10)   NOT NULL,

    SoLuongTonKho INT           NOT NULL DEFAULT 0,
    GiaTriTonKho  DECIMAL(18,2) NOT NULL DEFAULT 0,

    CONSTRAINT PK_Fact_TonKho
        PRIMARY KEY (MaThoiGian, MaCuaHang, MaMatHang),
    CONSTRAINT FK_TonKho_ThoiGian
        FOREIGN KEY (MaThoiGian) REFERENCES dbo.Dim_ThoiGian (MaThoiGian),
    CONSTRAINT FK_TonKho_CuaHang
        FOREIGN KEY (MaCuaHang)  REFERENCES dbo.Dim_CuaHang  (MaCuaHang),
    CONSTRAINT FK_TonKho_MatHang
        FOREIGN KEY (MaMatHang)  REFERENCES dbo.Dim_MatHang  (MaMatHang),
    CONSTRAINT CK_TonKho_SoLuong
        CHECK (SoLuongTonKho >= 0),
    CONSTRAINT CK_TonKho_GiaTri
        CHECK (GiaTriTonKho >= 0)
);
GO

-- ============================================================
-- 7. FACT_BANHANG
-- Loai: Transaction (tong hop theo thang)
-- Grain: 1 dong = 1 khach hang mua 1 mat hang trong 1 thang
-- Measures:
--   DoanhThu   : additive (cong duoc theo moi chieu)
--   SoLuongBan : additive (cong duoc theo moi chieu)
-- Dimensions: ThoiGian, MatHang, KhachHang
-- ============================================================
CREATE TABLE dbo.Fact_BanHang (
    MaThoiGian   INT           NOT NULL,
    MaMatHang    VARCHAR(10)   NOT NULL,
    MaKhachHang  VARCHAR(10)   NOT NULL,

    DoanhThu     DECIMAL(18,2) NOT NULL DEFAULT 0,
    SoLuongBan   INT           NOT NULL DEFAULT 0,

    CONSTRAINT PK_Fact_BanHang
        PRIMARY KEY (MaThoiGian, MaMatHang, MaKhachHang),
    CONSTRAINT FK_BanHang_ThoiGian
        FOREIGN KEY (MaThoiGian)  REFERENCES dbo.Dim_ThoiGian  (MaThoiGian),
    CONSTRAINT FK_BanHang_MatHang
        FOREIGN KEY (MaMatHang)   REFERENCES dbo.Dim_MatHang   (MaMatHang),
    CONSTRAINT FK_BanHang_KhachHang
        FOREIGN KEY (MaKhachHang) REFERENCES dbo.Dim_KhachHang (MaKhachHang),
    CONSTRAINT CK_BanHang_DoanhThu
        CHECK (DoanhThu >= 0),
    CONSTRAINT CK_BanHang_SoLuong
        CHECK (SoLuongBan >= 0)
);
GO
