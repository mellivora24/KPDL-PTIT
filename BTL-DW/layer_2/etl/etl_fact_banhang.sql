-- ============================================================
-- ETL_FACT_BANHANG.SQL
-- Anh xa du lieu tu IDB vao Fact_BanHang
-- ============================================================
-- Loai Fact : Transaction (tong hop theo thang)
-- Grain      : 1 dong = 1 khach hang mua 1 mat hang trong 1 thang
-- Measures   :
--   SoLuongBan : SUM(SoLuongDat) trong thang
--   DoanhThu   : SUM(SoLuongDat * GiaDat) trong thang (dung GiaDat - gia thuc te tai thoi diem giao dich)
-- ============================================================
-- Buoc xu ly:
--   1. Acquire  : JOIN DonDatHang + MatHangDuocDat tu IDB
--   2. Organize : GROUP BY (MaKH, MaMH, Thang, Nam)
--                 → tong hop tu muc Ngay sang muc Thang
--   3. Calculate: ThanhTien = SoLuongDat * GiaDat
--                 DoanhThu  = SUM(ThanhTien)
--                 SoLuongBan = SUM(SoLuongDat)
--   4. Lookup   : JOIN Dim_ThoiGian theo (Thang, Nam)
--                 JOIN Dim_KhachHang de validate MaKH
--                 JOIN Dim_MatHang   de validate MaMH
--   5. Load     : INSERT vao Fact_BanHang
-- ============================================================

USE DATA_WAREHOUSE;
GO

PRINT '>> Dang xu ly Fact_BanHang...';

-- Xoa du lieu cu neu can chay lai (tuy chon)
-- TRUNCATE TABLE dbo.Fact_BanHang;

INSERT INTO dbo.Fact_BanHang (
    MaThoiGian,
    MaMatHang,
    MaKhachHang,
    DoanhThu,
    SoLuongBan
)
SELECT
    dt.MaThoiGian,
    agg.MaMatHang,
    agg.MaKhachHang,
    agg.DoanhThu,
    agg.SoLuongBan
FROM (
    -- --------------------------------------------------------
    -- Buoc 1 + 2 + 3: Acquire + Organize + Calculate
    -- JOIN DonDatHang × MatHangDuocDat
    -- GROUP BY (MaKH, MaMH, Thang, Nam)
    -- --------------------------------------------------------
    SELECT
        don.MaKH                                        AS MaKhachHang,
        mhdd.MaMH                                       AS MaMatHang,
        MONTH(don.NgayDatHang)                          AS Thang,
        YEAR(don.NgayDatHang)                           AS Nam,
        SUM(mhdd.SoLuongDat * mhdd.GiaDat)             AS DoanhThu,
        SUM(mhdd.SoLuongDat)                            AS SoLuongBan
    FROM IDB.dbo.DonDatHang don
    JOIN IDB.dbo.MatHangDuocDat mhdd
        ON don.MaDon = mhdd.MaDon
    -- Validate: chi lay khach hang da co trong DW
    INNER JOIN dbo.Dim_KhachHang dk
        ON don.MaKH = dk.MaKhachHang
    -- Validate: chi lay mat hang da co trong DW
    INNER JOIN dbo.Dim_MatHang dm
        ON mhdd.MaMH = dm.MaMatHang
    GROUP BY
        don.MaKH,
        mhdd.MaMH,
        MONTH(don.NgayDatHang),
        YEAR(don.NgayDatHang)
) AS agg

-- Buoc 4: Lookup khoa thoi gian
INNER JOIN dbo.Dim_ThoiGian dt
    ON dt.Thang = agg.Thang
    AND dt.Nam  = agg.Nam

-- Tranh duplicate neu chay ETL nhieu lan
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.Fact_BanHang fb
    WHERE fb.MaThoiGian  = dt.MaThoiGian
      AND fb.MaMatHang   = agg.MaMatHang
      AND fb.MaKhachHang = agg.MaKhachHang
);

PRINT '   Fact_BanHang: OK - ' + CAST((SELECT COUNT(*) FROM dbo.Fact_BanHang) AS VARCHAR) + ' ban ghi';
GO
