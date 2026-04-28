-- ============================================================
-- ETL_FACT_TONKHO.SQL
-- Anh xa du lieu tu IDB vao Fact_TonKho
-- ============================================================
-- Loai Fact : Periodic Snapshot (chot so cuoi thang)
-- Grain      : 1 dong = 1 mat hang tai 1 cua hang trong 1 thang
-- Measures   :
--   SoLuongTonKho : lay ban ghi CUOI THANG (RowNum = 1)
--   GiaTriTonKho  : SoLuongTrongKho * Gia (tu Dim_MatHang)
-- ============================================================
-- Buoc xu ly:
--   1. Acquire  : lay du lieu tu IDB.MatHangDuocLuuTru
--   2. Organize : dung ROW_NUMBER() lay ban ghi cuoi thang
--   3. Calculate: tinh GiaTriTonKho = SoLuong * GiaNiemYet
--   4. Filter   : chi giu RowNum = 1 (snapshot cuoi thang)
--   5. Lookup   : JOIN Dim_ThoiGian theo (Thang, Nam)
--   6. Load     : INSERT vao Fact_TonKho
-- ============================================================
-- PHAI CHAY etl_dimension.sql TRUOC
-- ============================================================

USE DATA_WAREHOUSE;
GO

PRINT '>> Dang xu ly Fact_TonKho...';

-- Xoa du lieu cu
-- TRUNCATE TABLE dbo.Fact_TonKho;

INSERT INTO dbo.Fact_TonKho (
    MaThoiGian,
    MaCuaHang,
    MaMatHang,
    SoLuongTonKho,
    GiaTriTonKho
)
SELECT
    dt.MaThoiGian,
    snapshot.MaCuaHang,
    snapshot.MaMatHang,
    snapshot.SoLuongTrongKho,
    snapshot.SoLuongTrongKho * dm.Gia   AS GiaTriTonKho
FROM (
    -- --------------------------------------------------------
    -- Buoc 2 + 4: Organize + Filter
    -- Dung ROW_NUMBER() de lay ban ghi cuoi thang
    -- PARTITION BY (MaCuaHang, MaMatHang) → moi cap (CH, MH)
    -- ORDER BY ThoiGianLuuTru DESC       → moi nhat = RowNum 1
    -- --------------------------------------------------------
    SELECT
        lt.MaCuaHang,
        lt.MaMH                             AS MaMatHang,
        lt.SoLuongTrongKho                  AS SoLuongTrongKho,
        MONTH(lt.ThoiGianLuuTru)            AS Thang,
        YEAR(lt.ThoiGianLuuTru)             AS Nam,
        ROW_NUMBER() OVER (
            PARTITION BY lt.MaCuaHang, lt.MaMH,
                         YEAR(lt.ThoiGianLuuTru),
                         MONTH(lt.ThoiGianLuuTru)
            ORDER BY lt.ThoiGianLuuTru DESC
        ) AS RowNum
    FROM IDB.dbo.MatHangDuocLuuTru lt
    -- Validate: chi lay cua hang da co trong DW
    INNER JOIN dbo.Dim_CuaHang dc
        ON lt.MaCuaHang = dc.MaCuaHang
    -- Validate: chi lay mat hang da co trong DW
    INNER JOIN dbo.Dim_MatHang dm2
        ON lt.MaMH = dm2.MaMatHang
) AS snapshot

-- Buoc 4: Filter - chi giu ban ghi cuoi thang
INNER JOIN dbo.Dim_MatHang dm
    ON snapshot.MaMatHang = dm.MaMatHang

-- Buoc 5: Lookup khoa thoi gian
INNER JOIN dbo.Dim_ThoiGian dt
    ON dt.Thang = snapshot.Thang
    AND dt.Nam  = snapshot.Nam

WHERE snapshot.RowNum = 1

-- Tranh duplicate neu chay ETL nhieu lan
AND NOT EXISTS (
    SELECT 1 FROM dbo.Fact_TonKho ft
    WHERE ft.MaThoiGian = dt.MaThoiGian
      AND ft.MaCuaHang  = snapshot.MaCuaHang
      AND ft.MaMatHang  = snapshot.MaMatHang
);

PRINT '   Fact_TonKho: OK - ' + CAST((SELECT COUNT(*) FROM dbo.Fact_TonKho) AS VARCHAR) + ' ban ghi';
GO
