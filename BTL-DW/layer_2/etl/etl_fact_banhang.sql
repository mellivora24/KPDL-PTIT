WITH SourceData AS (
    -- 1. Extract + Join
    SELECT
        ddh.MaKhachHang,
        mhdd.MaMatHang,
        ddh.NgayDat,

        mhdd.SoLuongDat,
        mhdd.GiaDat,

        -- tách tháng năm
        MONTH(ddh.NgayDat) AS Thang,
        YEAR(ddh.NgayDat) AS Nam

    FROM IDB.dbo.DonDatHang ddh

    INNER JOIN IDB.dbo.MatHangDuocDat mhdd
        ON ddh.MaDonHang = mhdd.MaDonHang

    -- validate dimension
    INNER JOIN dbo.Dim_KhachHang dk
        ON ddh.MaKhachHang = dk.MaKhachHang

    INNER JOIN dbo.Dim_MatHang dm
        ON mhdd.MaMatHang = dm.MaMatHang
),

AggregatedData AS (
    -- 2 + 3 + 4: group + calculate
    SELECT
        MaKhachHang,
        MaMatHang,
        Thang,
        Nam,

        SUM(SoLuongDat) AS SoLuongBan,
        SUM(SoLuongDat * GiaDat) AS DoanhThu
    FROM SourceData
    GROUP BY
        MaKhachHang,
        MaMatHang,
        Thang,
        Nam
),

FinalData AS (
    -- 5. lookup time key
    SELECT
        dtg.MaThoiGian,
        a.MaMatHang,
        a.MaKhachHang,
        a.SoLuongBan,
        a.DoanhThu
    FROM AggregatedData a

    INNER JOIN dbo.Dim_ThoiGian dtg
        ON a.Thang = dtg.Thang
       AND a.Nam   = dtg.Nam
)

-- 6. Load
INSERT INTO dbo.Fact_BanHang (
    MaThoiGian,
    MaMatHang,
    MaKhachHang,
    SoLuongBan,
    DoanhThu
)
SELECT *
FROM FinalData;