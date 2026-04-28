-- ============================================================
-- ETL_DIMENSION.SQL
-- Anh xa du lieu tu IDB vao cac bang Dimension cua DW
-- ============================================================

USE DATA_WAREHOUSE;
GO

-- ============================================================
-- 1. DIM_THOIGIAN
-- Nguon  : Derived (tinh toan, khong lay tu IDB)
-- Loai   : SCD Type 0 (khong thay doi)
-- Phuong phap : Pre-populate tu ngay thanh lap VPDD som nhat den thang hien tai
-- MaThoiGian = YYYY * 100 + MM (vi du: 202401)
-- ============================================================
PRINT '>> Dang xu ly Dim_ThoiGian...';

-- Lay moc thoi gian bat dau = thang som nhat trong VPDD
DECLARE @StartDate DATE = (
    SELECT DATEFROMPARTS(YEAR(MIN(ThoiGianThanhLap)), MONTH(MIN(ThoiGianThanhLap)), 1)
    FROM IDB.dbo.VanPhongDaiDien
);
DECLARE @EndDate DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1);
DECLARE @Current DATE = @StartDate;

WHILE @Current <= @EndDate
BEGIN
    DECLARE @MaThoiGian INT  = YEAR(@Current) * 100 + MONTH(@Current);
    DECLARE @Thang      INT  = MONTH(@Current);
    DECLARE @Quy        INT  = CEILING(MONTH(@Current) / 3.0);
    DECLARE @Nam        INT  = YEAR(@Current);

    IF NOT EXISTS (SELECT 1 FROM dbo.Dim_ThoiGian WHERE MaThoiGian = @MaThoiGian)
    BEGIN
        INSERT INTO dbo.Dim_ThoiGian (MaThoiGian, Thang, Quy, Nam)
        VALUES (@MaThoiGian, @Thang, @Quy, @Nam);
    END

    SET @Current = DATEADD(MONTH, 1, @Current);
END

PRINT '   Dim_ThoiGian: OK - ' + CAST((SELECT COUNT(*) FROM dbo.Dim_ThoiGian) AS VARCHAR) + ' ban ghi';
GO

-- ============================================================
-- 2. DIM_VANPHONGDAIDIEN
-- Nguon  : IDB.dbo.VanPhongDaiDien
-- Loai   : SCD Type 1 (ghi de khi co thay doi)
-- Natural Key: MaThanhPho
-- Anh xa thuoc tinh:
--   IDB.MaThanhPho  → DW.MaThanhPho
--   IDB.TenThanhPho → DW.TenThanhPho
--   IDB.DiaChiVP    → DW.DiaChiVP
--   IDB.Bang        → DW.Bang
--   (bo qua ThoiGianThanhLap - khong co trong Dim)
-- ============================================================
PRINT '>> Dang xu ly Dim_VanPhongDaiDien...';

MERGE dbo.Dim_VanPhongDaiDien AS target
USING (
    SELECT MaThanhPho, TenThanhPho, DiaChiVP, Bang
    FROM IDB.dbo.VanPhongDaiDien
) AS source
ON target.MaThanhPho = source.MaThanhPho

-- Ban ghi moi: INSERT
WHEN NOT MATCHED BY TARGET THEN
    INSERT (MaThanhPho, TenThanhPho, DiaChiVP, Bang)
    VALUES (source.MaThanhPho, source.TenThanhPho, source.DiaChiVP, source.Bang)

-- Ban ghi cu co thay doi: UPDATE (SCD Type 1 - ghi de)
WHEN MATCHED AND (
    target.TenThanhPho <> source.TenThanhPho OR
    target.DiaChiVP    <> source.DiaChiVP    OR
    target.Bang        <> source.Bang
) THEN
    UPDATE SET
        target.TenThanhPho = source.TenThanhPho,
        target.DiaChiVP    = source.DiaChiVP,
        target.Bang        = source.Bang;

PRINT '   Dim_VanPhongDaiDien: OK - ' + CAST((SELECT COUNT(*) FROM dbo.Dim_VanPhongDaiDien) AS VARCHAR) + ' ban ghi';
GO

-- ============================================================
-- 3. DIM_CUAHANG
-- Nguon  : IDB.dbo.CuaHang INNER JOIN Dim_VanPhongDaiDien
-- Loai   : SCD Type 1 (ghi de khi co thay doi)
-- Natural Key: MaCuaHang
-- Luu y  : INNER JOIN voi Dim_VanPhongDaiDien de dam bao
--          chi load cua hang thuoc thanh pho da co trong DW
-- Anh xa thuoc tinh:
--   IDB.MaCuaHang   → DW.MaCuaHang
--   IDB.MaThanhPho  → DW.MaThanhPho  (validate qua Dim_VPDD)
--   IDB.SoDienThoai → DW.SDT
--   (bo qua ThoiGianMo - khong co trong Dim)
-- ============================================================
PRINT '>> Dang xu ly Dim_CuaHang...';

MERGE dbo.Dim_CuaHang AS target
USING (
    SELECT ch.MaCuaHang, ch.MaThanhPho, ch.SoDienThoai AS SDT
    FROM IDB.dbo.CuaHang ch
    INNER JOIN dbo.Dim_VanPhongDaiDien dvp   -- chi lay cua hang co VPDD hop le
        ON ch.MaThanhPho = dvp.MaThanhPho
) AS source
ON target.MaCuaHang = source.MaCuaHang

WHEN NOT MATCHED BY TARGET THEN
    INSERT (MaCuaHang, MaThanhPho, SDT)
    VALUES (source.MaCuaHang, source.MaThanhPho, source.SDT)

WHEN MATCHED AND (
    target.MaThanhPho <> source.MaThanhPho OR
    target.SDT        <> source.SDT
) THEN
    UPDATE SET
        target.MaThanhPho = source.MaThanhPho,
        target.SDT        = source.SDT;

PRINT '   Dim_CuaHang: OK - ' + CAST((SELECT COUNT(*) FROM dbo.Dim_CuaHang) AS VARCHAR) + ' ban ghi';
GO

-- ============================================================
-- 4. DIM_MATHANG
-- Nguon  : IDB.dbo.MatHang
-- Loai   : SCD Type 1 (ghi de khi co thay doi)
-- Natural Key: MaMH
-- Anh xa thuoc tinh:
--   IDB.MaMH        → DW.MaMatHang
--   IDB.MoTa        → DW.MoTa
--   IDB.KichCo      → DW.KichThuoc
--   IDB.TrongLuong  → DW.TrongLuong
--   IDB.Gia         → DW.Gia
--   (bo qua ThoiGianNhap - khong co trong Dim)
-- ============================================================
PRINT '>> Dang xu ly Dim_MatHang...';

MERGE dbo.Dim_MatHang AS target
USING (
    SELECT MaMH AS MaMatHang, MoTa, KichCo AS KichThuoc, TrongLuong, Gia
    FROM IDB.dbo.MatHang
) AS source
ON target.MaMatHang = source.MaMatHang

WHEN NOT MATCHED BY TARGET THEN
    INSERT (MaMatHang, MoTa, KichThuoc, TrongLuong, Gia)
    VALUES (source.MaMatHang, source.MoTa, source.KichThuoc, source.TrongLuong, source.Gia)

WHEN MATCHED AND (
    ISNULL(target.MoTa, '')       <> ISNULL(source.MoTa, '')       OR
    ISNULL(target.KichThuoc, '')  <> ISNULL(source.KichThuoc, '')  OR
    ISNULL(target.TrongLuong, 0)  <> ISNULL(source.TrongLuong, 0)  OR
    target.Gia                    <> source.Gia
) THEN
    UPDATE SET
        target.MoTa       = source.MoTa,
        target.KichThuoc  = source.KichThuoc,
        target.TrongLuong = source.TrongLuong,
        target.Gia        = source.Gia;

PRINT '   Dim_MatHang: OK - ' + CAST((SELECT COUNT(*) FROM dbo.Dim_MatHang) AS VARCHAR) + ' ban ghi';
GO

-- ============================================================
-- 5. DIM_KHACHHANG
-- Nguon  : IDB.dbo.KhachHang
--          LEFT JOIN IDB.dbo.KhachHangDuLich  (dl)
--          LEFT JOIN IDB.dbo.KhachHangBuuDien (bd)
-- Loai   : SCD Type 1 (ghi de khi co thay doi)
-- Natural Key: MaKH
-- Logic bien doi LoaiKhachHang (Overlap Generalization):
--   dl.MaKH NOT NULL AND bd.MaKH NOT NULL → 3 (Ca hai)
--   dl.MaKH NOT NULL AND bd.MaKH IS NULL  → 1 (Du lich)
--   dl.MaKH IS NULL  AND bd.MaKH NOT NULL → 2 (Buu dien)
-- ISNULL: dien 'Unknown' cho truong khong thuoc loai tuong ung
-- Anh xa thuoc tinh:
--   IDB.KhachHang.MaKH              → DW.MaKhachHang
--   IDB.KhachHang.TenKH             → DW.TenKhachHang
--   IDB.KhachHang.MaThanhPho        → DW.MaThanhPho (khong FK)
--   Logic CASE                      → DW.LoaiKhachHang
--   IDB.KhachHangDuLich.HDV         → DW.HuongDanVienDuLich
--   IDB.KhachHangBuuDien.DiaChi     → DW.DiaChiBuuDien
-- ============================================================
PRINT '>> Dang xu ly Dim_KhachHang...';

MERGE dbo.Dim_KhachHang AS target
USING (
    SELECT
        kh.MaKH                                         AS MaKhachHang,
        kh.TenKH                                        AS TenKhachHang,
        kh.MaThanhPho,
        CASE
            WHEN dl.MaKH IS NOT NULL AND bd.MaKH IS NOT NULL THEN 3  -- Ca hai
            WHEN dl.MaKH IS NOT NULL AND bd.MaKH IS NULL     THEN 1  -- Du lich
            WHEN dl.MaKH IS NULL     AND bd.MaKH IS NOT NULL THEN 2  -- Buu dien
        END                                             AS LoaiKhachHang,
        ISNULL(dl.HuongDanVienDuLich, 'Unknown')        AS HuongDanVienDuLich,
        ISNULL(bd.DiaChiBuuDien,      'Unknown')        AS DiaChiBuuDien
    FROM IDB.dbo.KhachHang kh
    LEFT JOIN IDB.dbo.KhachHangDuLich  dl ON kh.MaKH = dl.MaKH
    LEFT JOIN IDB.dbo.KhachHangBuuDien bd ON kh.MaKH = bd.MaKH
) AS source
ON target.MaKhachHang = source.MaKhachHang

WHEN NOT MATCHED BY TARGET THEN
    INSERT (MaKhachHang, TenKhachHang, MaThanhPho,
            LoaiKhachHang, HuongDanVienDuLich, DiaChiBuuDien)
    VALUES (source.MaKhachHang, source.TenKhachHang, source.MaThanhPho,
            source.LoaiKhachHang, source.HuongDanVienDuLich, source.DiaChiBuuDien)

WHEN MATCHED AND (
    target.TenKhachHang       <> source.TenKhachHang       OR
    target.MaThanhPho         <> source.MaThanhPho         OR
    target.LoaiKhachHang      <> source.LoaiKhachHang      OR
    target.HuongDanVienDuLich <> source.HuongDanVienDuLich OR
    target.DiaChiBuuDien      <> source.DiaChiBuuDien
) THEN
    UPDATE SET
        target.TenKhachHang       = source.TenKhachHang,
        target.MaThanhPho         = source.MaThanhPho,
        target.LoaiKhachHang      = source.LoaiKhachHang,
        target.HuongDanVienDuLich = source.HuongDanVienDuLich,
        target.DiaChiBuuDien      = source.DiaChiBuuDien;

PRINT '   Dim_KhachHang: OK - ' + CAST((SELECT COUNT(*) FROM dbo.Dim_KhachHang) AS VARCHAR) + ' ban ghi';
GO
