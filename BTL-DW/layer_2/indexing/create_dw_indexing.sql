-- ============================================================
-- INDEX - TANG VAT LY (Non-clustered)
-- Ho tro cac phep: Roll-up, Drill-down, Slice, Dice
-- ============================================================

-- Fact_TonKho
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TonKho_ThoiGian')
    CREATE NONCLUSTERED INDEX IX_TonKho_ThoiGian ON dbo.Fact_TonKho (MaThoiGian);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TonKho_CuaHang')
    CREATE NONCLUSTERED INDEX IX_TonKho_CuaHang ON dbo.Fact_TonKho (MaCuaHang);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TonKho_MatHang')
    CREATE NONCLUSTERED INDEX IX_TonKho_MatHang ON dbo.Fact_TonKho (MaMatHang);


-- Fact_BanHang
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_BanHang_ThoiGian')
    CREATE NONCLUSTERED INDEX IX_BanHang_ThoiGian ON dbo.Fact_BanHang (MaThoiGian);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_BanHang_MatHang')
    CREATE NONCLUSTERED INDEX IX_BanHang_MatHang ON dbo.Fact_BanHang (MaMatHang);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_BanHang_KhachHang')
    CREATE NONCLUSTERED INDEX IX_BanHang_KhachHang ON dbo.Fact_BanHang (MaKhachHang);


-- Dimension
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_KhachHang_LoaiKH')
    CREATE NONCLUSTERED INDEX IX_KhachHang_LoaiKH ON dbo.Dim_KhachHang (LoaiKhachHang);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CuaHang_ThanhPho')
    CREATE NONCLUSTERED INDEX IX_CuaHang_ThanhPho ON dbo.Dim_CuaHang (MaThanhPho);
GO

-- ============================================================
-- NON-CLUSTERED COLUMNSTORE INDEX - TANG VAT LY
-- Toi uu hoa phep quet (Scan) va tinh tong OLAP
-- ============================================================
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_NCCI_Fact_TonKho')
    CREATE NONCLUSTERED COLUMNSTORE INDEX IX_NCCI_Fact_TonKho
    ON dbo.Fact_TonKho (
        MaThoiGian,
        MaCuaHang,
        MaMatHang,
        SoLuongTonKho,
        GiaTriTonKho
    );
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_NCCI_Fact_BanHang')
    CREATE NONCLUSTERED COLUMNSTORE INDEX IX_NCCI_Fact_BanHang
    ON dbo.Fact_BanHang (
        MaThoiGian,
        MaMatHang,
        MaKhachHang,
        DoanhThu,
        SoLuongBan
    );
GO
