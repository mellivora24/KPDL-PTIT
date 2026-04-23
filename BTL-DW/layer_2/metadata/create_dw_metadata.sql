-- ============================================================
-- CREATE_METADATA.SQL
-- Tao 3 bang Metadata cho Data Warehouse
-- ============================================================
-- 1. metadata_technical_catalog : cau truc vat ly cua DW
-- 2. metadata_mapping           : anh xa IDB → DW
-- 3. metadata_business_glossary : dinh nghia thuat ngu nghiep vu
-- ============================================================

USE DW_PROJECT;
GO

-- ============================================================
-- DROP (neu can chay lai)
-- ============================================================
IF OBJECT_ID('dbo.metadata_business_glossary', 'U') IS NOT NULL DROP TABLE dbo.metadata_business_glossary;
IF OBJECT_ID('dbo.metadata_mapping',           'U') IS NOT NULL DROP TABLE dbo.metadata_mapping;
IF OBJECT_ID('dbo.metadata_technical_catalog', 'U') IS NOT NULL DROP TABLE dbo.metadata_technical_catalog;
GO

-- ============================================================
-- 1. METADATA_TECHNICAL_CATALOG
-- Luu tru cau truc vat ly cua toan bo DW
-- (ten bang, cot, kieu du lieu, khoa, loai do do, SCD)
-- ============================================================
CREATE TABLE dbo.metadata_technical_catalog (
    catalog_id      INT           IDENTITY(1,1) NOT NULL,
    table_name      VARCHAR(100)  NOT NULL,
    table_type      VARCHAR(20)   NOT NULL,   -- FACT | DIMENSION
    column_name     VARCHAR(100)  NOT NULL,
    ordinal_pos     INT           NOT NULL,
    data_type       VARCHAR(50)   NOT NULL,   -- xs:int, xs:string, xs:decimal
    sql_data_type   VARCHAR(50)   NOT NULL,   -- INT, VARCHAR, DECIMAL...
    max_length      INT           NULL,
    is_nullable     BIT           NOT NULL DEFAULT 1,
    is_primary_key  BIT           NOT NULL DEFAULT 0,
    is_foreign_key  BIT           NOT NULL DEFAULT 0,
    ref_table       VARCHAR(100)  NULL,       -- bang duoc tham chieu (neu la FK)
    ref_column      VARCHAR(100)  NULL,
    is_measure      BIT           NOT NULL DEFAULT 0,
    measure_type    VARCHAR(20)   NULL,       -- ADDITIVE | SEMI_ADDITIVE | NULL
    scd_type        VARCHAR(10)   NULL,       -- SCD0 | SCD1 | NULL (cho Fact)
    col_description NVARCHAR(500) NULL,
    CONSTRAINT PK_metadata_technical_catalog PRIMARY KEY (catalog_id)
);
GO

-- Nap du lieu tu sys.columns tu dong
INSERT INTO dbo.metadata_technical_catalog (
    table_name, table_type, column_name, ordinal_pos,
    data_type, sql_data_type, max_length,
    is_nullable, is_primary_key, is_foreign_key,
    ref_table, ref_column,
    is_measure, measure_type, scd_type, col_description
)
SELECT
    t.name                                                      AS table_name,
    CASE
        WHEN t.name LIKE 'Fact_%' THEN 'FACT'
        ELSE 'DIMENSION'
    END                                                         AS table_type,
    c.name                                                      AS column_name,
    c.column_id                                                 AS ordinal_pos,
    'xs:' + tp.name                                            AS data_type,
    UPPER(tp.name)                                              AS sql_data_type,
    CASE WHEN c.max_length = -1 THEN NULL ELSE c.max_length END AS max_length,
    c.is_nullable                                               AS is_nullable,
    CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END        AS is_primary_key,
    CASE WHEN fkc.parent_column_id IS NOT NULL THEN 1 ELSE 0 END AS is_foreign_key,
    reft.name                                                   AS ref_table,
    refc.name                                                   AS ref_column,
    -- Do do: cac cot co ten chua 'SoLuong', 'DoanhThu', 'GiaTri'
    CASE
        WHEN c.name IN ('SoLuongTonKho','GiaTriTonKho','DoanhThu','SoLuongBan') THEN 1
        ELSE 0
    END                                                         AS is_measure,
    CASE
        WHEN c.name IN ('DoanhThu','SoLuongBan')               THEN 'ADDITIVE'
        WHEN c.name IN ('SoLuongTonKho','GiaTriTonKho')         THEN 'SEMI_ADDITIVE'
        ELSE NULL
    END                                                         AS measure_type,
    CASE
        WHEN t.name = 'Dim_ThoiGian'        THEN 'SCD0'
        WHEN t.name LIKE 'Dim_%'            THEN 'SCD1'
        ELSE NULL
    END                                                         AS scd_type,
    NULL                                                        AS col_description
FROM sys.tables t
JOIN sys.columns c ON t.object_id = c.object_id
JOIN sys.types tp  ON c.user_type_id = tp.user_type_id
-- Xac dinh khoa chinh
LEFT JOIN sys.index_columns ic
    ON ic.object_id = c.object_id
    AND ic.column_id = c.column_id
    AND ic.index_id = 1
-- Xac dinh khoa ngoai
LEFT JOIN sys.foreign_key_columns fkc
    ON fkc.parent_object_id = c.object_id
    AND fkc.parent_column_id = c.column_id
LEFT JOIN sys.tables reft
    ON reft.object_id = fkc.referenced_object_id
LEFT JOIN sys.columns refc
    ON refc.object_id = fkc.referenced_object_id
    AND refc.column_id = fkc.referenced_column_id
WHERE t.schema_id = SCHEMA_ID('dbo')
  AND t.name IN (
      'Dim_ThoiGian','Dim_VanPhongDaiDien','Dim_CuaHang',
      'Dim_MatHang','Dim_KhachHang',
      'Fact_TonKho','Fact_BanHang'
  )
ORDER BY t.name, c.column_id;

-- Cap nhat mo ta thu cong cho cac cot quan trong
UPDATE dbo.metadata_technical_catalog SET col_description = N'Surrogate Key. Dinh dang YYYYMM. Vi du: 202401 = Thang 1 nam 2024'
WHERE table_name = 'Dim_ThoiGian' AND column_name = 'MaThoiGian';

UPDATE dbo.metadata_technical_catalog SET col_description = N'Phan loai KH: 1=Du lich, 2=Buu dien, 3=Ca hai (Overlap Generalization)'
WHERE table_name = 'Dim_KhachHang' AND column_name = 'LoaiKhachHang';

UPDATE dbo.metadata_technical_catalog SET col_description = N'Tong doanh thu trong thang. Additive: cong duoc theo moi chieu'
WHERE table_name = 'Fact_BanHang' AND column_name = 'DoanhThu';

UPDATE dbo.metadata_technical_catalog SET col_description = N'Ton kho cuoi thang (Snapshot). Semi-additive: KHONG cong theo chieu Thoi gian'
WHERE table_name = 'Fact_TonKho' AND column_name = 'SoLuongTonKho';

UPDATE dbo.metadata_technical_catalog SET col_description = N'Gia tri ton kho = SoLuongTonKho x Gia. Semi-additive'
WHERE table_name = 'Fact_TonKho' AND column_name = 'GiaTriTonKho';
GO

-- ============================================================
-- 2. METADATA_MAPPING
-- Luu tru anh xa tu nguon (IDB) den dich (DW)
-- Ho tro data lineage va kiem tra ETL
-- ============================================================
CREATE TABLE dbo.metadata_mapping (
    mapping_id          INT           IDENTITY(1,1) NOT NULL,
    target_table        VARCHAR(100)  NOT NULL,
    target_column       VARCHAR(100)  NOT NULL,
    source_system       VARCHAR(50)   NOT NULL,   -- IDB | IDB+DW | DERIVED
    source_table        VARCHAR(100)  NULL,
    source_column       VARCHAR(100)  NULL,
    transformation_rule NVARCHAR(500) NULL,
    transformation_type VARCHAR(50)   NULL,       -- DIRECT | EXPRESSION | LOOKUP
    load_operation      VARCHAR(20)   NOT NULL,   -- INSERT | UPSERT
    load_frequency      VARCHAR(20)   NOT NULL DEFAULT 'MONTHLY',
    natural_key         VARCHAR(100)  NULL,       -- khoa tu nhien tu IDB
    is_active           BIT           NOT NULL DEFAULT 1,
    etl_notes           NVARCHAR(500) NULL,
    created_at          DATETIME2     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_metadata_mapping PRIMARY KEY (mapping_id)
);
GO

INSERT INTO dbo.metadata_mapping
(target_table, target_column, source_system, source_table, source_column,
 transformation_rule, transformation_type, load_operation, load_frequency, natural_key, etl_notes)
VALUES
-- Dim_ThoiGian
('Dim_ThoiGian','MaThoiGian','DERIVED',NULL,NULL,
 'YEAR(ref_date)*100 + MONTH(ref_date)','EXPRESSION','INSERT','ON_DEMAND','MaThoiGian',
 N'Pre-populate toan bo tu ngay thanh lap VP som nhat. ETL chay ON_DEMAND'),
('Dim_ThoiGian','Thang','DERIVED',NULL,NULL,
 'MONTH(ref_date)','EXPRESSION','INSERT','ON_DEMAND',NULL,NULL),
('Dim_ThoiGian','Quy','DERIVED',NULL,NULL,
 'CEILING(MONTH(ref_date)/3.0) → Q1=1, Q2=2, Q3=3, Q4=4','EXPRESSION','INSERT','ON_DEMAND',NULL,NULL),
('Dim_ThoiGian','Nam','DERIVED',NULL,NULL,
 'YEAR(ref_date)','EXPRESSION','INSERT','ON_DEMAND',NULL,NULL),

-- Dim_VanPhongDaiDien
('Dim_VanPhongDaiDien','MaThanhPho','IDB','VanPhongDaiDien','MaThanhPho',
 N'Sao chep truc tiep. Natural Key de LOOKUP','DIRECT','UPSERT','MONTHLY','MaThanhPho',NULL),
('Dim_VanPhongDaiDien','TenThanhPho','IDB','VanPhongDaiDien','TenThanhPho',
 N'Sao chep truc tiep. SCD1: thay doi → ghi de','DIRECT','UPSERT','MONTHLY',NULL,NULL),
('Dim_VanPhongDaiDien','DiaChiVP','IDB','VanPhongDaiDien','DiaChiVP',
 N'Sao chep truc tiep. SCD1: thay doi → ghi de','DIRECT','UPSERT','MONTHLY',NULL,NULL),
('Dim_VanPhongDaiDien','Bang','IDB','VanPhongDaiDien','Bang',
 N'Sao chep truc tiep. SCD1: thay doi → ghi de','DIRECT','UPSERT','MONTHLY',NULL,NULL),

-- Dim_CuaHang
('Dim_CuaHang','MaCuaHang','IDB','CuaHang','MaCuaHang',
 N'Natural Key de LOOKUP. INNER JOIN Dim_VanPhongDaiDien de validate MaThanhPho','DIRECT','UPSERT','MONTHLY','MaCuaHang',NULL),
('Dim_CuaHang','MaThanhPho','IDB+DW','CuaHang','MaThanhPho',
 N'LOOKUP: kiem tra MaThanhPho ton tai trong Dim_VanPhongDaiDien','LOOKUP','UPSERT','MONTHLY',NULL,NULL),
('Dim_CuaHang','SDT','IDB','CuaHang','SoDienThoai',
 N'Doi ten cot. SCD1: doi so → UPDATE','DIRECT','UPSERT','MONTHLY',NULL,NULL),

-- Dim_MatHang
('Dim_MatHang','MaMatHang','IDB','MatHang','MaMH',
 N'Natural Key de LOOKUP','DIRECT','UPSERT','MONTHLY','MaMH',NULL),
('Dim_MatHang','MoTa','IDB','MatHang','MoTa',
 N'Sao chep truc tiep. NULL duoc phep','DIRECT','UPSERT','MONTHLY',NULL,NULL),
('Dim_MatHang','KichThuoc','IDB','MatHang','KichCo',
 N'Doi ten cot: KichCo → KichThuoc. SCD1','DIRECT','UPSERT','MONTHLY',NULL,NULL),
('Dim_MatHang','TrongLuong','IDB','MatHang','TrongLuong',
 N'Sao chep truc tiep. SCD1','DIRECT','UPSERT','MONTHLY',NULL,NULL),
('Dim_MatHang','Gia','IDB','MatHang','Gia',
 N'Gia niem yet. SCD1: thay doi → ghi de (khong luu lich su)','DIRECT','UPSERT','MONTHLY',NULL,NULL),

-- Dim_KhachHang
('Dim_KhachHang','MaKhachHang','IDB','KhachHang','MaKH',
 N'Natural Key de LOOKUP','DIRECT','UPSERT','MONTHLY','MaKH',NULL),
('Dim_KhachHang','TenKhachHang','IDB','KhachHang','TenKH',
 N'Doi ten cot. SCD1: thay doi → ghi de','DIRECT','UPSERT','MONTHLY',NULL,NULL),
('Dim_KhachHang','MaThanhPho','IDB','KhachHang','MaThanhPho',
 N'Thong tin dia ly. KHONG co FK → Dim_VanPhongDaiDien (VPDD khong quan ly KH)','DIRECT','UPSERT','MONTHLY',NULL,NULL),
('Dim_KhachHang','LoaiKhachHang','IDB','KhachHang+KhachHangDuLich+KhachHangBuuDien',NULL,
 N'CASE: dl NOT NULL AND bd NOT NULL→3, dl NOT NULL→1, bd NOT NULL→2','EXPRESSION','UPSERT','MONTHLY',NULL,
 N'Overlap Generalization: 1 KH co the thuoc ca 2 loai'),
('Dim_KhachHang','HuongDanVienDuLich','IDB','KhachHangDuLich','HuongDanVienDuLich',
 N'ISNULL(dl.HuongDanVienDuLich, ''Unknown'')','EXPRESSION','UPSERT','MONTHLY',NULL,NULL),
('Dim_KhachHang','DiaChiBuuDien','IDB','KhachHangBuuDien','DiaChiBuuDien',
 N'ISNULL(bd.DiaChiBuuDien, ''Unknown'')','EXPRESSION','UPSERT','MONTHLY',NULL,NULL),

-- Fact_TonKho
('Fact_TonKho','MaThoiGian','IDB+DW','MatHangDuocLuuTru','ThoiGianLuuTru',
 N'LOOKUP Dim_ThoiGian theo (MONTH, YEAR). Ky bao cao cua snapshot','LOOKUP','INSERT','MONTHLY',NULL,NULL),
('Fact_TonKho','MaCuaHang','IDB+DW','MatHangDuocLuuTru','MaCuaHang',
 N'FK → Dim_CuaHang. Cua hang luu tru hang ton kho','LOOKUP','INSERT','MONTHLY',NULL,NULL),
('Fact_TonKho','MaMatHang','IDB+DW','MatHangDuocLuuTru','MaMH',
 N'FK → Dim_MatHang. Mat hang ton kho','LOOKUP','INSERT','MONTHLY',NULL,NULL),
('Fact_TonKho','SoLuongTonKho','IDB','MatHangDuocLuuTru','SoLuongTrongKho',
 N'Snapshot cuoi thang (RowNum=1). Khong cong duoc theo chieu thoi gian','DIRECT','INSERT','MONTHLY',NULL,
 N'Semi-additive: dung ROW_NUMBER() PARTITION BY (CH,MH,Thang,Nam) ORDER BY ThoiGianLuuTru DESC'),
('Fact_TonKho','GiaTriTonKho','IDB+DW','MatHangDuocLuuTru+Dim_MatHang',NULL,
 N'SoLuongTrongKho x Dim_MatHang.Gia','EXPRESSION','INSERT','MONTHLY',NULL,
 N'Semi-additive. Cong duoc theo MH va CH, khong cong theo ThoiGian'),

-- Fact_BanHang
('Fact_BanHang','MaThoiGian','IDB+DW','DonDatHang','NgayDatHang',
 N'LOOKUP Dim_ThoiGian theo (MONTH(NgayDat), YEAR(NgayDat))','LOOKUP','INSERT','MONTHLY',NULL,NULL),
('Fact_BanHang','MaMatHang','IDB+DW','MatHangDuocDat','MaMH',
 N'FK → Dim_MatHang','LOOKUP','INSERT','MONTHLY',NULL,NULL),
('Fact_BanHang','MaKhachHang','IDB+DW','DonDatHang','MaKH',
 N'FK → Dim_KhachHang','LOOKUP','INSERT','MONTHLY',NULL,NULL),
('Fact_BanHang','SoLuongBan','IDB','MatHangDuocDat','SoLuongDat',
 N'SUM(SoLuongDat) GROUP BY (MaKH, MaMH, Thang, Nam). Additive','EXPRESSION','INSERT','MONTHLY',NULL,
 N'Tong hop tu muc Ngay → Thang. Additive: cong duoc theo moi chieu'),
('Fact_BanHang','DoanhThu','IDB','MatHangDuocDat','SoLuongDat x GiaDat',
 N'SUM(SoLuongDat x GiaDat). Dung GiaDat (gia thuc te) khong dung Gia niem yet','EXPRESSION','INSERT','MONTHLY',NULL,
 N'Additive. GiaDat phản anh doanh thu thuc te tai thoi diem giao dich');
GO

-- ============================================================
-- 3. METADATA_BUSINESS_GLOSSARY
-- Dinh nghia cac thuat ngu nghiep vu
-- ============================================================
CREATE TABLE dbo.metadata_business_glossary (
    glossary_id  INT           IDENTITY(1,1) NOT NULL,
    term_en      VARCHAR(100)  NOT NULL,       -- ten tieng Anh
    term_vn      NVARCHAR(100) NOT NULL,       -- ten tieng Viet
    definition   NVARCHAR(500) NOT NULL,       -- dinh nghia
    category     VARCHAR(50)   NOT NULL,       -- METRIC|DIMENSION|ETL_CONCEPT|BUSINESS_RULE|ANALYSIS_CONCEPT
    dw_table     VARCHAR(100)  NULL,
    dw_column    VARCHAR(100)  NULL,
    formula      NVARCHAR(200) NULL,
    CONSTRAINT PK_metadata_business_glossary PRIMARY KEY (glossary_id)
);
GO

INSERT INTO dbo.metadata_business_glossary
(term_en, term_vn, definition, category, dw_table, dw_column, formula)
VALUES
-- Metrics
('DoanhThu','Doanh thu',
 N'Tong gia tri tien te thu ve tu hoat dong ban hang trong ky bao cao',
 'METRIC','Fact_BanHang','DoanhThu','SUM(SoLuongDat * GiaDat) GROUP BY (MaKH, MaMH, Thang, Nam)'),
('SoLuongBan','So luong ban',
 N'Tong so don vi san pham ban ra trong ky bao cao',
 'METRIC','Fact_BanHang','SoLuongBan','SUM(SoLuongDat) GROUP BY (MaKH, MaMH, Thang, Nam)'),
('SoLuongTonKho','So luong ton kho',
 N'So luong ton cua mot mat hang tai mot cua hang vao thoi diem chot so cuoi thang',
 'METRIC','Fact_TonKho','SoLuongTonKho',N'Snapshot cuoi thang: ROW_NUMBER()=1'),
('GiaTriTonKho','Gia tri ton kho',
 N'Gia tri von dang dong lai trong kho tai thoi diem chot so cuoi thang',
 'METRIC','Fact_TonKho','GiaTriTonKho','SoLuongTonKho * Dim_MatHang.Gia'),

-- Dimensions
('LoaiKhachHang','Loai khach hang',
 N'Phan loai KH theo phuong thuc giao dich: 1=Du lich, 2=Buu dien, 3=Ca hai',
 'DIMENSION','Dim_KhachHang','LoaiKhachHang',NULL),
('Grain_BanHang','Muc do hat - Ban hang',
 N'Mot dong trong Fact_BanHang = 1 khach hang mua 1 mat hang trong 1 thang',
 'DIMENSION','Fact_BanHang',NULL,NULL),
('Grain_TonKho','Muc do hat - Ton kho',
 N'Mot dong trong Fact_TonKho = 1 mat hang tai 1 cua hang vao cuoi 1 thang',
 'DIMENSION','Fact_TonKho',NULL,NULL),
('MaThoiGian','Ma thoi gian',
 N'Surrogate Key cua chieu thoi gian. Dinh dang YYYYMM. Vi du: 202401',
 'DIMENSION','Dim_ThoiGian','MaThoiGian','YEAR*100 + MONTH'),

-- Business Rules
('SupplyPriorityRule','Quy tac uu tien cung ung',
 N'Khi xu ly don hang, he thong uu tien lay tu kho tai thanh pho khach hang sinh song',
 'BUSINESS_RULE',NULL,NULL,NULL),
('GiaDat_vs_GiaNiemYet','Gia dat va Gia niem yet',
 N'GiaDat la gia thuc te tai thoi diem giao dich, co the khac gia niem yet hien tai',
 'BUSINESS_RULE','Fact_BanHang','DoanhThu',NULL),
('OverlapGeneralization','Tong quat hoa chong lap',
 N'1 KH co the dong thoi la KH du lich va KH buu dien → LoaiKhachHang = 3',
 'BUSINESS_RULE','Dim_KhachHang','LoaiKhachHang',NULL),
('PeriodicSnapshot','Bang Fact Snapshot Dinh ky',
 N'Ghi trang thai ton kho tai thoi diem chot so cuoi moi thang, khong ghi tung giao dich',
 'ETL_CONCEPT','Fact_TonKho',NULL,NULL),
('NaturalKey','Khoa tu nhien',
 N'Khoa dinh danh co y nghia nghiep vu tu IDB, dung de tra cuu trong ETL',
 'ETL_CONCEPT',NULL,NULL,NULL),
('FactConstellation','Fact Constellation Schema',
 N'Mo hinh DW gom nhieu bang Fact dung chung cac bang Dim',
 'ETL_CONCEPT',NULL,NULL,NULL),
('SCD_Type0','SCD Type 0 - Khong thay doi',
 N'Gia tri KHONG BAO GIO thay doi sau khi nap. Ap dung cho Dim_ThoiGian',
 'ETL_CONCEPT','Dim_ThoiGian',NULL,NULL),
('SCD_Type1','SCD Type 1 - Ghi de',
 N'Thay doi → ghi de, KHONG luu su lich bien dong. Ap dung cho cac Dim con lai',
 'ETL_CONCEPT',NULL,NULL,NULL),

-- Analysis Concepts
('AdditiveFact','Fact cong don',
 N'Do do cong duoc theo TAT CA cac chieu. Vi du: DoanhThu, SoLuongBan',
 'ANALYSIS_CONCEPT','Fact_BanHang',NULL,NULL),
('SemiAdditiveFact','Fact ban cong don',
 N'Co the cong theo Mat hang va Cua hang, KHONG cong theo Thoi gian',
 'ANALYSIS_CONCEPT','Fact_TonKho',NULL,NULL),
('RollUp','Cuon len (Roll-Up)',
 N'Tong hop du lieu tu chi tiet → tong quat. Vi du: Thang → Quy → Nam',
 'ANALYSIS_CONCEPT',NULL,NULL,NULL),
('DrillDown','Khoan xuong (Drill-Down)',
 N'Di tu tong hop → chi tiet. Nguoc voi Roll-Up',
 'ANALYSIS_CONCEPT',NULL,NULL,NULL);
GO
