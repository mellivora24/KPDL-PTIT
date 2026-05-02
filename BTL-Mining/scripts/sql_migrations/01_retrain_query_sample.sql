USE DATA_MINING;
GO

-- Retrain model sẽ sử dụng tất cả dữ liệu có actual_outcome
SELECT 
    a.status, a.duration, ...,
    o.actual_outcome AS target
FROM applications a
JOIN outcomes o ON a.id = o.application_id
WHERE o.actual_outcome IS NOT NULL

-- Training lần đầu tiên sẽ chỉ sử dụng dữ liệu từ UCI
SELECT ... , target
FROM applications
WHERE data_source = 'UCI'