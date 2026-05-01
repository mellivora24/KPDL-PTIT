CREATE LOGIN [NT Service\MSSQLServerOLAPService] FROM WINDOWS;

USE <Tên DB>;
CREATE USER [NT Service\MSSQLServerOLAPService] FOR LOGIN [NT Service\MSSQLServerOLAPService];

ALTER ROLE db_datareader ADD MEMBER [NT Service\MSSQLServerOLAPService];