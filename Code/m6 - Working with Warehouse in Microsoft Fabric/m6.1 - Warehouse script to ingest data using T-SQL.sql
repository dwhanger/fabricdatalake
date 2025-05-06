-- Create table TaxiZones using CTAS command

CREATE TABLE TaxiZones
AS
SELECT *
FROM SilverLakehouse.dbo.taxizones
WHERE service_zone <> 'N/A'
GO

-- Query TaxiZones table

SELECT *
FROM TaxiZones
GO

-- Create Drivers table

CREATE TABLE Drivers
(
    DriverLicenseNumber     INT,
    Name                    VARCHAR(100),
    Type                    VARCHAR(50),
    ExpirationDate          DATE,
    LastDateUpdated         DATE
)
GO

-- Load data to Drivers table using COPY INTO command

COPY INTO Drivers
(DriverLicenseNumber, Name, Type, ExpirationDate, LastDateUpdated)

FROM 'https://<data lake>.dfs.core.windows.net/<container>/<path>'

WITH 
(
    FILE_TYPE = 'CSV',
    
    CREDENTIAL = (IDENTITY= 'Storage Account Key', SECRET='<access key>'),
    
    FIRSTROW = 2
)
GO

-- Query Drivers table

SELECT *
FROM Drivers
GO














