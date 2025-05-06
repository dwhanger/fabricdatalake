-- Stored procedure to generate a Location Report

CREATE PROCEDURE PrepareLocationReport
AS
BEGIN

    DROP TABLE IF EXISTS LocationReport;

    CREATE TABLE LocationReport
    AS    
        SELECT tz.Borough
            , COUNT(1) AS TotalRides
            , SUM(TotalAmount) AS PaymentAmount

        FROM yellowtaxis yt

            JOIN TaxiZones tz ON yt.PickupLocationId = tz.LocationID
        
        GROUP BY tz.Borough
        
        ORDER BY tz.Borough;

END
GO



-- Execute procedure

EXEC PrepareLocationReport
GO
















