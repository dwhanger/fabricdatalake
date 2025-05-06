-- Function to get Taxi Zones by Service Zone

CREATE FUNCTION dbo.ZonesByType( @service_zone VARCHAR(50) )

RETURNS TABLE

AS
RETURN
(
    SELECT *
    FROM dbo.TaxiZones
    WHERE service_zone = @service_zone
)

GO




SELECT *
FROM ZonesByType('Yellow Zone')

GO



















