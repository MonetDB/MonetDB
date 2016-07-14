CREATE FUNCTION geographic_to_cartesian(lat FLOAT, lon FLOAT) RETURNS POINT /* convert geographic coordinates to Cartesian while creating a point */
BEGIN
    DECLARE deg_to_rad FLOAT, lat_rad FLOAT, lon_rad FLOAT, aux1 FLOAT, aux2 FLOAT;
    SET deg_to_rad = pi() / 180;
    SET lat_rad = lat * deg_to_rad;
    SET lon_rad = lon * deg_to_rad;
    SET aux1 = sys.cos(lat_rad);
    SET aux2 = 6371 * aux1;
    RETURN sys.st_makepoint(aux2 * sys.cos(lon_rad), aux2 * sys.sin(lon_rad), 6371 * sys.sin(lat_rad));
END;
CREATE TABLE test_table (lat FLOAT, lon FLOAT);
SELECT geographic_to_cartesian(lat, lon) AS calc_point FROM test_table;

DROP TABLE test_table;
DROP FUNCTION geographic_to_cartesian;
