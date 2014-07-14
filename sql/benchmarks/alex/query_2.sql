-- An OLAP-style ROLL-UP summing up 7 consecutive layers to produce
-- a single 2D array (from the 3D tomograph) 
-- tomo : 256 * 256 * 154 cells


SELECT x, z, SUM(val) FROM tomo 
WHERE y BETWEEN 47 AND 53 
GROUP BY x, z;

