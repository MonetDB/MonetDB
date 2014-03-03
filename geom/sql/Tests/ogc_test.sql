-- Conformance Item N1
SELECT f_table_name
FROM geometry_columns;
-- Conformance Item N2
SELECT g_table_name
FROM geometry_columns;
-- Conformance Item N3
SELECT storage_type
FROM geometry_columns
WHERE f_table_name = 'streams';
-- Conformance Item N4
SELECT geometry_type
FROM geometry_columns
WHERE f_table_name = 'streams';
-- Conformance Item N5
SELECT coord_dimension
FROM geometry_columns
WHERE f_table_name = 'streams';
-- Conformance Item N6
SELECT max_ppr
FROM geometry_columns
WHERE f_table_name = 'streams';
-- Conformance Item N7
SELECT srid
FROM geometry_columns
WHERE f_table_name = 'streams';
-- Conformance Item N8
SELECT srtext
FROM SPATIAL_REF_SYS
WHERE SRID = 101;
