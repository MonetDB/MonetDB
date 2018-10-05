-- Data integrity checks on geom specific tables (see 40_geom.sql)

-- Primary Key checks
SELECT COUNT(*) AS duplicates, "srid" FROM "sys"."spatial_ref_sys" GROUP BY "srid" HAVING COUNT(*) > 1;

-- Alternate Key Uniqueness checks
SELECT COUNT(*) AS duplicates, "auth_name", "auth_srid", "srtext", "proj4text" FROM "sys"."spatial_ref_sys" GROUP BY "auth_name", "auth_srid", "srtext", "proj4text" HAVING COUNT(*) > 1;

-- Foreign Key checks
SELECT "auth_srid", * FROM "sys"."spatial_ref_sys" WHERE "auth_srid" NOT IN (SELECT "srid" FROM "sys"."spatial_ref_sys");

-- NOT NULL checks
SELECT "srid", * FROM "sys"."spatial_ref_sys" WHERE "srid" IS NULL;
SELECT "auth_name", * FROM "sys"."spatial_ref_sys" WHERE "auth_name" IS NULL;
SELECT "auth_srid", * FROM "sys"."spatial_ref_sys" WHERE "auth_srid" IS NULL;
SELECT "srtext", * FROM "sys"."spatial_ref_sys" WHERE "srtext" IS NULL;
SELECT "proj4text", * FROM "sys"."spatial_ref_sys" WHERE "proj4text" IS NULL;

