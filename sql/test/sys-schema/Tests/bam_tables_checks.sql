-- Data integrity checks on bam schema tables

-- Primary Key checks
SELECT COUNT(*) AS duplicates, "file_id" FROM "bam"."files" GROUP BY "file_id" HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, "id", "file_id" FROM "bam"."pg" GROUP BY "id", "file_id" HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, "id", "file_id" FROM "bam"."rg" GROUP BY "id", "file_id" HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, "sn", "file_id" FROM "bam"."sq" GROUP BY "sn", "file_id" HAVING COUNT(*) > 1;

-- Alternate Key Uniqueness checks


-- Foreign Key checks
SELECT * FROM "bam"."pg" WHERE "file_id" NOT IN (SELECT "file_id" FROM "bam"."files");
SELECT * FROM "bam"."rg" WHERE "file_id" NOT IN (SELECT "file_id" FROM "bam"."files");
SELECT * FROM "bam"."sq" WHERE "file_id" NOT IN (SELECT "file_id" FROM "bam"."files");

-- NOT NULL checks
-- query used to synthesize bam specific SQLs for checking where the NOT NULL column has a NULL value
-- select 'SELECT "'||c.name||'", * FROM "'||s.name||'"."'||t.name||'" WHERE "'||c.name||'" IS NULL;' AS qry
--   from columns c join tables t on c.table_id = t.id join schemas s on t.schema_id = s.id
--  where c."null" = false and t.type not in (1, 11) and s.name = 'bam' order by s.name, t.name, c.number, c.name;
-- 20 rows:
SELECT "qname", * FROM "bam"."export" WHERE "qname" IS NULL;
SELECT "flag", * FROM "bam"."export" WHERE "flag" IS NULL;
SELECT "rname", * FROM "bam"."export" WHERE "rname" IS NULL;
SELECT "pos", * FROM "bam"."export" WHERE "pos" IS NULL;
SELECT "mapq", * FROM "bam"."export" WHERE "mapq" IS NULL;
SELECT "cigar", * FROM "bam"."export" WHERE "cigar" IS NULL;
SELECT "rnext", * FROM "bam"."export" WHERE "rnext" IS NULL;
SELECT "pnext", * FROM "bam"."export" WHERE "pnext" IS NULL;
SELECT "tlen", * FROM "bam"."export" WHERE "tlen" IS NULL;
SELECT "seq", * FROM "bam"."export" WHERE "seq" IS NULL;
SELECT "qual", * FROM "bam"."export" WHERE "qual" IS NULL;

SELECT "file_id", * FROM "bam"."files" WHERE "file_id" IS NULL;
SELECT "file_location", * FROM "bam"."files" WHERE "file_location" IS NULL;
SELECT "dbschema", * FROM "bam"."files" WHERE "dbschema" IS NULL;

SELECT "id", * FROM "bam"."pg" WHERE "id" IS NULL;
SELECT "file_id", * FROM "bam"."pg" WHERE "file_id" IS NULL;

SELECT "id", * FROM "bam"."rg" WHERE "id" IS NULL;
SELECT "file_id", * FROM "bam"."rg" WHERE "file_id" IS NULL;

SELECT "sn", * FROM "bam"."sq" WHERE "sn" IS NULL;
SELECT "file_id", * FROM "bam"."sq" WHERE "file_id" IS NULL;

