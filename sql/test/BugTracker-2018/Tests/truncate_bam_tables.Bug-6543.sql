SELECT (COUNT(*) > 0) AS has_rows FROM bam.sq;
TRUNCATE TABLE bam.sq;
SELECT (COUNT(*) > 0) AS has_rows FROM bam.sq;

SELECT (COUNT(*) > 0) AS has_rows FROM bam.rg;
TRUNCATE TABLE bam.rg;
SELECT (COUNT(*) > 0) AS has_rows FROM bam.rg;

SELECT (COUNT(*) > 0) AS has_rows FROM bam.pg;
TRUNCATE TABLE bam.pg;
SELECT (COUNT(*) > 0) AS has_rows FROM bam.pg;

SELECT (COUNT(*) > 0) AS has_rows FROM bam.export;
TRUNCATE TABLE bam.export;
SELECT (COUNT(*) > 0) AS has_rows FROM bam.export;

SELECT (COUNT(*) > 0) AS has_rows FROM bam.files;
TRUNCATE TABLE bam.files;
SELECT (COUNT(*) > 0) AS has_rows FROM bam.files;

