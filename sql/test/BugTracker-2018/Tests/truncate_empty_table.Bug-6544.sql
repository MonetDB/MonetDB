CREATE TABLE bam_files (
	"file_id"        BIGINT        NOT NULL,
	"file_location"  CLOB          NOT NULL,
	"dbschema"       SMALLINT      NOT NULL,
	"format_version" VARCHAR(7),
	"sorting_order"  VARCHAR(10),
	"comments"       CLOB,
	CONSTRAINT "files_pkey_file_id" PRIMARY KEY ("file_id")
);

CREATE TABLE bam_sq (
	"sn"      CLOB          NOT NULL,
	"file_id" BIGINT        NOT NULL,
	"ln"      INTEGER,
	"as"      INTEGER,
	"m5"      CLOB,
	"sp"      CLOB,
	"ur"      CLOB,
	CONSTRAINT "sq_pkey_sn_file_id" PRIMARY KEY ("sn", "file_id"),
	CONSTRAINT "sq_fkey_file_id" FOREIGN KEY ("file_id") REFERENCES bam_files ("file_id")
);

CREATE TABLE bam_rg (
	"id"      CLOB          NOT NULL,
	"file_id" BIGINT        NOT NULL,
	"cn"      CLOB,
	"ds"      CLOB,
	"dt"      TIMESTAMP(6),
	"fo"      CLOB,
	"ks"      CLOB,
	"lb"      CLOB,
	"pg"      CLOB,
	"pi"      INTEGER,
	"pl"      CLOB,
	"pu"      CLOB,
	"sm"      CLOB,
	CONSTRAINT "rg_pkey_id_file_id" PRIMARY KEY ("id", "file_id"),
	CONSTRAINT "rg_fkey_file_id" FOREIGN KEY ("file_id") REFERENCES bam_files ("file_id")
);

SELECT COUNT(*) FROM bam_files;
SELECT COUNT(*) FROM bam_sq;
SELECT COUNT(*) FROM bam_rg;

TRUNCATE TABLE bam_sq;
TRUNCATE TABLE bam_rg;
TRUNCATE TABLE bam_files;

SELECT COUNT(*) FROM bam_files;
SELECT COUNT(*) FROM bam_sq;
SELECT COUNT(*) FROM bam_rg;

DROP TABLE bam_sq;
DROP TABLE bam_rg;
DROP TABLE bam_files;

