CREATE SCHEMA bam;

CREATE TABLE "bam"."files" (
    "file_id"                       SMALLINT    NOT NULL,
    "file_location"                 STRING      NOT NULL,
    "dbschema"                      SMALLINT    NOT NULL,
    "storage_mask"                  STRING      NOT NULL,
    "format_version"                VARCHAR(7),
    "sorting_order"                 VARCHAR(10),
    "comments"                      STRING,
    CONSTRAINT "files_pkey_file_id" PRIMARY KEY (file_id)
);

CREATE TABLE "bam"."sq" (
    "sn"                            STRING      NOT NULL,
    "file_id"                       SMALLINT    NOT NULL,
    "ln"                            INT         NOT NULL,
    "as"                            INT,
    "m5"                            STRING,
    "sp"                            STRING,
    "ur"                            STRING,
    CONSTRAINT "sq_pkey_sn_file_id" PRIMARY KEY (sn, file_id),
    CONSTRAINT "sq_fkey_file_id" FOREIGN KEY (file_id) REFERENCES bam.files (file_id)
);

CREATE TABLE "bam"."rg" (
    "id"                            STRING      NOT NULL,
    "file_id"                       SMALLINT    NOT NULL,
    "cn"                            STRING,
    "ds"                            STRING,
    "dt"                            TIMESTAMP,
    "fo"                            STRING,
    "ks"                            STRING,
    "lb"                            STRING,
    "pg"                            STRING,
    "pi"                            INT,
    "pl"                            STRING,
    "pu"                            STRING,
    "sm"                            STRING,
    CONSTRAINT "rg_pkey_id_file_id" PRIMARY KEY (id, file_id),
    CONSTRAINT "rg_fkey_file_id" FOREIGN KEY (file_id) REFERENCES bam.files (file_id)
);

CREATE TABLE "bam"."pg" (
    "id"                            STRING      NOT NULL,
    "file_id"                       SMALLINT    NOT NULL,
    "pn"                            STRING,
    "cl"                            STRING,
    "pp"                            STRING,
    "vn"                            STRING,
    CONSTRAINT "pg_pkey_id_file_id" PRIMARY KEY (id, file_id),
    CONSTRAINT "pg_fkey_file_id" FOREIGN KEY (file_id) REFERENCES bam.files (file_id)
);
