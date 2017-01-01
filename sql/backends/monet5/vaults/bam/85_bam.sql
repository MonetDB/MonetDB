-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

CREATE SCHEMA bam;

CREATE PROCEDURE bam.bam_loader_repos(bam_repos STRING, dbschema SMALLINT)
EXTERNAL NAME bam.bam_loader_repos;

CREATE PROCEDURE bam.bam_loader_files(bam_files STRING, dbschema SMALLINT)
EXTERNAL NAME bam.bam_loader_files;

CREATE PROCEDURE bam.bam_loader_file(bam_file STRING, dbschema SMALLINT)
EXTERNAL NAME bam.bam_loader_file;

CREATE PROCEDURE bam.bam_drop_file(file_id BIGINT, dbschema SMALLINT)
EXTERNAL NAME bam.bam_drop_file;


CREATE FUNCTION bam.bam_flag(flag SMALLINT, name STRING)
RETURNS BOOLEAN EXTERNAL NAME bam.bam_flag;

CREATE FUNCTION bam.reverse_seq(seq STRING)
RETURNS STRING EXTERNAL NAME bam.reverse_seq;

CREATE FUNCTION bam.reverse_qual(qual STRING)
RETURNS STRING EXTERNAL NAME bam.reverse_qual;

CREATE FUNCTION bam.seq_length(cigar STRING)
RETURNS INT EXTERNAL NAME bam.seq_length;

CREATE FUNCTION bam.seq_char(ref_pos INT, alg_seq STRING, alg_pos INT, alg_cigar STRING)
RETURNS CHAR(1) EXTERNAL NAME bam.seq_char;


CREATE PROCEDURE bam.sam_export(output_path STRING)
EXTERNAL NAME bam.sam_export;

CREATE PROCEDURE bam.bam_export(output_path STRING)
EXTERNAL NAME bam.bam_export;



CREATE TABLE bam.files (
    file_id                     BIGINT          NOT NULL,
    file_location               STRING          NOT NULL,
    dbschema                    SMALLINT        NOT NULL,
    format_version              VARCHAR(7),
    sorting_order               VARCHAR(10),
    comments                    STRING,
    CONSTRAINT files_pkey_file_id PRIMARY KEY (file_id)
);

CREATE TABLE bam.sq (
    sn                          STRING          NOT NULL,
    file_id                     BIGINT          NOT NULL,
    ln                          INT,
    "as"                        INT,
    m5                          STRING,
    sp                          STRING,
    ur                          STRING,
    CONSTRAINT sq_pkey_sn_file_id PRIMARY KEY (sn, file_id),
    CONSTRAINT sq_fkey_file_id FOREIGN KEY (file_id) REFERENCES bam.files (file_id)
);

CREATE TABLE bam.rg (
    id                          STRING          NOT NULL,
    file_id                     BIGINT          NOT NULL,
    cn                          STRING,
    ds                          STRING,
    dt                          TIMESTAMP,
    fo                          STRING,
    ks                          STRING,
    lb                          STRING,
    pg                          STRING,
    pi                          INT,
    pl                          STRING,
    pu                          STRING,
    sm                          STRING,
    CONSTRAINT rg_pkey_id_file_id PRIMARY KEY (id, file_id),
    CONSTRAINT rg_fkey_file_id FOREIGN KEY (file_id) REFERENCES bam.files (file_id)
);

CREATE TABLE bam.pg (
    id                          STRING          NOT NULL,
    file_id                     BIGINT          NOT NULL,
    pn                          STRING,
    cl                          STRING,
    pp                          STRING,
    vn                          STRING,
    CONSTRAINT pg_pkey_id_file_id PRIMARY KEY (id, file_id),
    CONSTRAINT pg_fkey_file_id FOREIGN KEY (file_id) REFERENCES bam.files (file_id)
);

CREATE TABLE bam.export (
    qname                       STRING          NOT NULL,
    flag                        SMALLINT        NOT NULL,
    rname                       STRING          NOT NULL,
    pos                         INT             NOT NULL,
    mapq                        SMALLINT        NOT NULL,
    cigar                       STRING          NOT NULL,
    rnext                       STRING          NOT NULL,
    pnext                       INT             NOT NULL,
    tlen                        INT             NOT NULL,
    seq                         STRING          NOT NULL,
    qual                        STRING          NOT NULL
);

update sys._tables
    set system = true
    where name in ('export', 'files', 'pg', 'rg', 'sq')
        and schema_id = (select id from sys.schemas where name = 'bam');
