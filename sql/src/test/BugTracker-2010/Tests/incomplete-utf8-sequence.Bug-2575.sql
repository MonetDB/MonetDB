CREATE TABLE tbl_bug2575 (
        "documentid" BIGINT        NOT NULL,
        "seq"        SMALLINT      NOT NULL,
        "trigram"    CHAR(3)       NOT NULL
);

copy 2 records into tbl_bug2575 from stdin using delimiters '\t','\n','';
10001160000	29	.v.
10001690001	0	co√

DROP TABLE tbl_bug2575;
