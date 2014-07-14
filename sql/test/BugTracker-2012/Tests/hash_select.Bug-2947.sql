START TRANSACTION;
SET SCHEMA "sys";
CREATE TABLE "sys"."c__has_a" (
        "owner_table"    VARCHAR(32672),
        "owner_id"       BIGINT,
        "relation_name"  VARCHAR(32672),
        "property_table" VARCHAR(32672),
        "property_id"    BIGINT,
        "property_class" VARCHAR(32672)
);
CREATE INDEX "c__has_a_property_index" ON "sys"."c__has_a" ("property_table", "property_id");
COPY 1 RECORDS INTO "sys"."c__has_a" FROM stdin USING DELIMITERS '\t','\n','"';
NULL	NULL	NULL	"SIMPLE"	4	"simpleclass"
COMMIT;

select * from c__has_a where owner_table is null and property_table='SIMPLE' and property_id = 4;

DROP table c__has_a;



CREATE TABLE FOO (
       "a" VARCHAR(8),
       "b" VARCHAR(8),
       "c" BIGINT);
CREATE INDEX FOO_INDEX ON FOO ("b","c");

insert into foo (b,c)values('foo',3);

select * from foo where a is null and b='foo' and c=3;

DROP TABLE FOO;
