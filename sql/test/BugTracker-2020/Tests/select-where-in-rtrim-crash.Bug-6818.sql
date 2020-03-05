CREATE TABLE "dd_field" (
        "TABEL"    VARCHAR(25)   NOT NULL,
        "VELD"     VARCHAR(25)   NOT NULL,
        "TYPE"     VARCHAR(4)    NOT NULL
);

select "TABEL", "VELD"
 from dd_field f
 where ("TABEL", "VELD") in (select "TABEL", "VELD" from dd_field);
-- no problemo

select "TABEL", "VELD"
 from dd_field f
 where (rtrim("TABEL")) in (select "TABEL" from dd_field);
-- no problemo

select "TABEL", "VELD"
 from dd_field f
 where (rtrim("TABEL"), rtrim("VELD")) in (select "TABEL", "VELD" from dd_field);
-- sql/backends/monet5/rel_bin.c:920: exp_bin: Assertion `0' failed.

DROP  TABLE "dd_field";

