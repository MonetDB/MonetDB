CREATE TABLE "sys"."anbinew" (
        "naam"             VARCHAR(128),
        "vestigingsplaats" VARCHAR(64),
        "beschikking"      DATE,
        "einddatum"        DATE,
        "intrekking"       DATE
);
CREATE TABLE "sys"."anbikvk2" (
        "naam"             VARCHAR(128),
        "vestigingsplaats" VARCHAR(32),
        "beschikking"      DATE,
        "einddatum"        DATE,
        "intrekking"       DATE,
        "kvks"             INTEGER
);
CREATE TABLE "sys"."kvk" (
        "id"                INTEGER       NOT NULL,
        "kvk"               BIGINT,
        "bedrijfsnaam"      VARCHAR(256),
        "adres"             VARCHAR(256),
        "postcode"          VARCHAR(10),
        "plaats"            VARCHAR(32),
        "type"              VARCHAR(14),
        "kvks"              INTEGER,
        "sub"               INTEGER,
        "bedrijfsnaam_size" SMALLINT,
        "adres_size"        SMALLINT,
        CONSTRAINT "kvk_id_pkey" PRIMARY KEY ("id")
);

select * from (select naam, vestigingsplaats, beschikking, einddatum,
intrekking from anbinew except select naam, vestigingsplaats, beschikking,
einddatum, intrekking from anbikvk2) as x, kvk where naam = UPPER(bedrijfsnaam)
and kvks not in (select kvk from anbikvk2) limit 10;

drop table kvk;
drop table anbikvk2;
drop table anbinew;
