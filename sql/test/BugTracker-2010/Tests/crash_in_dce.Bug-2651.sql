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

select naam, vestigingsplaats, beschikking, einddatum, intrekking, kvks
from anbinew, kvk where lower(naam) = lower(bedrijfsnaam) and lower(plaats) =
lower(vestigingsplaats) and kvks not in (select kvk from anbikvk2);

drop table kvk;
drop table anbikvk2;
drop table anbinew;
