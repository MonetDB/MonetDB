CREATE SEQUENCE "sys"."seq_5958" AS INTEGER;
CREATE TABLE "sys"."anbi_intern" (
	"id"         INTEGER       NOT NULL DEFAULT next value for "sys"."seq_5958",
	"naam"       VARCHAR(1024),
	"plaats"     VARCHAR(54),
	"begindatum" DATE,
	"einddatum"  DATE,
	"intrekking" DATE,
	"activiteit" VARCHAR(16),
	CONSTRAINT "anbi_intern_id_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "sys"."anbi_kvk" (
	"kvks" INTEGER,
	"anbi" INTEGER
);
CREATE TABLE "sys"."kvk" (
	"kvk"          BIGINT,
	"bedrijfsnaam" VARCHAR(512),
	"kvks"         INTEGER,
	"sub"          INTEGER,
	"adres"        VARCHAR(64),
	"postcode"     VARCHAR(8),
	"plaats"       VARCHAR(32),
	"type"         VARCHAR(14),
	"status"       VARCHAR(256),
	"website"      VARCHAR(128),
	"vestiging"    BIGINT,
	"rechtsvorm"   VARCHAR(48),
	"lat_rad"      DECIMAL(9,9),
	"lon_rad"      DECIMAL(9,9),
	"anbi"         DATE
);

update kvk set anbi = (select begindatum from sys.anbi_kvk, anbi_intern where
anbi_kvk.anbi = anbi_intern.id and kvk.kvks = anbi_kvk.kvks and
anbi_intern.einddatum is null) where kvk.kvks in (select kvks from anbi_kvk);

drop table sys.kvk;
drop table sys.anbi_kvk;
drop table sys.anbi_intern;
