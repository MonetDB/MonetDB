-- http://bugs.monetdb.org/show_bug.cgi?id=2577

CREATE TABLE "sys"."kvk" (
        "id"                serial,
        "kvk"               bigint,
        "bedrijfsnaam"      varchar(256),
        "adres"             varchar(64),
        "postcode"          char(6),
        "plaats"            varchar(32),
        "type"              varchar(14),
        "kvks"              int,
        "sub"               int,
        "bedrijfsnaam_size" smallint,
        "adres_size"        smallint
);

CREATE TABLE "sys"."kvk_nieuw" (
        "kvk"          bigint,
        "bedrijfsnaam" varchar(256),
        "kvks"         int,
        "sub"          int,
        "adres"        varchar(64),
        "postcode"     varchar(8),
        "plaats"       varchar(32),
        "type"         varchar(14)
);

CREATE TABLE "sys"."kvk_extra" (
        "kvk"          bigint,
        "bedrijfsnaam" varchar(256),
        "straat"       varchar(256),
        "nummer"       int,
        "toevoeging"   varchar(16),
        "postcode"     char(6),
        "plaats"       varchar(32),
        "website"      varchar(128),
        "type"         varchar(14),
        "status"       varchar(64),
        "kvks"         int,
        "sub"          int
);

CREATE TABLE "sys"."kvk_extra_nieuw" (
        "kvk"          bigint,
        "bedrijfsnaam" varchar(256),
        "straat"       varchar(256),
        "nummer"       int,
        "toevoeging"   varchar(16),
        "postcode"     varchar(10),
        "plaats"       varchar(32),
        "website"      varchar(128),
        "type"         varchar(14),
        "status"       varchar(64),
        "kvks"         int,
        "sub"          int
);

INSERT INTO "kvk" ("kvk", "bedrijfsnaam", "adres", "postcode", "plaats", "type", "kvks", "sub", "bedrijfsnaam_size", "adres_size")
	VALUES (1, 'MonetDB', 'Lelielaan 6', '1234AB', 'Brugstede', 'db', 1, 1, 12, 14);

INSERT INTO "kvk_nieuw"
	VALUES (2, 'PostgreSQL', 1, 1, 'Olifantenlei 12', '2345BC', 'Trompethuis', 'db');

INSERT INTO "kvk_extra"
	VALUES (3, 'MySQL', 'Dolfijnstraat', 24, NULL, '3456CD', 'Dolfinarium', 'www.mysql.com', 'db', 'alive', 1, 1);

INSERT INTO "kvk_extra_nieuw"
	VALUES (4, 'CouchDB', 'Zetelkade', 48, 'b', '4567DE', 'Zandbank', 'www.couchdb.org', 'db', 'relaxed', 1, 1);

-- reporter says this returns NULL values
select kvk from kvk where kvk not in (
	select kvk from kvk_nieuw
	union
	select kvk from kvk_extra
	union
	select kvk from kvk_extra_nieuw
);

-- and this should do what he wants
select kvk from kvk except (
	select kvk from kvk_nieuw
	union
	select kvk from	kvk_extra
	union
	select kvk from kvk_extra_nieuw
);


DROP TABLE kvk;
DROP TABLE kvk_nieuw;
DROP TABLE kvk_extra;
DROP TABLE kvk_extra_nieuw;

