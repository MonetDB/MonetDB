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

CREATE TABLE "sys"."anbi" (
        "naam"             varchar(128),
        "vestigingsplaats" varchar(64),
        "beschikking"      date,
        "einddatum"        date,
        "intrekking"       date
);

select bedrijfsnaam, plaats, kvks from kvk,anbi where bedrijfsnaam in (anbi.naam);

drop table "sys"."anbi";
drop table "sys"."kvk";
