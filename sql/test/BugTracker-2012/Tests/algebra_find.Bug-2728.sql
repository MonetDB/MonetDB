CREATE TABLE "sys"."kvk" (
        "kvk"          BIGINT,
        "bedrijfsnaam" VARCHAR(256),
        "kvks"         INTEGER,
        "sub"          INTEGER,
        "adres"        VARCHAR(256),
        "postcode"     VARCHAR(10),
        "plaats"       VARCHAR(32),
        "type"         VARCHAR(14),
        "website"      VARCHAR(128)
);
CREATE TABLE "sys"."vve" (
        "kd"            INTEGER,
        "naam1"         VARCHAR(255),
        "naam2"         VARCHAR(255),
        "naam3"         VARCHAR(255),
        "straatnaam"    VARCHAR(255),
        "huisnummer"    INTEGER,
        "toevoeging"    VARCHAR(16),
        "postcode"      CHAR(6),
        "plaats"        VARCHAR(32),
        "appartementen" INTEGER,
        "zeter"         VARCHAR(32)
);

insert into vve values (1, 'test', 'test2', 'test3', 'Oude Trambaan', 7, null, '2265CA', 'Leidschendam', 1, 'ergens');
insert into kvk values (0, 'test', 0, 0, 'Oude Trambaan 7', '2265CA', 'Leidschendam', 'iets', 'geen');
insert into kvk values (1, 'test', 1, 0, 'Oude Trambaan 8', '2265CA', 'Leidschendam', 'iets', 'geen');

select count(*) from vve, kvk where toevoeging is null and vve.huisnummer is not null and vve.postcode = kvk.postcode and kvk.adres like ' %'||vve.huisnummer;

drop table kvk;
drop table vve;
