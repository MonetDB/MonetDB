-- implementation limitation on OR semantics
CREATE TABLE "sys"."kvk" (
"kvk" bigint,
"bedrijfsnaam" varchar(255),
"adres" varchar(64),
"postcode" varchar(6),
"plaats" varchar(32),
"type" varchar(16)
);

CREATE TABLE "sys"."anbi" (
"naam" varchar(256),
"vestigingsplaats" varchar(32),
"beschikking" date,
"intrekking" date
);

select * from kvk,anbi where lower(naam) = lower(bedrijfsnaam) and
(vestigingsplaats = 'DEN HAAG' or vestigingsplaats LIKE '%GRAVE%');


drop table kvk;
drop table anbi;
