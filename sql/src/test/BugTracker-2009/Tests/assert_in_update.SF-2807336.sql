CREATE TABLE "sys"."anbi" ( "naam" varchar(256), "vestigingsplaats"
varchar(32), "beschikking" date, "intrekking" date, kvk bigint);
CREATE TABLE "sys"."kvk" ("kvk" bigint, "bedrijfsnaam" varchar(255),
"adres" varchar(64), "postcode" varchar(6), "plaats" varchar(32), "type"
varchar(16));
select kvk.kvk from kvk,anbi where lower(naam) = lower(bedrijfsnaam)
and lower(plaats) = lower(vestigingsplaats);

update anbi set kvk = (select kvk.kvk from kvk,anbi where lower(naam)
= lower(bedrijfsnaam) and lower(plaats) = lower(vestigingsplaats));

update anbi set kvk = (select kvk from kvk,anbi where lower(naam)
= lower(bedrijfsnaam) and lower(plaats) = lower(vestigingsplaats));

drop table anbi;
drop table kvk;
