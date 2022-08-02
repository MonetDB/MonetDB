create table kvk (kvk bigint, bedrijfsnaam varchar(255), adres varchar(64), postcode varchar(6), plaats varchar(32), type varchar(16));
create table anbi (naam varchar(256), vestigingsplaats varchar(32), beschikking timestamp, intrekking timestamp);

select naam, vestigingsplaats, beschikking from anbi except select naam, vestigingsplaats, beschikking from anbi, kvk where lower(naam) = lower(bedrijfsnaam) and lower(plaats) = lower(vestigingsplaats);
select naam, vestigingsplaats from anbi except select naam, vestigingsplaats, beschikking from anbi, kvk where lower(naam) = lower(bedrijfsnaam) and lower(plaats) = lower(vestigingsplaats);

drop table kvk;
drop table anbi;
