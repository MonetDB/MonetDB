
CREATE TABLE "sys"."kvk" (
	"kvk" bigint,
	"bedrijfsnaam" varchar(255),
	"adres" varchar(64),
	"postcode" varchar(6),
	"plaats" varchar(32),
	"type" varchar(16),
	"kvks" int,
	"sub" int
);

CREATE TABLE "sys"."concernrelaties" (
	"kvk" bigint,
	"ouder" varchar(30),
	"postcode" varchar(6),
	"plaats" varchar(32),
	"ouderkvk" int
);

select * from kvk,concernrelaties where upper(bedrijfsnaam) like
'VANAD%' and (kvk.kvk=concernrelaties.kvk or kvk.kvk=ouderkvk) limit 10;

drop table kvk;
drop table concernrelaties;
