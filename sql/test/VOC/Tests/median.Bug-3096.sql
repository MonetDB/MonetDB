
CREATE TABLE "voyages" (
	"number"            integer	NOT NULL,
	"number_sup"        char(1)	NOT NULL,
	"trip"              integer,
	"trip_sup"          char(1),
	"boatname"          varchar(50),
	"master"            varchar(50),
	"tonnage"           integer,
	"type_of_boat"      varchar(30),
	"built"             varchar(15),
	"bought"            varchar(15),
	"hired"             varchar(15),
	"yard"              char(1),
	"chamber"           char(1),
	"departure_date"    date,
	"departure_harbour" varchar(30),
	"cape_arrival"      date,
	"cape_departure"    date,
	"cape_call"         boolean,
	"arrival_date"      date,
	"arrival_harbour"   varchar(30),
	"next_voyage"       integer,
	"particulars"       varchar(530)
);

select median(departure_date) - min(departure_date) from voyages;
select max(departure_date) - min(departure_date) from voyages;

drop table voyages;
