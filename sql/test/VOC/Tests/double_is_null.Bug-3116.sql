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

SELECT hired, count(*)  FROM voyages WHERE   true  AND hired IS NOT NULL AND
hired is not null GROUP BY hired ORDER BY hired;

drop table voyages;

