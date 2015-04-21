create table keywords (
	"action" integer,
	"as" integer,
	"authorization" integer,
	"column" integer,
	"cycle" integer,
	"distinct" integer,
	"increment" integer,
	"maxvalue" integer,
	"minvalue" integer,
	"plan" integer,
	"schema" integer,
	"start" integer,
	"statement" integer,
	"table" integer
);
insert into keywords values (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

select distinct action from sys.keywords;
select distinct as from sys.keywords;
select distinct authorization from sys.keywords;
select distinct column from sys.keywords;
select distinct cycle from sys.keywords;
select distinct distinct from sys.keywords;
select distinct increment from sys.keywords;
select distinct maxvalue from sys.keywords;
select distinct minvalue from sys.keywords;
select distinct plan from sys.keywords;
select distinct schema from sys.keywords;
select distinct start from sys.keywords;
select distinct statement from sys.keywords;
select distinct table from sys.keywords;

drop table keywords;



