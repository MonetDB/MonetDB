create table sql_keywords (
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
insert into sql_keywords values (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

select distinct action from sys.sql_keywords;
select distinct as from sys.sql_keywords;
select distinct authorization from sys.sql_keywords;
select distinct column from sys.sql_keywords;
select distinct cycle from sys.sql_keywords;
select distinct distinct from sys.sql_keywords;
select distinct increment from sys.sql_keywords;
select distinct maxvalue from sys.sql_keywords;
select distinct minvalue from sys.sql_keywords;
select distinct plan from sys.sql_keywords;
select distinct schema from sys.sql_keywords;
select distinct start from sys.sql_keywords;
select distinct statement from sys.sql_keywords;
select distinct table from sys.sql_keywords;

drop table sql_keywords;



