create table keywords (
	"action" integer,
	"default" integer,
	"schema" integer,
	"start" integer,
	"statement" integer,
	"user" integer
);
insert into keywords values (1, 2, 3, 4, 5, 6);

SELECT tbl.action FROM sys.keywords tbl;
SELECT tbl.default FROM sys.keywords tbl;
SELECT tbl.schema FROM sys.keywords tbl;
SELECT tbl.start FROM sys.keywords tbl;
SELECT tbl.statement FROM sys.keywords tbl;
SELECT tbl.user FROM sys.keywords tbl;

SELECT action FROM sys.keywords;
SELECT default FROM sys.keywords;
SELECT schema FROM sys.keywords;
SELECT start FROM sys.keywords;
SELECT statement FROM sys.keywords;

drop table keywords;
