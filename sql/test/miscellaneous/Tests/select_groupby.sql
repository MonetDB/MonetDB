create schema "myschema";
create sequence "myschema"."myseq";
create table myschema.mytable
(
	"first" char(100),
	"second" double,
	"third" double,
	"fourth" clob,
	"fifth" clob,
	"sixth" double,
	"seventh" clob,
	"eighth" date,
	"ninth" double,
	"tenth" double,
	"eleventh" char(100),
	"tweelfth" char(4),
	"thirteenth" char(50),
	"fourteenth" char(50),
	"fifteenth" clob,
	"sixteenth" char(100),
	"seventeenth" char(100),
	"eighteenth" char(30),
	"nineteenth" double,
	"twentieth" char(100),
	"twentieth-first" clob,
	"twentieth-second" double,
	"twentieth-third" double,
	"twentieth-fourth" double,
	"twentieth-fifth" double,
	"twentieth-sixth" double,
	"twentieth-seventh" char(100),
	"twentieth-eighth" char(100),
	"twentieth-ninth" char(100),
	"thirtieth" char(14),
	"thirtieth-first" bigint,
	"thirtieth-second" bigint,
	"thirtieth-third" bigint,
	"thirtieth-fourth" bigint,
	"thirtieth-fifth" bigint,
	"thirtieth-sixth" bigint,
	"thirtieth-seventh" bigint,
	"thirtieth-eighth" bigint,
	"thirtieth-ninth" bigint,
	"fortieth" bigint,
	"fortieth-first" tinyint,
	"fortieth-second" blob,
	"fortieth-third" int default next value for "myschema"."myseq"
);
INSERT INTO myschema.mytable ("first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth", "eleventh", "tweelfth", "thirteenth", "fourteenth", "fifteenth", "sixteenth", 
							  "seventeenth", "eighteenth", "nineteenth", "twentieth", "twentieth-first", "twentieth-second", "twentieth-third", "twentieth-fourth", "twentieth-fifth", "twentieth-sixth", 
							  "twentieth-seventh", "twentieth-eighth", "twentieth-ninth", "thirtieth", "thirtieth-first", "thirtieth-second", "thirtieth-third", "thirtieth-fourth", "thirtieth-fifth", 
							  "thirtieth-sixth", "thirtieth-seventh", "thirtieth-eighth", "thirtieth-ninth", "fortieth", "fortieth-first", "fortieth-second", "fortieth-third") 
							  VALUES ('1234 / 0', 1234, 1, 'have', 'a', 850.85, 'very', '2019-03-18', 2, 16, 'nice', 'CC', 'DD', 'Day', 'somewhere', 'you', 'will', '3840', 
							  455, '11790 - Something', 'Paper', 850.83, 344, 0, 506.83, 59.5689, 'Good', 'Food', 'hello', '0000001234/00', 3000001215, 3000003378, 3000000346, 
							  3000003378, 3000000346, 3000000002, 3000000015, 3000000001, null, 3000000073, 0, null, 1);
SELECT
mytable."eighth" AS "first-projection",
mytable."twentieth", mytable."twentieth-seventh",
mytable."twentieth-third", mytable."twentieth-second"
FROM myschema.mytable
WHERE ("first" = '227 / 0' AND "eleventh" = 'Lekker' AND (EXTRACT(YEAR FROM mytable."eighth")*100 + EXTRACT(MONTH FROM mytable."eighth"))/100.0 = '2014.030')
GROUP BY "first-projection", "twentieth", "twentieth-seventh", "twentieth-third", "twentieth-second" LIMIT 1001;

PREPARE SELECT
mytable."eighth" AS "first-projection",
mytable."twentieth", mytable."twentieth-seventh",
mytable."twentieth-third", mytable."twentieth-second"
FROM myschema.mytable
WHERE ("first" = '227 / 0' AND "eleventh" = 'Lekker' AND (EXTRACT(YEAR FROM mytable."eighth")*100 + EXTRACT(MONTH FROM mytable."eighth"))/100.0 = '2014.030')
GROUP BY "first-projection", "twentieth", "twentieth-seventh", "twentieth-third", "twentieth-second" LIMIT 1001;
exec ** ();

PREPARE SELECT
mytable."eighth" AS "first-projection",
mytable."twentieth", mytable."twentieth-seventh",
mytable."twentieth-third", mytable."twentieth-second"
FROM myschema.mytable
WHERE ("first" = ? AND "eleventh" = ? AND (EXTRACT(YEAR FROM mytable."eighth") * cast(? as bigint) + EXTRACT(MONTH FROM mytable."eighth")) / cast(? as bigint) = cast(? as bigint))
GROUP BY "first-projection", "twentieth", "twentieth-seventh", "twentieth-third", "twentieth-second" LIMIT ?;
exec ** ('a', 'b', 923, 51, 942, 544);

TRUNCATE myschema.mytable;

SELECT
mytable."eighth" AS "first-projection",
mytable."twentieth", mytable."twentieth-seventh",
mytable."twentieth-third", mytable."twentieth-second"
FROM myschema.mytable
WHERE ("first" = '227 / 0' AND "eleventh" = 'Lekker' AND (EXTRACT(YEAR FROM mytable."eighth")*100 + EXTRACT(MONTH FROM mytable."eighth"))/100.0 = '2014.030')
GROUP BY "first-projection", "twentieth", "twentieth-seventh", "twentieth-third", "twentieth-second" LIMIT 1001;

PREPARE SELECT
mytable."eighth" AS "first-projection",
mytable."twentieth", mytable."twentieth-seventh",
mytable."twentieth-third", mytable."twentieth-second"
FROM myschema.mytable
WHERE ("first" = '227 / 0' AND "eleventh" = 'Lekker' AND (EXTRACT(YEAR FROM mytable."eighth")*100 + EXTRACT(MONTH FROM mytable."eighth"))/100.0 = '2014.030')
GROUP BY "first-projection", "twentieth", "twentieth-seventh", "twentieth-third", "twentieth-second" LIMIT 1001;
exec ** ();

PREPARE SELECT
mytable."eighth" AS "first-projection",
mytable."twentieth", mytable."twentieth-seventh",
mytable."twentieth-third", mytable."twentieth-second"
FROM myschema.mytable
WHERE ("first" = ? AND "eleventh" = ? AND (EXTRACT(YEAR FROM mytable."eighth") * cast(? as bigint) + EXTRACT(MONTH FROM mytable."eighth")) / cast(? as bigint) = cast(? as bigint))
GROUP BY "first-projection", "twentieth", "twentieth-seventh", "twentieth-third", "twentieth-second" LIMIT ?;
exec ** ('a', 'b', 923, 51, 942, 544);

create function "sys"."dummy"("col1" blob, "col2" blob, "col3" integer) returns boolean external name "unknown"."idontexist"; --error, MAL implementation of sys.dummy doesn't exist.
SELECT "sys"."dummy"("fortieth-second", blob '', '0') AS "alias1", "fortieth-third" FROM "myschema"."mytable"; --error, function doesn't exist

create table "myschema"."mytable2"
(
"'col1'" date,
"'col2'" int,
"'col3'" varchar(256),
"'col4'" clob,
"'col5'" clob,
"'col6'" decimal,
"'col7'" decimal,
"'col8'" date,
"'col9'" date,
"'col10'" int,
"'col11'" int,
"'col12'" decimal,
"'col13'" int,
"'col14'" varchar(256),
"'col15'" varchar(256),
"'col16'" int,
"'col17'" varchar(256),
"'col18'" int,
"'col19'" varchar(256),
"'col20'" bigint,
"'col21'" varchar(256),
"'col22'" varchar(256),
"'col23'" varchar(256),
"'col24'" varchar(256),
"'col25'" int,
"'col26'" varchar(256),
"'col27'" clob,
"'col28'" varchar(256),
"'col29'" decimal,
"'col30'" decimal,
"'col31'" decimal,
"'col32'" decimal,
"'col33'" clob,
"'col34'" clob,
"'col35'" int,
"'col36'" int
);

insert into "myschema"."mytable2" values (
'1998-01-03', 2239, 'col3', 'col4', 'col5', 73.28, 68.29, '2005-05-12', '2010-03-03', 563, 63, 56.3, 852, 'col14',
'col15', 134, 'col17', 892, 'col19', 9348, 'col21', 'col22', 'col23', 'col24', 934, 'col26', 'col27', 'col28', 849.2739, 1742.718,
395.824, 39.824, 'col33', 'col34', 395824, 3789);

SELECT "'col26'" FROM "myschema"."mytable2" WHERE 
( ( ("'col19'"='Some information of my hobby')) AND 
( ("'col9'"='' AND "'col3'"='ABCDE' AND (EXTRACT(YEAR FROM "'col1'")*100 + EXTRACT(MONTH FROM "'col1'"))/100.0='2011.040' 
AND "'col14'"='A description of something' AND "'col8'"='2015-08-07')) AND  ( ("'col34'"='home to')) AND  ( ("'col36'"='2013'))) GROUP BY "'col26'"
LIMIT 1001 OFFSET 0; --error, from the condition "'col9'"='', it fails to covert '' to date

PREPARE SELECT "'col26'" FROM "myschema"."mytable2" WHERE 
( ( ("'col19'"='Some information of my hobby')) AND 
( ("'col9'"='' AND "'col3'"='ABCDE' AND (EXTRACT(YEAR FROM "'col1'")*100 + EXTRACT(MONTH FROM "'col1'"))/100.0='2011.040' 
AND "'col14'"='A description of something' AND "'col8'"='2015-08-07')) AND  ( ("'col34'"='home to')) AND  ( ("'col36'"='2013'))) GROUP BY "'col26'"
LIMIT 1001 OFFSET 0;

exec ** (); --error, from the condition "'col9'"='', it fails to covert '' to date

SELECT "'col26'" FROM "myschema"."mytable2" WHERE 
( ( ("'col19'"='Some information of my hobby')) AND 
( ("'col9'"='2015-06-11' AND "'col3'"='ABCDE' AND (EXTRACT(YEAR FROM "'col1'")*100 + EXTRACT(MONTH FROM "'col1'"))/100.0='2011.040' 
AND "'col14'"='A description of something' AND "'col8'"='2015-08-07')) AND  ( ("'col34'"='home to')) AND  ( ("'col36'"='2013'))) GROUP BY "'col26'"
LIMIT 1001 OFFSET 0;

PREPARE SELECT "'col26'" FROM "myschema"."mytable2" WHERE 
( ( ("'col19'"='Some information of my hobby')) AND 
( ("'col9'"='2015-06-11' AND "'col3'"='ABCDE' AND (EXTRACT(YEAR FROM "'col1'")*100 + EXTRACT(MONTH FROM "'col1'"))/100.0='2011.040' 
AND "'col14'"='A description of something' AND "'col8'"='2015-08-07')) AND  ( ("'col34'"='home to')) AND  ( ("'col36'"='2013'))) GROUP BY "'col26'"
LIMIT 1001 OFFSET 0;

exec ** ();

prepare select 1;
exec ** (); 

prepare select cast(? as interval second);
exec ** (blob 'aaaa'); --error, cannot cast

prepare select cast(? as interval second);
exec ** (time '10:00:00' + 1); --error, no such binary operator

drop schema "myschema" cascade;
