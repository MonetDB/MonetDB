start transaction;
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
rollback;
