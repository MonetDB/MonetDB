CREATE FUNCTION "sys"."test1"() RETURNS TABLE ("col1" int, "col2" int) BEGIN RETURN SELECT 1, 2; END;

CREATE VIEW "sys"."test1" ("col1", "col2") AS SELECT "col1", "col2" FROM "sys"."test1"();

select count(*) from dependencies inner join tables on dependencies.id = tables.id where tables.name = 'test1';
select count(*) from dependencies inner join functions on dependencies.id = functions.id where functions.name = 'test1';

DROP FUNCTION "test1"(); --error, view test1 depends on function test1;

CREATE FUNCTION "sys"."test2"() RETURNS TABLE ("col1" int, "col2" int) BEGIN RETURN SELECT "col1", "col2" FROM "sys"."test1"; END;

DROP VIEW "test1"; --error, function "test2" depends on view test1;

select count(*) from dependencies inner join tables on dependencies.id = tables.id where tables.name = 'test1';
select count(*) from dependencies inner join functions on dependencies.id = functions.id where functions.name = 'test1';

DROP FUNCTION "test2"();
DROP VIEW "test1";
DROP FUNCTION "test1"();

select count(*) from dependencies inner join tables on dependencies.id = tables.id where tables.name = 'test1';
select count(*) from dependencies inner join functions on dependencies.id = functions.id where functions.name = 'test1';
