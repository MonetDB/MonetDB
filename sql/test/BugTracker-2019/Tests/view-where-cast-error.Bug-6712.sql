start transaction;
CREATE TABLE x ("id" INTEGER, "attribute" CHARACTER LARGE OBJECT, "value" CHARACTER LARGE OBJECT);
INSERT INTO x VALUES (1, 'version', '3.15.0');
INSERT INTO x VALUES (1, 'executiontime', '100848');
INSERT INTO x VALUES (2, 'version', '3.15.0');
INSERT INTO x VALUES (2, 'executiontime', '54340');
INSERT INTO x VALUES (3, 'version', '3.15.0');
INSERT INTO x VALUES (3, 'executiontime', '96715');
create view executiontimes as select * from x where attribute = 'executiontime';

select id from executiontimes where cast(value as bigint) > 80000;
rollback;
