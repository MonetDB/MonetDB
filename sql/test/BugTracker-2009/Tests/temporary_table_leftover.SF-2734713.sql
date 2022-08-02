START TRANSACTION;
CREATE TEMPORARY TABLE persistent_temopary (id integer);
blabla;
COMMIT;

START TRANSACTION;
CREATE TEMPORARY TABLE persistent_temopary (id integer);
blabla;
COMMIT;

drop table persistent_temopary;
