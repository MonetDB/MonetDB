CREATE TABLE bugexample ( id INTEGER);

START TRANSACTION;
ALTER TABLE bugexample ADD COLUMN newcolumn integer;
ALTER TABLE bugexample ALTER COLUMN newcolumn SET NOT NULL;
COMMIT;  

drop table bugexample;

