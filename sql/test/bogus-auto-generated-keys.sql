-- generate a simple table with an auto-generated key (id)
CREATE TABLE gen_keys (
	"id" serial,
	"x" varchar(12)
);

-- perform an update, useless, but illustrates the bug, this time no
-- generated key is reported, which is correct
UPDATE gen_keys SET "x" = 'bla' WHERE "id" = 12;

-- insert some value, should get a generated key
INSERT INTO gen_keys ("x") VALUES ('boe');

-- update again, we expect NO generated key, but we DO get one
UPDATE gen_keys SET "x" = 'bla' WHERE "id" = 1;
UPDATE gen_keys SET "x" = 'bla' WHERE "id" = 12;

-- ok, cleanup a bit
DROP TABLE gen_keys;
