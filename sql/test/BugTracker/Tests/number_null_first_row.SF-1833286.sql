CREATE TABLE decimal_null ( promoid NUMERIC(8, 2));

-- this will fail
INSERT into "decimal_null" values (NULL);

-- this will work
INSERT into "decimal_null" values (2.3);

-- now the one that failed will work
INSERT into "decimal_null" values (NULL);

drop table decimal_null;

CREATE TABLE decimal_null ( promoid NUMERIC(8, 2) NOT NULL);

-- this will fail
INSERT into "decimal_null" values (NULL);

-- this will work
INSERT into "decimal_null" values (2.3);

-- this will fail
INSERT into "decimal_null" values (NULL);

drop table decimal_null;
