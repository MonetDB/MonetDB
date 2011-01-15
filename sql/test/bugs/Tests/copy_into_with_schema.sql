
CREATE TABLE copy_into_without_schema (
column1 VARCHAR(32),
column2 VARCHAR(32)
);

COPY 2 RECORDS INTO copy_into_without_schema FROM STDIN USING DELIMITERS ',', '\n';
test,test
test,test

CREATE SCHEMA abc;

CREATE TABLE abc.copy_into_with_schema (
column1 VARCHAR(32),
column2 VARCHAR(32)
);

COPY 2 RECORDS INTO abc.copy_into_with_schema FROM STDIN USING DELIMITERS ',', '\n';
test,test
test,test

INSERT into abc.copy_into_with_schema VALUES('one','two');

drop schema abc cascade;
drop table copy_into_without_schema;
