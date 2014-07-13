START TRANSACTION;
CREATE TABLE testuuid (
        column1 UUID
);
COPY 1 RECORDS INTO testuuid FROM stdin USING DELIMITERS '\t','\n','"';
38fded43-79ef-41b0-a2af-05d20d7c4d51

select * from testuuid;

ROLLBACK;
