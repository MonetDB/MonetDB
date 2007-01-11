CREATE TABLE testtable (
id INTEGER,
data BLOB
);

INSERT
INTO testtable (id, data)
VALUES (0, BLOB '00');

INSERT
INTO testtable (id, data)
VALUES (1, BLOB '');

INSERT
INTO testtable (id)
VALUES (2);

INSERT
INTO testtable (id, data)
VALUES (2, NULL);

drop table testtable;
