START TRANSACTION;
CREATE LOADER pyloader07() LANGUAGE PYTHON {
    _emit.emit({'s': 33, 't': 42});
 
};
CREATE TABLE pyloader07table FROM LOADER pyloader07();

SELECT * FROM pyloader07table;
DROP TABLE pyloader07table;
DROP LOADER pyloader07;
ROLLBACK;

START TRANSACTION;

CREATE TABLE tstamp(d DATE, s TIME, t TIMESTAMP);


CREATE LOADER pyloader07() LANGUAGE PYTHON {
    _emit.emit({'d': '2014-05-20', 's': '00:02:30', 't': '2014-05-20 00:02:30'});
    _emit.emit({'d': ['2014-05-20'], 's': ['00:02:30'], 't': ['2014-05-20 00:02:30']});
};

COPY LOADER INTO tstamp FROM pyloader07();

SELECT * FROM tstamp;
DROP TABLE tstamp;
DROP LOADER pyloader07;

ROLLBACK;
