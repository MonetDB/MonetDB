prepare CREATE TABLE IF NOT EXISTS mydata ( sensor VARCHAR(64) NOT NULL, dt TIMESTAMP NOT NULL, val INTEGER NOT NULL, exponent INTEGER NOT NULL, CONSTRAINT data_pk PRIMARY KEY(sensor, dt));
exec **();

prepare CREATE VIEW myview AS SELECT m.sensor, m.dt, m.val * power(10, m.exponent) FROM mydata m;
exec **();

SELECT * FROM mydata;
SELECT * FROM myview;

DROP VIEW myview;
DROP TABLE mydata;
