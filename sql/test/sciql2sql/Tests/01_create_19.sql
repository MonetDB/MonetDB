-- create a fixed time series array
--! CREATE ARRAY experiment(run date DIMENSION[TIMESTAMP '2010-01-01': INTERVAL '1' HOUR: TIMESTAMP '2010-01-01'], payload FLOAT DEFAULT 0.0 );
--! SELECT * FROM experiment;
--! DROP ARRAY experiment;

CREATE TABLE experiment(run date CHECK( run >= TIMESTAMP '2010-01-01' AND run < TIMESTAMP '2010-02-01'), payload FLOAT DEFAULT 0.0 );
SELECT * FROM experiment;
DROP TABLE experiment;
