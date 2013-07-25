-- create an unbounded time series array
CREATE ARRAY timeseries (tick TIMESTAMP DIMENSION, payload FLOAT DEFAULT 0.0 );

SELECT * FROM timeseries;

DROP ARRAY timeseries;

