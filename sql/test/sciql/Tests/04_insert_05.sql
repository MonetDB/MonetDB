CREATE ARRAY experiment(run date DIMENSION[TIMESTAMP '2010-01-01': INTERVAL '1' HOUR: TIMESTAMP '2010-01-01'], payload FLOAT DEFAULT 0.0 );
CREATE ARRAY timeseries (tick TIMESTAMP DIMENSION, payload FLOAT DEFAULT 0.0 );

INSERT INTO experiment SELECT tick, (next(payload) - payload)/ CAST(next(tick)-tick AS MINUTE) FROM timeseries;

DROP ARRAY experiment;
DROP ARRAY timeseries;

