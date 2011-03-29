CREATE ARRAY experiment( 
	run date DIMENSION[TIMESTAMP '2010-01-01':*: INTERVAL '1' MINUTE], 
	payload FLOAT DEFAULT 0.0 );

CREATE ARRAY timeseries (
    tick TIMESTAMP DIMENSION,
    payload FLOAT DEFAULT 0.0 );

CREATE TABLE experiment2(
    run date DIMENSION[ TIMESTAMP '2010-01-01':*: INTERVAL'1' day],
    payload ARRAY (
        x integer DIMENSION[4],
        y integer DIMENSION[4],
        val float DEFAULT 0.0)
);
