-- create a nested array
CREATE ARRAY experiment2(
    run date DIMENSION[ TIMESTAMP '2010-01-01': INTERVAL'1' day : *],
    payload ARRAY (x integer DIMENSION[4], y integer DIMENSION[4], v float DEFAULT 0.0)
);

DROP ARRAY experiment2;

