CREATE TABLE ranges(low integer, hgh integer);
INSERT INTO ranges VALUES (1,3),(2,4),(5,6),(7,7);

SELECT * 
FROM generate_series(0,10,1) AS s JOIN ranges ON ( s.value >= ranges.low AND s.value < ranges.hgh);

DROP TABLE ranges;
