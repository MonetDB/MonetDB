SELECT INTERVAL '1' HOUR / 2, INTERVAL '1' HOUR * 1000 / 2000; --all output 1800.000
SELECT INTERVAL '1' HOUR / 2.0; -- 1800.000
SELECT INTERVAL '1' HOUR * 1000.0 / 2000.0; --1800.000
SELECT INTERVAL '1' HOUR * 1000 / 1800000; -- 2.000
SELECT INTERVAL '1' HOUR * CAST(1000 AS DOUBLE); -- 3600000.000
SELECT INTERVAL '4' MONTH * 2.03; -- 8
SELECT INTERVAL '1' MONTH * 1.2; -- 1
SELECT INTERVAL '1' MONTH / 2.0; -- 0
SELECT INTERVAL '1' MONTH / 1.5; -- 0
SELECT INTERVAL '1' MONTH / 1.0; -- 1
SELECT INTERVAL '1' SECOND * 1.2; --1.200
SELECT INTERVAL '2' MONTH / -1.4; -- 2
SELECT INTERVAL '1' HOUR / INTERVAL '1800' SECOND; --error on typing branch, cannot divide intervals
SELECT INTERVAL '3' MONTH * INTERVAL '3' MONTH; --error on typing branch, cannot multiply intervals
select mya + interval '2' second from (select interval '3' second * 1.2) as mya(mya); -- 5.600

SELECT INTERVAL '5' MONTH * cast(2.44 as double); -- 12
SELECT INTERVAL '5' MONTH * cast(2.29 as real); -- 11
SELECT INTERVAL '1' MONTH * cast(1.0 as double); -- 1
SELECT INTERVAL '1' SECOND * cast(2.44 as double); -- 2.440
SELECT INTERVAL '5' SECOND * cast(2.29 as real); -- 11.450
SELECT INTERVAL '5' SECOND * cast(1.0 as double); -- 5.000

SELECT INTERVAL '1' MONTH / cast(2.0 as double); -- 0
SELECT INTERVAL '1' MONTH / cast(1.5 as double); -- 0
SELECT INTERVAL '1' MONTH / cast(1.0 as double); -- 1

SELECT INTERVAL '1' SECOND / cast(2.0 as double); -- 0.500
SELECT INTERVAL '5' SECOND / cast(1.5 as double); -- 3.330
SELECT INTERVAL '5' SECOND / cast(1.0 as double); -- 5.000
