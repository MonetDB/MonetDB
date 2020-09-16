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

SELECT 1 / INTERVAL '2' MONTH;  --error on typing branch, cannot divide an integer by an interval

SELECT INTERVAL '5' MONTH * cast(2.44 as double); -- 12
SELECT INTERVAL '5' MONTH * cast(2.29 as real); -- 11
SELECT INTERVAL '1' MONTH * cast(1.0 as double); -- 1
SELECT INTERVAL '1' SECOND * cast(2.44 as double); -- 2.440
SELECT INTERVAL '5' SECOND * cast(2.29 as real); -- 11.450
SELECT INTERVAL '5' SECOND * cast(1.0 as double); -- 5.000

SELECT cast(2.56 as double) * INTERVAL '5' MONTH; -- 13
SELECT cast(3.1 as real) * INTERVAL '3' SECOND; -- 9.300

SELECT INTERVAL '1' MONTH / cast(2.0 as double); -- 1
SELECT INTERVAL '1' MONTH / cast(1.5 as double); -- 1
SELECT INTERVAL '1' MONTH / cast(1.0 as double); -- 1

SELECT INTERVAL '-10' MONTH / cast(2.0 as real); -- -5
SELECT INTERVAL '7' MONTH / cast(1.5 as real); -- 5
SELECT INTERVAL '9' YEAR / cast(1.0 as real); -- 108

SELECT INTERVAL '1' SECOND / cast(2.0 as double); -- 0.500
SELECT INTERVAL '5' SECOND / cast(1.5 as double); -- 3.330
SELECT INTERVAL '5' SECOND / cast(1.0 as double); -- 5.000

SELECT INTERVAL '-100' DAY / cast(23.34 as real); -- -370179.936
SELECT INTERVAL '32' MINUTE / cast(45.5677 as real); -- 42.135
SELECT INTERVAL '67' MINUTE / cast(1.57 as real); -- 2560.510

SELECT INTERVAL '-10.34' SECOND / cast(-1.8 as real); -- 5.744
SELECT INTERVAL '-10.34' SECOND / -1.8; -- 5.740

SELECT INTERVAL '42' DAY / cast(0 as real); -- division by zero
SELECT INTERVAL '-6' YEAR / 0.0; -- division by zero

SELECT x, y, x * y from (values(interval '0' month),(interval '-3' month),(interval '6' month)) as x(x), (values(1.1),(3.4),(-7)) as y(y);
SELECT x, y, x / y from (values(interval '0' second),(interval '-56' day),(interval '67' minute)) as x(x), (values(1.1),(3.4),(-7)) as y(y);

SELECT x, y, x * y from (values(interval '0' month),(interval '-3' month),(interval '6' month)) as x(x), (values(cast(1.1 as double)),(cast(3.4 as real)),(cast(-7 as double))) as y(y);
SELECT x, y, x / y from (values(interval '0' second),(interval '-56' day),(interval '67' minute)) as x(x), (values(cast(1.1 as double)),(cast(3.4 as real)),(cast(-7 as double))) as y(y);
