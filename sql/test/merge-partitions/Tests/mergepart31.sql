START TRANSACTION;
CREATE MERGE TABLE splitted (stamp TIMESTAMP, val INT) PARTITION BY RANGE ON (stamp);
CREATE TABLE first_decade (stamp TIMESTAMP, val INT);
CREATE TABLE second_decade (stamp TIMESTAMP, val INT);
CREATE TABLE third_decade (stamp TIMESTAMP, val INT);

ALTER TABLE splitted ADD TABLE first_decade AS PARTITION FROM TIMESTAMP '2000-01-01 00:00:00' TO TIMESTAMP '2010-01-01 00:00:00';
ALTER TABLE splitted ADD TABLE second_decade AS PARTITION FROM TIMESTAMP '2010-01-01 00:00:00' TO TIMESTAMP '2020-01-01 00:00:00';
ALTER TABLE splitted ADD TABLE third_decade AS PARTITION FROM TIMESTAMP '2020-01-01 00:00:00' TO RANGE MAXVALUE WITH NULL VALUES;
INSERT INTO splitted VALUES (TIMESTAMP '2000-01-01 00:00:00', 1), (TIMESTAMP '2002-12-03 20:00:00', 2), (TIMESTAMP '2012-05-12 21:01:00', 3), (TIMESTAMP '2019-12-12 23:59:59', 4);
INSERT INTO splitted VALUES (TIMESTAMP '2020-01-01 00:00:00', 5), (NULL, 6);

plan select 1 from splitted where stamp = TIMESTAMP '2020-01-01 00:00:00'; --only third child passes

plan select 1 from splitted where stamp IN (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2000-02-01 00:00:00'); --only first child passes

plan select 1 from splitted where stamp IN (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2010-01-01 00:00:00'); --third child pruned

plan select 1 from splitted where stamp IN (TIMESTAMP '2000-02-01 00:00:00', TIMESTAMP '2010-02-01 00:00:00', TIMESTAMP '2020-02-01 00:00:00'); --nothing gets pruned

plan select 1 from splitted where stamp BETWEEN TIMESTAMP '2020-01-01 00:00:00' AND TIMESTAMP '2020-10-01 00:00:00'; --only third child passes

plan select 1 from splitted where stamp NOT BETWEEN TIMESTAMP '2020-01-01 00:00:00' AND TIMESTAMP '2020-10-01 00:00:00'; --third child pruned

plan select 1 from splitted where stamp BETWEEN TIMESTAMP '2010-01-01 00:00:00' AND TIMESTAMP '2020-03-01 00:00:00'; --first child pruned

plan select 1 from splitted where stamp BETWEEN TIMESTAMP '2000-02-01 00:00:00' AND TIMESTAMP '2020-03-01 00:00:00'; --nothing gets pruned

plan select 1 from splitted where stamp NOT BETWEEN TIMESTAMP '2000-02-01 00:00:00' AND TIMESTAMP '2020-03-01 00:00:00'; --all children pruned

select * from splitted where stamp NOT BETWEEN TIMESTAMP '2000-02-01 00:00:00' AND TIMESTAMP '2020-03-01 00:00:00';
	-- empty

plan select 1 from splitted where stamp > TIMESTAMP '2010-03-01 00:00:00'; --first child pruned

plan select 1 from splitted where stamp <= TIMESTAMP '2009-01-01 00:00:00'; --only first child passes

plan select 1 from splitted where stamp >= TIMESTAMP '2010-01-01 00:00:00' AND stamp < TIMESTAMP '2019-01-01 00:00:00'; --only second child passes

plan select 1 from splitted where stamp <= TIMESTAMP '2020-10-01 00:00:00'; --nothing gets pruned

plan select 1 from splitted where stamp < TIMESTAMP '2000-01-01 00:00:00'; --all children pruned

plan select 1 from splitted where stamp <= TIMESTAMP '2000-01-01 00:00:00'; --only first child passes

plan select 1 from splitted where stamp is null; --only third child passes

plan select 1 from splitted where stamp is null and stamp < TIMESTAMP '2008-01-01 00:00:00'; --all children pruned

plan select 1 from splitted where stamp > TIMESTAMP '2020-01-01 00:00:00' and stamp <= TIMESTAMP '2020-01-01 00:00:00'; --only third child passes

plan select 1 from splitted where stamp >= TIMESTAMP '2000-01-01 00:00:00' and stamp < TIMESTAMP '2020-01-01 00:00:00'; --third child pruned

plan select 1 from splitted where stamp > TIMESTAMP '2010-01-01 00:00:00' and stamp < TIMESTAMP '2020-01-01 00:00:00'; --only second child passes

plan select 1 from splitted where stamp >= TIMESTAMP '2010-01-01 00:00:00' and stamp < TIMESTAMP '2020-01-01 00:00:00'; --only second child passes

plan select 1 from splitted where stamp > TIMESTAMP '2001-01-02 00:00:00' and stamp < TIMESTAMP '2015-01-01 00:00:00'; --third child pruned

plan select 1 from splitted where stamp > TIMESTAMP '2010-01-01 00:00:00' and stamp < TIMESTAMP '2010-01-01 00:00:00'; --all children pruned

plan select 1 from splitted where stamp > TIMESTAMP '2009-01-01 00:00:00' and stamp <= TIMESTAMP '2010-01-01 00:00:00'; --third child pruned

plan select 1 from splitted where stamp > TIMESTAMP '2009-01-01 00:00:00' and stamp <= TIMESTAMP '2020-01-01 00:00:00'; --nothing gets pruned

CREATE TABLE fourth_decade (stamp TIMESTAMP, val INT);
ALTER TABLE splitted ADD TABLE fourth_decade AS PARTITION FROM RANGE MINVALUE TO TIMESTAMP '2000-01-01 00:00:00';
INSERT INTO splitted VALUES (TIMESTAMP '1999-01-01 00:00:00', 7);

plan select 1 from splitted where stamp >= TIMESTAMP '2000-01-01 00:00:00' and stamp <= TIMESTAMP '2001-01-01 00:00:00'; --only first child passes

plan select 1 from splitted where stamp > TIMESTAMP '1999-01-01 00:00:00' and stamp <= TIMESTAMP '2001-01-01 00:00:00'; --second and third children pruned

plan select 1 from splitted where stamp = TIMESTAMP '2010-01-01 00:00:00'; --only second child passes

plan select 1 from splitted where stamp = TIMESTAMP '2000-01-01 00:00:00'; --only first child passes

ALTER TABLE splitted DROP TABLE second_decade;
ALTER TABLE splitted DROP TABLE third_decade;
ALTER TABLE splitted DROP TABLE fourth_decade;

plan select 1 from splitted where stamp = TIMESTAMP '2010-01-01 00:00:00'; --all children pruned

select 1 from splitted where stamp = TIMESTAMP '2010-01-01 00:00:00'; --all children pruned
 	-- empty

CREATE MERGE TABLE splitted2 (stamp INT, val INT) PARTITION BY VALUES ON (stamp);
CREATE TABLE first_decade2 (stamp INT, val INT);
CREATE TABLE second_decade2 (stamp INT, val INT);
CREATE TABLE third_decade2 (stamp INT, val INT);
ALTER TABLE splitted2 ADD TABLE first_decade2 AS PARTITION IN (1,2);
ALTER TABLE splitted2 ADD TABLE second_decade2 AS PARTITION IN (3,4) WITH NULL VALUES;
ALTER TABLE splitted2 ADD TABLE third_decade2 AS PARTITION IN (5,6,7);
INSERT INTO splitted2 VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6);

plan select 1 from splitted2 where stamp = 5; --only third child passes

plan select 1 from splitted2 where val = 1; --nothing gets pruned (the table is partitioned by column stamp)

plan select 1 from splitted2 where stamp is null; --only second child passes

plan select 1 from splitted2 where stamp is null and stamp in (3,4,5); --all children pruned

plan select 1 from splitted2 where stamp is null and stamp in (5,6); --all children pruned

plan select 1 from splitted2 where stamp = 10; --all children pruned

select 1 from splitted2 where stamp = 10;
 	-- empty

plan select 1 from splitted2 where stamp in (2,1); --only first child passes

plan select 1 from splitted2 where stamp in (4); --only second child passes

plan select 1 from splitted2 where stamp in (5,6) and stamp in (6,7); --only third child passes

plan select 1 from splitted2 where stamp in (5,6) and stamp > 100; --only third child passes

plan select 1 from splitted2 where stamp in (1,2,3); --all children pruned

plan select 1 from splitted2 where stamp in (8,9); --all children pruned

select 1 from splitted2 where stamp in (8,9); --all children pruned
 	-- empty

CREATE MERGE TABLE table1 (a int, b int) PARTITION BY RANGE ON (a);
CREATE TABLE another1 (a int, b int);
CREATE TABLE another2 (a int, b int);

ALTER TABLE table1 ADD TABLE another1 AS PARTITION FROM 10 TO 10;
ALTER TABLE table1 ADD TABLE another2 AS PARTITION FROM 11 TO 11;

plan select 1 from table1 where a = 10; --only first child passes

plan select 1 from table1 where a = 11; --only second child passes

plan select 1 from table1 where a = 10 or a = 11; --nothing gets pruned

plan select 1 from table1 where a >= 10; --nothing gets pruned

plan select 1 from table1 where a > 10; --only second child passes

plan select 1 from table1 where a <= 10; --only first child passes

plan select 1 from table1 where a between 10 and 10; --only first child passes

plan select 1 from table1 where a = 10 or b = 11; --nothing gets pruned

plan select 1 from table1 where a not between 10 and 11; --all children pruned

plan select 1 from table1 where a not between 10 and 10; --only second child passes

ROLLBACK;
