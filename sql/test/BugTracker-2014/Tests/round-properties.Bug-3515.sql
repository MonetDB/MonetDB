START TRANSACTION;
CREATE TABLE ceil_floor_round (a numeric);
INSERT INTO ceil_floor_round VALUES ('-5.499999');
INSERT INTO ceil_floor_round VALUES ('-5.499');
INSERT INTO ceil_floor_round VALUES ('0.0');
SELECT a, round(a, 0) FROM ceil_floor_round;
ROLLBACK;
