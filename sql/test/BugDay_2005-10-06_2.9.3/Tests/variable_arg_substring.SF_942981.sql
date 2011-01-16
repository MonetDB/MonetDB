CREATE TABLE N(i int);
INSERT INTO N VALUES(1),(5),(100),(3),(7);
SELECT SUBSTRING('hallo' FROM i FOR 3) FROM N WHERE i <= CHAR_LENGTH('hallo') + 2;
drop table N;
