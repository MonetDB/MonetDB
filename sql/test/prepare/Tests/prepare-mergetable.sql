START TRANSACTION;
CREATE FUNCTION mymax(a int, b int) returns int begin if a > b then return a; else return b; end if; end; 
CREATE MERGE TABLE x(a int);
CREATE TABLE child1(a int);
INSERT INTO child1 VALUES (0),(2),(3);
ALTER TABLE x ADD TABLE child1;
PREPARE SELECT mymax(a, ?) from x;
exec **(1);
ROLLBACK;
