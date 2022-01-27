START TRANSACTION;

CREATE TABLE Test (c1 int not null, c2 varchar(255) not null, c3 int not null);
INSERT INTO Test VALUES (1, 'asd', 1);
PREPARE INSERT INTO Test SELECT c1, ?, ? FROM Test;
EXEC **('aa', 2);
EXEC **(10, '9');
PREPARE INSERT INTO Test SELECT ?, ?, ? FROM Test;
EXEC **(4, 'cc', 3);
EXEC **('11', 12, '13');

SELECT c1, c2, c3 FROM Test;

CREATE TABLE Test2 (c1 int not null, c2 varchar(255) not null, c3 varchar(255) null);
INSERT INTO Test2 VALUES (1, 'asd', 'asd');
PREPARE INSERT INTO Test2 SELECT c1, ?, ? FROM Test2;
EXEC **('bb', 'aa');
EXEC **(14, 15);
PREPARE INSERT INTO Test2 SELECT ?, ?, ? FROM Test2;
EXEC **(5, 'ee','dd');
EXEC **('16', 17, 18);

SELECT c1, c2, c3 FROM Test2;

CREATE TABLE Test3 (c1 int, c2 varchar(255), c3 int);
INSERT INTO Test3 VALUES (1, 'asd', 1);
PREPARE INSERT INTO Test3 SELECT c1, ?, ? FROM Test3;
EXEC **('ff', 6);
EXEC **(19, '20');
PREPARE INSERT INTO Test3 SELECT ?, ?, ? FROM Test3;
EXEC **(7, 'gg', 8);
EXEC **('21', 22, '23');

SELECT c1, c2, c3 FROM Test3;

ROLLBACK;
