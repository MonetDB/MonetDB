START TRANSACTION;

SAVEPOINT MonetSP0;

CREATE TABLE table_Test_Csavepoints ( id int, PRIMARY KEY (id) );

SAVEPOINT MonetSP1;

SELECT id FROM table_Test_Csavepoints;

INSERT INTO table_Test_Csavepoints VALUES (1);
INSERT INTO table_Test_Csavepoints VALUES (2);
INSERT INTO table_Test_Csavepoints VALUES (3);

SAVEPOINT MonetSP2;

SELECT id FROM table_Test_Csavepoints;

RELEASE SAVEPOINT MonetSP0;

-- Mserver crashes here
SELECT id FROM table_Test_Csavepoints;

ROLLBACK;
