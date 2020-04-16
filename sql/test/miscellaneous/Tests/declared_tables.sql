start transaction;
declare table iamdeclared (a int, b varchar(16));
insert into iamdeclared values (1, 'one');
select a, b from iamdeclared;
drop table iamdeclared;
rollback;

declare table x (a int);
declare table x (b int); --error table x is already declared
drop table x;
drop table x; --error, not table x exists

declare a int;
select a;
plan declare b int;
explain declare b int;
select a;
select b; --error, b was never declared

/* Testing declared tables inside UDFs */
CREATE OR REPLACE FUNCTION testtruncate() RETURNS INT 
BEGIN
    DECLARE TABLE z (a int);
    INSERT INTO z VALUES (1);
    UPDATE z SET a = 2 WHERE a = 1;
    TRUNCATE z;
    INSERT INTO z VALUES (3);
    DELETE FROM z WHERE a = 3;
    RETURN SELECT a FROM z;
END;

CREATE OR REPLACE FUNCTION testtruncate() RETURNS INT 
BEGIN
    DECLARE TABLE w (c int);
    INSERT INTO w VALUES (1);
    UPDATE w SET c = 2 WHERE c = 1;
    TRUNCATE w;
    INSERT INTO w VALUES (3);
    DELETE FROM w WHERE c = 3;
    RETURN SELECT c FROM w;
END;

SELECT testtruncate();
SELECT testtruncate();

DROP FUNCTION testtruncate;
