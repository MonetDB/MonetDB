START TRANSACTION;
CREATE TABLE foo (i INT, s STRING);
CREATE TABLE bar (i INT, s STRING);

SELECT
    CASE 
        WHEN f.i % 2 THEN
            f.s
		ELSE
            (SELECT b.s FROM bar b WHERE b.i = f.i)
	END
		FROM foo f;


ROLLBACK;
