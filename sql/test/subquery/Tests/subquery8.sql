START TRANSACTION;
CREATE TABLE foo (i INT, s STRING);
CREATE TABLE bar (i INT, s STRING);

SELECT
    CASE 
        WHEN f.i % 2 THEN
            (SELECT b.s FROM bar b WHERE b.i = f.i)
		ELSE
            f.s
	END
		FROM foo f;


ROLLBACK;
