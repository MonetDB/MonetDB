CREATE FUNCTION groupElements(beg INTEGER)
RETURNS TABLE (x INTEGER)
BEGIN
    CREATE TABLE tmp(x INTEGER); 
    INSERT INTO tmp VALUES(beg), (beg+1), (beg+2);
    RETURN tmp; 
END;

SELECT * FROM groupElements(1);

CREATE TABLE tmp(x INTEGER);
INSERT INTO tmp VALUES (1),(2);
SELECT * FROM (SELECT * FROM tmp) AS A;

SELECT (SELECT * FROM groupElements(pivot.x))
FROM (SELECT * FROM tmp) as pivot;

DROP TABLE tmp;
DROP FUNCTION groupElements;
