CREATE TABLE h (a int, b int);
INSERT INTO h VALUES (0, 10);
INSERT INTO h VALUES (10, 20);
INSERT INTO h VALUES (20, 30);

CREATE TABLE d (x int);
INSERT INTO d VALUES (5);
INSERT INTO d VALUES (6);
INSERT INTO d VALUES (11);

-- This one works:
SELECT h.a, h.b, COUNT(*) 
FROM   h
INNER JOIN d
ON    (h.a <= d.x AND d.x < h.b)
GROUP BY h.a, h.b
ORDER BY a, b
;

-- But this one crashes the server:
SELECT h.a, h.b, COUNT(*) 
FROM   h
INNER JOIN d
ON    (d.x BETWEEN h.a AND h.b)
GROUP BY h.a, h.b
ORDER BY a, b
;

drop table h;
drop table d;
