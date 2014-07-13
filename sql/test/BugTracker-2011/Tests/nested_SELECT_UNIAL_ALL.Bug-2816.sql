CREATE TABLE bug_2816 (c INT);
INSERT INTO bug_2816 VALUES (1);
SELECT outtab.c
FROM (SELECT intab.c
      FROM (SELECT bug_2816.c
            FROM bug_2816) intab
            UNION ALL 
            SELECT bug_2816.c
            FROM bug_2816) outtab
GROUP BY outtab.c;
DROP TABLE bug_2816;
