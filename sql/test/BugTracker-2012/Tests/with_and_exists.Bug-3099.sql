CREATE TABLE rank (pre INTEGER, post INTEGER);
INSERT INTO rank VALUES (1, 6);
INSERT INTO rank VALUES (2, 3);
INSERT INTO rank VALUES (4, 5);

WITH
  span1 AS ( SELECT pre FROM rank ),
  span2 AS ( SELECT pre FROM rank )
SELECT span1.pre AS pre1, span2.pre AS pre2
FROM   span1, span2
WHERE  EXISTS (
         SELECT 1
         FROM   rank as ancestor
         WHERE  ancestor.pre < span1.pre
         AND    span1.pre < ancestor.post
         AND    ancestor.pre < span2.pre
         AND    span2.pre < ancestor.post)
AND    span1.pre <> span2.pre
ORDER BY pre1, pre2;

WITH
  span1 AS ( SELECT pre AS pre1 FROM rank ),
  span2 AS ( SELECT pre AS pre2 FROM rank )
SELECT DISTINCT span1.pre1 AS pre1, span2.pre2 AS pre2
FROM   span1, span2
WHERE  EXISTS (
         SELECT 1
         FROM   rank as ancestor
         WHERE  ancestor.pre < span1.pre1
         AND    span1.pre1 < ancestor.post
         AND    ancestor.pre < span2.pre2
         AND    span2.pre2 < ancestor.post)
AND    span1.pre1 <> span2.pre2
ORDER BY pre1, pre2;

SELECT DISTINCT span1.pre AS pre1, span2.pre AS pre2
FROM   rank AS span1, rank AS span2
WHERE  EXISTS (
         SELECT ancestor.pre
         FROM   rank as ancestor
         WHERE  ancestor.pre < span1.pre
         AND    span1.pre < ancestor.post
         AND    ancestor.pre < span2.pre
         AND    span2.pre < ancestor.post)
AND    span1.pre <> span2.pre
ORDER BY pre1, pre2;

WITH
  span1 AS ( SELECT pre FROM rank ),
  span2 AS ( SELECT pre FROM rank )
SELECT DISTINCT span1.pre AS pre1, span2.pre AS pre2
FROM   span1, span2, rank AS ancestor
WHERE  ancestor.pre < span1.pre
AND    span1.pre < ancestor.post
AND    ancestor.pre < span2.pre
AND    span2.pre < ancestor.post
AND    span1.pre <> span2.pre
ORDER BY pre1, pre2;

drop table rank;
