-- http://dev.lsstcorp.org/trac/wiki/dbQuery055 
SELECT COUNT(*)
FROM   Star
WHERE  ra BETWEEN 1 AND 2
  AND  decl BETWEEN 2 AND 3;
