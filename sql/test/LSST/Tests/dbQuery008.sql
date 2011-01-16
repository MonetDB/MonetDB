-- http://dev.lsstcorp.org/trac/wiki/dbQuery008 
SELECT *
FROM   VarObject
WHERE  ra BETWEEN 1 AND 2
  AND  decl BETWEEN 2 AND 3;
