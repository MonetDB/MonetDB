-- http://dev.lsstcorp.org/trac/wiki/dbQuery014 
SELECT *
FROM   Galaxy
WHERE  ra   BETWEEN 1 AND 2
   AND decl BETWEEN 1 AND 2;
