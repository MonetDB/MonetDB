-- http://dev.lsstcorp.org/trac/wiki/dbQuery032 
SELECT objectId
FROM   Galaxy
WHERE  rMag < 3
AND  extinction_r > 12 ;
