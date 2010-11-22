-- http://dev.lsstcorp.org/trac/wiki/dbQuery004 
SELECT uMag, gMag, rMag, iMag, zMag, yMag
FROM   Object
WHERE  (objectId  % 100 )= 0.01;
