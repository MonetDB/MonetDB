-- http://dev.lsstcorp.org/trac/wiki/dbQuery037 
SELECT objectId, ra, decl,
       uMag, gMag, rMag, iMag, zMag, yMag
FROM   Galaxy
WHERE  izColor > 1.0;
