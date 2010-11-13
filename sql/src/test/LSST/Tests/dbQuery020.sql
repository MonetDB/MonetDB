-- http://dev.lsstcorp.org/trac/wiki/dbQuery020 
SELECT gMag, objectId
FROM   Galaxy
WHERE  gMag <= 22
   AND ugColor >= -0.27 AND ugColor < 0.71
   AND grColor >= -0.24 AND grColor < 0.35
   AND riColor >= -0.27 AND riColor < 0.57
   AND izColor >= -0.35 AND izColor < 0.70;
