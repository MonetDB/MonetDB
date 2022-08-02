-- http://dev.lsstcorp.org/trac/wiki/dbQuery045 
SELECT objectId
FROM   Galaxy
WHERE  uVarProb = 100
   OR  gVarProb = 100
   OR  rVarProb = 100
   OR  iVarProb = 100
   OR  zVarProb = 100
   OR  yVarProb = 100;
