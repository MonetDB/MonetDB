-- http://dev.lsstcorp.org/trac/wiki/dbQuery026 
SELECT objectId, ra, decl, uMag
FROM   Star
WHERE  (muRa > 0.5 OR muDecl > 0.5)
   AND rMag > 18.0
   AND grColor < 0.2;
