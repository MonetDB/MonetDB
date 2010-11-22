-- http://dev.lsstcorp.org/trac/wiki/dbQuery053 
SELECT parallax, uMag, gMag, rMag, iMag, zMag, yMag, muRA, muDecl,
       uVarProb, gVarProb, rVarProb, iVarProb, zVarProb, yVarProb
FROM   Star
WHERE  ra BETWEEN 1 AND 2
  AND  decl BETWEEN 5 AND 6;
