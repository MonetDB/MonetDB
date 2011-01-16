-- http://dev.lsstcorp.org/trac/wiki/dbQuery018 
SELECT objectId,
       uMag, gMag, rMag, iMag, zMag, yMag
       ra, decl
FROM   Star       -- or Galaxy
WHERE  ( ugColor > 2.0 OR uMag > 22.3 )
AND    iMag BETWEEN 0 AND 19
AND    grColor > 1.0
AND    ( riColor < (0.08 + 0.42 * (grColor - 0.96)) OR grColor > 2.26 )
AND    izColor < 0.25;
