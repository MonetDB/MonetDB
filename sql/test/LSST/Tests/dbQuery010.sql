-- http://dev.lsstcorp.org/trac/wiki/dbQuery010 
SELECT  s1.objectId AS objectId1,
        s2.objectId AS objectId2
FROM    Star      AS s1
JOIN    Star      AS s2 USING (objectId)
JOIN    Neighbors AS N  ON (s2.objectId = N.neighborObjectId)
WHERE   N.distance < 0.5/60       -- distance is 1/2 arc second or less
--  AND   s1.run != s2.run -- observations are from two different runs (SDSS)
  AND   s1.uMag BETWEEN 1 AND 27  -- magnitudes are reasonable
  AND   s1.gMag BETWEEN 1 AND 27
  AND   s1.rMag BETWEEN 1 AND 27
  AND   s1.iMag BETWEEN 1 AND 27
  AND   s1.zMag BETWEEN 1 AND 27
  AND   s1.yMag BETWEEN 1 AND 27
  AND   s2.uMag BETWEEN 1 AND 27
  AND   s2.gMag BETWEEN 1 AND 27
  AND   s2.rMag BETWEEN 1 AND 27
  AND   s2.iMag BETWEEN 1 AND 27
  AND   s2.zMag BETWEEN 1 AND 27
  AND   s2.yMag BETWEEN 1 AND 27
  AND (                           -- and one of the colors is  different.
         ABS(s1.uMag-s2.uMag) > .1 + (ABS(s1.uMagErr) + ABS(s2.uMagErr))
      OR ABS(s1.gMag-s2.gMag) > .1 + (ABS(s1.gMagErr) + ABS(s2.gMagErr))
      OR ABS(s1.rMag-s2.rMag) > .1 + (ABS(s1.rMagErr) + ABS(s2.rMagErr))
      OR ABS(s1.iMag-s2.iMag) > .1 + (ABS(s1.iMagErr) + ABS(s2.iMagErr))
      OR ABS(s1.zMag-s2.zMag) > .1 + (ABS(s1.zMagErr) + ABS(s2.zMagErr))
      OR ABS(s1.yMag-s2.yMag) > .1 + (ABS(s1.yMagErr) + ABS(s2.yMagErr)));
