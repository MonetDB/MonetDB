-- http://dev.lsstcorp.org/trac/wiki/dbQuery036 
-- extendedParam ->extendedness
SELECT  ROUND(uMag-gMag,0) AS UG, 
        ROUND(gMag-rMag,0) AS GR, 
        ROUND(rMag-iMag,0) AS RI, 
        ROUND(iMag-zMag,0) AS IZ,
        ROUND(zMag-yMag,0) AS ZY,
        COUNT(*) AS pop
FROM    MovingObject, DiaSource
WHERE   DiaSource.extendedness < 0.2  -- is a star
  AND DiaSource.diaSourceId = MovingObject.movingObjectId
  AND   (uMag+gMag+rMag+iMag+zMag+yMag) < 150 -- exclude bogus magnitudes (== 999) 
GROUP BY UG, GR, RI, IZ, ZY
HAVING pop > 500    -- Common bucktes have 500 or more members, so delete them
ORDER BY pop;
