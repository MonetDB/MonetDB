-- http://dev.lsstcorp.org/trac/wiki/dbQuery012
SELECT  COUNT(*)                                                AS total,     
        SUM( CASE WHEN (typeId=3) THEN 1 ELSE 0 END)            AS galaxies,
        SUM( CASE WHEN (typeId=6) THEN 1 ELSE 0 END)            AS stars,
        SUM( CASE WHEN (typeId NOT IN (3,6)) THEN 1 ELSE 0 END) AS other
FROM    Object
JOIN    _Object2Type USING(objectId)
WHERE  (ugColor > 2.0 OR uMag > 22.3) -- apply the quasar color cut.
   AND iMag BETWEEN 0 AND 19
   AND grColor > 1.0
   AND ( (riColor < 0.08 + 0.42 * (grColor - 0.96)) OR (grColor > 2.26 ) )
   AND izColor < 0.25;
