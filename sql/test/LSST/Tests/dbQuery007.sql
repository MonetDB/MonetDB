-- http://dev.lsstcorp.org/trac/wiki/dbQuery007 
-- variables need to be fixed 
-- (:raMin, :declMin, :raMax, :declMax)= (1,2,3,4)
-- (:varMin AND :varMax) (5,6)
SELECT *
FROM   Source_pt1
JOIN   Filter USING(filterId)
WHERE  areaSpec_box(1,2,3,4)
  AND  filterName = 'u'
  -- AND  variability BETWEEN 5 AND 6     UNKNOWN attribute
ORDER BY objectId, taiMidPoint ASC;
