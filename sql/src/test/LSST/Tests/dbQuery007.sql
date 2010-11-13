-- http://dev.lsstcorp.org/trac/wiki/dbQuery007 
SELECT *
FROM   Source_pt1
JOIN   Filter USING(filterId)
WHERE  areaSpec_box(raMin, :declMin, :raMax, :declMax)
  AND  filterName = 'u'
  AND  variability BETWEEN :varMin AND :varMax
ORDER BY objectId, taiObs ASC;
