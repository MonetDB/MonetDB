-- http://dev.lsstcorp.org/trac/wiki/dbQuery046
SELECT o.objectId
FROM   BRG
JOIN   Neighbors n USING (objectId)
JOIN   Object o ON (n.neighborId = o.objectId)
WHERE  n.radius < :distance
  AND  Ellipticity(o.fwhmA, o.fwhmB) > :ellipticity
  AND  o.fwhmTheta BETWEEN :fwhmMin AND :fwhmMax;

