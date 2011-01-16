-- http://dev.lsstcorp.org/trac/wiki/dbQuery047 
SELECT v.objectId
FROM   Galaxy g
JOIN   Neighbors n USING (objectId)
JOIN   VarObject v ON (n.neighborObjectId = v.objectId)
WHERE  n.distance < 0.01;
