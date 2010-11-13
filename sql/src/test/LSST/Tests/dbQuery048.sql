-- http://dev.lsstcorp.org/trac/wiki/dbQuery048 
SELECT objectId
FROM   (SELECT v.objectId AS objectId,
               COUNT(n.neighborObjectId) AS neighbors
        FROM   VarObject v
        JOIN   Neighbors n USING (objectId)
        WHERE  n.distance < 0.1
        GROUP BY v.objectId) AS C
WHERE  neighbors > 0.4;
