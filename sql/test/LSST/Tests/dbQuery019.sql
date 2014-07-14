-- http://dev.lsstcorp.org/trac/wiki/dbQuery019 
SELECT objectId
FROM   Object
JOIN   DIASource USING(objectId)
WHERE  latestObsTime > '2007-01-01'
GROUP BY (objectId)
HAVING COUNT(objectId) = 1;
