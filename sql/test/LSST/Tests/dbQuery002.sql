-- http://dev.lsstcorp.org/trac/wiki/dbQuery002 
SELECT *
FROM   VarObject
JOIN   _Object2Type USING(objectId)
WHERE  typeId = 2
AND    probability = 0.34;
