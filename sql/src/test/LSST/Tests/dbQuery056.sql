-- http://dev.lsstcorp.org/trac/wiki/dbQuery056
SELECT count(*) 
FROM   Source_pt1;

SELECT count(*) 
FROM   Source_pt1
JOIN   Object USING (objectId)
WHERE  extendedParam < 0.2;

SELECT count(*) 
FROM   Object
JOIN   VarObject USING (objectId
JOIN   _Object2Type USING (objectId) 
JOIN   ObjectType   USING (typeId)
WHERE  ObjectType.description = "star";


