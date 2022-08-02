-- http://dev.lsstcorp.org/trac/wiki/dbQuery044 
SELECT v.objectId, v.ra, v.decl,
       o.objectId, o.ra, o.decl
FROM   VarObject v, Star o
JOIN   _Object2Type USING (objectId)
JOIN   ObjectType   ON (_Object2Type.typeId = ObjectType.typeId)
WHERE  v.objectId <> o.objectId
   AND ABS(v.ra - o.ra) < 0.003 / COS(RADIANS(o.decl))
   AND ABS(v.decl - o.decl) < 0.003
   AND ObjectType.description = "star"
   AND (o.grColor < 0.0 OR o.riColor < 0.0 OR o.ugColor < 1.0);
