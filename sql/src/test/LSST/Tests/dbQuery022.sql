-- http://dev.lsstcorp.org/trac/wiki/dbQuery022 
INSERT INTO Neighbors(objectId, neighborObjectId, distance)
  SELECT o1.objectId, o2.objectId,
         3600*DEGREES( ACOS(o1.cx*o2.cx+o1.cy*o2.cy+o1.cz*o2.cz) ) AS dist
  FROM   Object o1, Object o2
  WHERE  o1.objectId <> o2.objectId
    AND  o2.zone BETWEEN o1.zone-1 AND o1.zone+1
    AND  o2.ra BETWEEN o1.ra-0.0003/COS(RADIANS(o2.decl))
    AND  o1.ra+0.0003/COS(RADIANS(o2.decl))
  HAVING dist<1.0;
