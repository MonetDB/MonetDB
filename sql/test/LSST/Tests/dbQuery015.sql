-- http://dev.lsstcorp.org/trac/wiki/dbQuery015 
SELECT v.ra, v.decl
FROM   VarObject v, Object o
WHERE  abs(v.ra-o.ra) < 2
   AND abs(v.decl-o.decl) < 3
   AND o.objectId = 1234;
