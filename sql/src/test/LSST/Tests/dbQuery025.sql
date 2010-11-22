-- http://dev.lsstcorp.org/trac/wiki/dbQuery025 
SELECT DISTINCT o1.objectId, o1.ra, o1.decl, ps.url
FROM   Galaxy o1, PostageStampJpegs ps
WHERE  ABS(ps.ra   - o1.ra  ) < ps.sizeRa/(2*COS(RADIANS(o1.decl)))
   AND ABS(ps.decl - o1.decl) < ps.sizeDecl/2
   AND (
        SELECT COUNT(o2.objectId)
        FROM   Galaxy o2
        WHERE  o1.objectId <> o2.objectId
          AND  ABS(o1.ra   - o2.ra  ) < 0.1/COS(RADIANS(o2.decl))
          AND  ABS(o1.decl - o2.decl) < 0.1
       ) > 10000;
