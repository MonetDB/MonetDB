-- http://dev.lsstcorp.org/trac/wiki/dbQuery024 
SELECT  G.objectId,                               -- output galaxy
        G.uMag,G.gMag,G.rMag,G.iMag,G.zMag,G.yMag -- and magnitudes
FROM    Galaxy G
JOIN    _Source2Object M1 ON (G.objectId = M1.objectId)
JOIN    _Source2Object M2 ON (M1.sourceId = M2.sourceId)
JOIN    Star S ON (M2.objectId = S.objectId);
