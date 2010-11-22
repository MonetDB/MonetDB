-- http://dev.lsstcorp.org/trac/wiki/dbQuery013 
SELECT  DISTINCT o.objectId
FROM    Object     o
JOIN    Neighbors  n USING (objectId)
JOIN    Object     x ON (x.objectId = n.neighborObjectId)
  AND o.objectId <> x.objectId
  AND ABS(o.ugColor - x.ugColor) < 0.05  -- o and x have similar spectra
  AND ABS(o.grColor - x.grColor) < 0.05
  AND ABS(o.riColor - x.riColor) < 0.05 
  AND ABS(o.izColor - x.izColor) < 0.05;
