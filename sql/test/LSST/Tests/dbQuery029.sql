-- http://dev.lsstcorp.org/trac/wiki/dbQuery029
SELECT 
          o.ra, o.decl, o.flags, o.type, o.objid,
          o.psfMag_g, o.psfMag_r, o.psfMag_i, o.gMag, o.rMag, o.iMag, 
          o.petroRad_r, 
          o.q_g, o.q_r, o.q_i, 
          o.u_g, o.u_r, o.u_i, 
          o.mE1_r, o.mE2_r, o.mRrCc_r, o.mCr4_r, 
          o.isoA_r, o.isoB_r, o.isoAGrad_r, o.isoBGrad_r, o.isoPhi_r, 
          n.distance, p.r, p.g
FROM      Object as o 
LEFT JOIN Neighbors as n on o.objid=n.objid, 
JOIN      Object p ON (p.objId = n.neighborObjId)
WHERE     (o.ra > 120) and (o.ra < 240) 
    AND   (o.r > 16.) and (o.r<21.0) 
    AND   n.neighborObjId = (
               SELECT TOP 1 nn.neighborObjId
               FROM   Neighbors nn
               JOIN   Object pp ON (nn.neighborObjId = pp.objectId)
               WHERE  nn.objectId = o.objectId 
               ORDER BY pp.r
                          )
	LIMIT 100;

