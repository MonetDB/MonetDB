-- http://dev.lsstcorp.org/trac/wiki/dbQuery043 
SELECT  S1.movingObjectId AS s1, 
        S2.movingObjectId AS s2
FROM    MovingObject S1,                                   -- S1 is the white dwarf
        MovingObject S2,                                   -- S2 is the second star
		DiaSource E1, DiaSource E2
WHERE   E1.extendedness < 0.2                       -- is star
   AND  E2.extendedness < 0.2                       -- is star
  AND E1.diaSourceId = S1.movingObjectId
  AND E2.diaSourceId = S2.movingObjectId
--   AND  spDist(S1.ra, S1.decl, S2.ra, S2.decl) < .05 -- the 5 arcsecond test
   AND  S1.uMag-S1.gMag < 0.4                        -- and S1 meets Paul Szkody's color cut
   AND  S1.gMag-S1.rMag < 0.7                        -- for white dwarfs
   AND  S1.rMag-S1.iMag > 0.4 
   AND  S1.iMag-S1.zMag > 0.4;
