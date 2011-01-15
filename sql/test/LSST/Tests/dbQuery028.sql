set     @binned =       dbo.fPhotoFlags('BINNED1') +    -- avoids SQL2K optimizer problem
                        dbo.fPhotoFlags('BINNED2') +
                        dbo.fPhotoFlags('BINNED4') ;
set     @blended =      dbo.fPhotoFlags('BLENDED');     -- avoids SQL2K optimizer problem
set     @noDeBlend =    dbo.fPhotoFlags('NODEBLEND');   -- avoids SQL2K optimizer problem
set     @child =        dbo.fPhotoFlags('CHILD');       -- avoids SQL2K optimizer problem
set     @edge =         dbo.fPhotoFlags('EDGE');        -- avoids SQL2K optimizer problem
set     @saturated =    dbo.fPhotoFlags('SATURATED');   -- avoids SQL2K optimizer problem

-- http://dev.lsstcorp.org/trac/wiki/dbQuery028
SELECT  G.objectId, COUNT(N.NeighborObjID) AS pop
FROM    Galaxy     AS G,                                     -- first gravitational lens candidate   
JOIN    Neighbors  AS N   ON (G.objectId = N.objectId),      -- precomputed list of neighbors
        Galaxy     AS U   ON (U.objectId = N.neighborObjId), -- a neighbor galaxy of G
        photoZ     AS Gpz ON (G.objectId = Gpz.objectId),    -- photoZ of G.
        photoZ     AS Npz ON (U.objectId = Npz.objectId)     -- Neighbor photoZ
WHERE   G.ra   BETWEEN 190 AND 200      -- changed range so matches perspnal DB.
  AND   G.decl BETWEEN -5  AND 5
  AND   N.objectId < N.neighborObjID    -- 30 arcseconds of one another.
  AND   ABS(Gpz.Z - Npz.Z) < 0.05       -- restrict the photoZ differences
  -- Color cut for an BCG courtesy of James Annis of Fermilab
  AND (G.flags & @binned) > 0  
  AND (G.flags & ( @blended + @noDeBlend + @child)) != @blended
  AND (G.flags & (@edge + @saturated)) = 0  
  AND G.petroMag_i > 17.5
  AND (G.petroMag_r > 15.5 OR G.petroR50_r > 2)
  AND (G.petroMag_r < 30 AND G.g < 30 AND G.r < 30 AND G.i < 30)
  AND (G.rMag < 19.2 
  AND ( 1=1 or
        (G.rMag < (13.1 + (7/3)*G.grColor +     -- deRed_r < 13.1 + 0.7 / 0.3 * deRed_gr
                4 *(G.riColor - 0.18 ))         -- 1.2 / 0.3 * deRed_ri          
                AND (( G.riColor - G.grColor/4 - 0.18) BETWEEN -0.2 AND  0.2 )
                AND ((G.rMag +                  -- petSB - deRed_r + 2.5 log10(2Pi*petroR50^2)
                        2.5 * LOG( 2 * 3.1415 * G.petroR50_r * G.petroR50_r )) < 24.2 )  
        ) OR
        ((G.rMag < 19.5 )
         AND (( G.riColor - G.grColor/4 -.18) >
         AND (G.grColor > ( 1.35 + 0.25 * G.riColor ))
         AND ((G.rMag  +        -- petSB - deRed_r + 2.5 log10(2Pi*petroR50^2)          
                2.5 * LOG( 2 * 3.1415 * G.petroR50_r * G.petroR50_r )) < 23.3 ))
        )
      )
GROUP BY G.objectId;

