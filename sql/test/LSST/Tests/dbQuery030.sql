SET @binned =   dbo.fPhotoFlags('BINNED1') +
dbo.fPhotoFlags('BINNED2') +
                dbo.fPhotoFlags('BINNED4') 
-- http://dev.lsstcorp.org/trac/wiki/dbQuery030
SET @deblendedChild =   dbo.fPhotoFlags('BLENDED')   +
                        dbo.fPhotoFlags('NODEBLEND') +
                        dbo.fPhotoFlags('CHILD')
SET @blended = dbo.fPhotoFlags('BLENDED')
DECLARE @edgedSaturated BIGINT
SET @edgedSaturated =   dbo.fPhotoFlags('EDGE') +
                        dbo.fPhotoFlags('SATURATED')
SELECT  run,
        camCol,
        rerun,
        field,
        objID,
        ra,
        dec
FROM    Galaxy
WHERE  (flags & @binned)> 0  
   AND (flags & @deblendedChild) !=  @blended
   AND (flags & @edgedSaturated)  = 0  
   AND petroMag_i > 17.5
   AND (petroMag_r > 15.5 OR petroR50_r > 2)
   AND (petroMag_r > 0 AND g>0 AND r>0 AND i>0)
   AND (petroR50_r > 0 ) -- petroR50 value is valid, need to avoid log(0)
   AND ((    (petroMag_r-extinction_r) < 19.2
         AND (petroMag_r - extinction_r < (13.1 + (7/3)*( g - r) + 4 *( r  - i) - 4 * 0.18 ))          
         AND (((r-i) - (g-r)/4 - 0.18) <  0.2) 
         AND (((r-i) - (g-r)/4 - 0.18) > -0.2) 
         AND           -- petSB - deRed_r + 2.5 log10(2Pi*petroR50^2)
             ((petroMag_r - extinction_r + 2.5 * LOG( 2 * 3.1415 * petroR50_r * petroR50_r )) < 24.2)
             OR
             (    (petroMag_r - extinction_r < 19.5) 
              AND (( (r-i) - (g-r)/4 - 0.18 ) > (0.45 - 4*( g-r)) )  -- 0.45 - deRed_gr/0.25       
              AND ((g-r) > (1.35 + 0.25 *(r-i)))
              AND  -- petSB - deRed_r + 2.5 log10(2Pi*petroR50^2)
                  ((petroMag_r - extinction_r  +  2.5 * LOG(2 * 3.1415 * petroR50_r * petroR50_r)) < 23.3) 
             )
       )) ;
