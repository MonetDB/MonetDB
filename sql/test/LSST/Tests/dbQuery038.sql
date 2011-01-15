-- http://dev.lsstcorp.org/trac/wiki/dbQuery038
SELECT  objectId
FROM    Galaxy                               -- select galaxies   
WHERE   rMag + rho < 24                      -- brighter than magnitude 24 in the red spectral band  
    AND isoA_r BETWEEN 30 AND 60;             -- major axis between 30" and 60"

