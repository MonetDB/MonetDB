-- http://dev.lsstcorp.org/trac/wiki/dbQuery035 
SELECT  objectId
FROM    Object
WHERE   uMag-gMag < 0.4
   AND  gMag-rMag < 0.7
   AND  rMag-iMag > 0.4
   AND  Mag-zMag > 0.4;
