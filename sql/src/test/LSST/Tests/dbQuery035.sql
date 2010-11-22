-- http://dev.lsstcorp.org/trac/wiki/dbQuery035 
SELECT  movingObjectId
FROM    MovingObject
WHERE   uMag-gMag < 0.4
   AND  gMag-rMag < 0.7
   AND  rMag-iMag > 0.4
   AND  iMag-zMag > 0.4;
