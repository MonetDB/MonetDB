-- http://dev.lsstcorp.org/trac/wiki/dbQuery001 
SELECT taiMidPoint, modelMag, modelMagErr
FROM   Source
WHERE  objectId = 123
   AND filterId = 3;
