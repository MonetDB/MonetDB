-- http://dev.lsstcorp.org/trac/wiki/dbQuery011 
SELECT *
FROM   VarObject
WHERE  uTimescale < 1
   AND gTimescale < 1
   AND rTimescale < 1
   AND iTimescale < 1
   AND zTimescale < 1
   AND yTimescale < 1
    OR primaryPeriod BETWEEN 2 AND 2
    OR uAmplitude > 1
    OR gAmplitude > 1
    OR rAmplitude > 1
    OR iAmplitude > 1
    OR zAmplitude > 1
    OR yAmplitude > 1;
