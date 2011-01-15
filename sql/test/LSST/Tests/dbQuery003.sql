-- http://dev.lsstcorp.org/trac/wiki/dbQuery003 
SELECT  *
FROM    Object
WHERE   ra      BETWEEN 1 AND 2
   AND  decl    BETWEEN 1 AND 2
   AND  zMag    BETWEEN 1 AND 2
   AND  grColor BETWEEN 1 AND 2
   AND  izColor BETWEEN 1 AND 2;
