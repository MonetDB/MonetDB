-- http://dev.lsstcorp.org/trac/wiki/dbQuery054 
SELECT objectId
FROM   Star
WHERE  ra BETWEEN 1 AND 2
  AND  decl BETWEEN 2 AND 3
  AND  grColor > 0.1
  AND  (muRa * muRa + muDecl * muDecl) < 0.34
  AND  redshift BETWEEN 2 AND 3;
