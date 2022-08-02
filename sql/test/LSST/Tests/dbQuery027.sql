-- http://dev.lsstcorp.org/trac/wiki/dbQuery027 
SELECT *
FROM   Object
WHERE  ABS(muRa)   > 1 * muRaErr
    OR ABS(muDecl) > 1 * muDeclErr;
