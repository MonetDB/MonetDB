create schema strimps;

CREATE FILTER FUNCTION strimps.filter(strs STRING, q STRING) EXTERNAL NAME strimps.strimpfilter;
GRANT EXECUTE ON FILTER FUNCTION strimps.filter TO PUBLIC;

CREATE PROCEDURE sys.createstrimps(sch string, tab string, col string)
       EXTERNAL NAME sql.createstrimps;
