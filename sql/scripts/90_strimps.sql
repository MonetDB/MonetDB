CREATE FILTER FUNCTION sys.strimp_filter(strs STRING, q STRING) EXTERNAL NAME strimps.strimpfilter;
GRANT EXECUTE ON FILTER FUNCTION sys.strimp_filter TO PUBLIC;

CREATE PROCEDURE sys.strimp_create(sch string, tab string, col string)
       EXTERNAL NAME sql.createstrimps;
