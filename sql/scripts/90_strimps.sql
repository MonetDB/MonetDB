create schema strimps;

-- create procedure strimps.strmpcreate(b string)
-- external name bat.strimpCreate;
-- grant execute on procedure strimps.strmpcreate to public;

CREATE FILTER FUNCTION strimps.filter(strs STRING, q STRING) EXTERNAL NAME bat.strimpfilter;
GRANT EXECUTE ON FILTER FUNCTION strimps.filter TO PUBLIC;
