CREATE OR REPLACE FUNCTION levenshtein(s1 string, s2 string) RETURNS integer EXTERNAL NAME mdb."levenshtein_utf8";

