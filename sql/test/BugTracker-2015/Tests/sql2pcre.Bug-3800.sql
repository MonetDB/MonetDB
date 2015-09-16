SELECT '\\a' LIKE '\\\\a';
SELECT '\\a' LIKE '\\\\\\\\a';
SELECT 'xa' LIKE '_a{1}';
SELECT 'xa$b' LIKE '_a$b';

CREATE FUNCTION sql2pcre(pat TEXT, esc TEXT) RETURNS TEXT EXTERNAL NAME pcre.sql2pcre;

SELECT sql2pcre('a', '\\');
SELECT sql2pcre('_', '\\');
SELECT sql2pcre('%', '\\');
SELECT sql2pcre('_??', '?');
SELECT sql2pcre('_{', '\\');
SELECT sql2pcre('%^%', '\\');

DROP FUNCTION sql2pcre;
