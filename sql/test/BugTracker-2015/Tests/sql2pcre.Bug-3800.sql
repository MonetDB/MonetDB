SELECT E'\\a' LIKE E'\\\\a';
SELECT E'\\a' LIKE E'\\\\\\\\a';
SELECT 'xa' LIKE '_a{1}';
SELECT 'xa$b' LIKE '_a$b';

CREATE FUNCTION sql2pcre(pat TEXT, esc TEXT) RETURNS TEXT EXTERNAL NAME pcre.sql2pcre;

SELECT sql2pcre('a', E'\\');
SELECT sql2pcre('_', E'\\');
SELECT sql2pcre('%', E'\\');
SELECT sql2pcre('_??', '?');
SELECT sql2pcre('_{', E'\\');
SELECT sql2pcre('%^%', E'\\');

DROP FUNCTION sql2pcre;
