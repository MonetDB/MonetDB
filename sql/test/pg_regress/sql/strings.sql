--
-- STRINGS
-- Test various data entry syntaxes.
--

-- SQL92 string continuation syntax
-- E021-03 character string literals
SELECT 'first line'
' - next line'
	' - third line'
	AS "Three lines to one";

-- illegal string continuation syntax
SELECT 'first line'
' - next line' /* this comment is not allowed here */
' - third line'
	AS "Illegal comment within continuation";

--
-- test conversions between various string types
-- E021-10 implicit casting among the character data types
--

SELECT CAST(f1 AS text) AS "text(char)" FROM CHAR_TBL;

SELECT CAST(f1 AS text) AS "text(varchar)" FROM VARCHAR_TBL;

SELECT CAST(name 'namefield' AS text) AS "text(name)";

-- since this is an explicit cast, it should truncate w/o error:
SELECT CAST(f1 AS char(10)) AS "char(text)" FROM TEXT_TBL;
-- note: implicit-cast case is tested in char.sql

SELECT CAST(f1 AS char(20)) AS "char(text)" FROM TEXT_TBL;

SELECT CAST(f1 AS char(10)) AS "char(varchar)" FROM VARCHAR_TBL;

SELECT CAST(name 'namefield' AS char(10)) AS "char(name)";

SELECT CAST(f1 AS varchar) AS "varchar(text)" FROM TEXT_TBL;

SELECT CAST(f1 AS varchar) AS "varchar(char)" FROM CHAR_TBL;

SELECT CAST(name 'namefield' AS varchar) AS "varchar(name)";

--
-- test SQL92 string functions
-- E### and T### are feature reference numbers from SQL99
--

-- E021-09 trim function
SELECT TRIM(BOTH FROM '  bunch o blanks  ') = 'bunch o blanks' AS "bunch o blanks";

SELECT TRIM(LEADING FROM '  bunch o blanks  ') = 'bunch o blanks  ' AS "bunch o blanks  ";

SELECT TRIM(TRAILING FROM '  bunch o blanks  ') = '  bunch o blanks' AS "  bunch o blanks";

SELECT TRIM(BOTH 'x' FROM 'xxxxxsome Xsxxxxx') = 'some Xs' AS "some Xs";

-- E021-06 substring expression
SELECT SUBSTRING('1234567890' FROM 3) = '34567890' AS "34567890";

SELECT SUBSTRING('1234567890' FROM 4 FOR 3) = '456' AS "456";

-- T581 regular expression substring (with SQL99's bizarre regexp syntax)
SELECT SUBSTRING('abcdefg' FROM 'a#"(b_d)#"%' FOR '#') AS "bcd";

-- No match should return NULL
SELECT SUBSTRING('abcdefg' FROM '#"(b_d)#"%' FOR '#') IS NULL AS "True";

-- Null inputs should return NULL
SELECT SUBSTRING('abcdefg' FROM '(b|c)' FOR NULL) IS NULL AS "True";
SELECT SUBSTRING(NULL FROM '(b|c)' FOR '#') IS NULL AS "True";
SELECT SUBSTRING('abcdefg' FROM NULL FOR '#') IS NULL AS "True";

-- PostgreSQL extension to allow omitting the escape character;
-- here the regexp is taken as Posix syntax
SELECT SUBSTRING('abcdefg' FROM 'c.e') AS "cde";

-- With a parenthesized subexpression, return only what matches the subexpr
SELECT SUBSTRING('abcdefg' FROM 'b(.*)f') AS "cde";


-- E021-11 position expression
SELECT POSITION('4' IN '1234567890') = '4' AS "4";

SELECT POSITION(5 IN '1234567890') = '5' AS "5";

-- T312 character overlay function
SELECT OVERLAY('abcdef' PLACING '45' FROM 4) AS "abc45f";

SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5) AS "yabadaba";

SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5 FOR 0) AS "yabadabadoo";

SELECT OVERLAY('babosa' PLACING 'ubb' FROM 2 FOR 4) AS "bubba";

--
-- test LIKE
-- Be sure to form every test as a LIKE/NOT LIKE pair.
--

-- simplest examples
-- E061-04 like predicate
SELECT 'hawkeye' LIKE 'h%' AS "true";
SELECT 'hawkeye' NOT LIKE 'h%' AS "false";

SELECT 'hawkeye' LIKE 'H%' AS "false";
SELECT 'hawkeye' NOT LIKE 'H%' AS "true";

SELECT 'hawkeye' LIKE 'indio%' AS "false";
SELECT 'hawkeye' NOT LIKE 'indio%' AS "true";

SELECT 'hawkeye' LIKE 'h%eye' AS "true";
SELECT 'hawkeye' NOT LIKE 'h%eye' AS "false";

SELECT 'indio' LIKE '_ndio' AS "true";
SELECT 'indio' NOT LIKE '_ndio' AS "false";

SELECT 'indio' LIKE 'in__o' AS "true";
SELECT 'indio' NOT LIKE 'in__o' AS "false";

SELECT 'indio' LIKE 'in_o' AS "false";
SELECT 'indio' NOT LIKE 'in_o' AS "true";

-- unused escape character
SELECT 'hawkeye' LIKE 'h%' ESCAPE '#' AS "true";
SELECT 'hawkeye' NOT LIKE 'h%' ESCAPE '#' AS "false";

SELECT 'indio' LIKE 'ind_o' ESCAPE '$' AS "true";
SELECT 'indio' NOT LIKE 'ind_o' ESCAPE '$' AS "false";

-- escape character
-- E061-05 like predicate with escape clause
SELECT 'h%' LIKE 'h#%' ESCAPE '#' AS "true";
SELECT 'h%' NOT LIKE 'h#%' ESCAPE '#' AS "false";

SELECT 'h%wkeye' LIKE 'h#%' ESCAPE '#' AS "false";
SELECT 'h%wkeye' NOT LIKE 'h#%' ESCAPE '#' AS "true";

SELECT 'h%wkeye' LIKE 'h#%%' ESCAPE '#' AS "true";
SELECT 'h%wkeye' NOT LIKE 'h#%%' ESCAPE '#' AS "false";

SELECT 'h%awkeye' LIKE 'h#%a%k%e' ESCAPE '#' AS "true";
SELECT 'h%awkeye' NOT LIKE 'h#%a%k%e' ESCAPE '#' AS "false";

SELECT 'indio' LIKE '_ndio' ESCAPE '$' AS "true";
SELECT 'indio' NOT LIKE '_ndio' ESCAPE '$' AS "false";

SELECT 'i_dio' LIKE 'i$_d_o' ESCAPE '$' AS "true";
SELECT 'i_dio' NOT LIKE 'i$_d_o' ESCAPE '$' AS "false";

SELECT 'i_dio' LIKE 'i$_nd_o' ESCAPE '$' AS "false";
SELECT 'i_dio' NOT LIKE 'i$_nd_o' ESCAPE '$' AS "true";

SELECT 'i_dio' LIKE 'i$_d%o' ESCAPE '$' AS "true";
SELECT 'i_dio' NOT LIKE 'i$_d%o' ESCAPE '$' AS "false";

-- escape character same as pattern character
SELECT 'maca' LIKE 'm%aca' ESCAPE '%' AS "true";
SELECT 'maca' NOT LIKE 'm%aca' ESCAPE '%' AS "false";

SELECT 'ma%a' LIKE 'm%a%%a' ESCAPE '%' AS "true";
SELECT 'ma%a' NOT LIKE 'm%a%%a' ESCAPE '%' AS "false";

SELECT 'bear' LIKE 'b_ear' ESCAPE '_' AS "true";
SELECT 'bear' NOT LIKE 'b_ear' ESCAPE '_' AS "false";

SELECT 'be_r' LIKE 'b_e__r' ESCAPE '_' AS "true";
SELECT 'be_r' NOT LIKE 'b_e__r' ESCAPE '_' AS "false";

SELECT 'be_r' LIKE '__e__r' ESCAPE '_' AS "false";
SELECT 'be_r' NOT LIKE '__e__r' ESCAPE '_' AS "true";


--
-- test ILIKE (case-insensitive LIKE)
-- Be sure to form every test as an ILIKE/NOT ILIKE pair.
--

SELECT 'hawkeye' ILIKE 'h%' AS "true";
SELECT 'hawkeye' NOT ILIKE 'h%' AS "false";

SELECT 'hawkeye' ILIKE 'H%' AS "true";
SELECT 'hawkeye' NOT ILIKE 'H%' AS "false";

SELECT 'hawkeye' ILIKE 'H%Eye' AS "true";
SELECT 'hawkeye' NOT ILIKE 'H%Eye' AS "false";

SELECT 'Hawkeye' ILIKE 'h%' AS "true";
SELECT 'Hawkeye' NOT ILIKE 'h%' AS "false";

--
-- test implicit type conversion
--

-- E021-07 character concatenation
SELECT 'unknown' || ' and unknown' AS "Concat unknown types";

SELECT text 'text' || ' and unknown' AS "Concat text to unknown type";

SELECT char(20) 'characters' || ' and text' AS "Concat char to unknown type";

SELECT text 'text' || char(20) ' and characters' AS "Concat text to char";

SELECT text 'text' || varchar ' and varchar' AS "Concat text to varchar";

--
-- test substr with toasted text values
--
CREATE TABLE toasttest(f1 text);

insert into toasttest values(repeat('1234567890',10000));
insert into toasttest values(repeat('1234567890',10000));

--
-- Ensure that some values are uncompressed, to test the faster substring
-- operation used in that case
--
alter table toasttest alter column f1 set storage external;
insert into toasttest values(repeat('1234567890',10000));
insert into toasttest values(repeat('1234567890',10000));

-- If the starting position is zero or less, then return from the start of the string
-- adjusting the length to be consistent with the "negative start" per SQL92.
SELECT substr(f1, -1, 5) from toasttest;

-- If the length is less than zero, an ERROR is thrown.
SELECT substr(f1, 5, -1) from toasttest;

-- If no third argument (length) is provided, the length to the end of the
-- string is assumed.
SELECT substr(f1, 99995) from toasttest;

-- If start plus length is > string length, the result is truncated to
-- string length
SELECT substr(f1, 99995, 10) from toasttest;

DROP TABLE toasttest;

--
-- test substr with toasted bytea values
--
CREATE TABLE toasttest(f1 bytea);

insert into toasttest values(decode(repeat('1234567890',10000),'escape'));
insert into toasttest values(decode(repeat('1234567890',10000),'escape'));

--
-- Ensure that some values are uncompressed, to test the faster substring
-- operation used in that case
--
alter table toasttest alter column f1 set storage external;
insert into toasttest values(decode(repeat('1234567890',10000),'escape'));
insert into toasttest values(decode(repeat('1234567890',10000),'escape'));

-- If the starting position is zero or less, then return from the start of the string
-- adjusting the length to be consistent with the "negative start" per SQL92.
SELECT substr(f1, -1, 5) from toasttest;

-- If the length is less than zero, an ERROR is thrown.
SELECT substr(f1, 5, -1) from toasttest;

-- If no third argument (length) is provided, the length to the end of the
-- string is assumed.
SELECT substr(f1, 99995) from toasttest;

-- If start plus length is > string length, the result is truncated to
-- string length
SELECT substr(f1, 99995, 10) from toasttest;

DROP TABLE toasttest;

--
-- test length
--

SELECT length('abcdef') AS "length_6";

--
-- test strpos
--

SELECT strpos('abcdef', 'cd') AS "pos_3";

SELECT strpos('abcdef', 'xy') AS "pos_0";

--
-- test replace
--
SELECT replace('abcdef', 'de', '45') AS "abc45f";

SELECT replace('yabadabadoo', 'ba', '123') AS "ya123da123doo";

SELECT replace('yabadoo', 'bad', '') AS "yaoo";

--
-- test split_part
--
select split_part('joeuser@mydatabase','@',0) AS "an error";

select split_part('joeuser@mydatabase','@',1) AS "joeuser";

select split_part('joeuser@mydatabase','@',2) AS "mydatabase";

select split_part('joeuser@mydatabase','@',3) AS "empty string";

select split_part('@joeuser@mydatabase@','@',2) AS "joeuser";

--
-- test to_hex
--
select to_hex(256*256*256 - 1) AS "ffffff";

select to_hex(256::bigint*256::bigint*256::bigint*256::bigint - 1) AS "ffffffff";

--
-- MD5 test suite - from IETF RFC 1321
-- (see: ftp://ftp.rfc-editor.org/in-notes/rfc1321.txt)
--
select md5('') = 'd41d8cd98f00b204e9800998ecf8427e' AS "TRUE";

select md5('a') = '0cc175b9c0f1b6a831c399e269772661' AS "TRUE";

select md5('abc') = '900150983cd24fb0d6963f7d28e17f72' AS "TRUE";

select md5('message digest') = 'f96b697d7cb7938d525a2f31aaf161d0' AS "TRUE";

select md5('abcdefghijklmnopqrstuvwxyz') = 'c3fcd3d76192e4007dfb496cca67e13b' AS "TRUE";

select md5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') = 'd174ab98d277d9f5a5611c2c9f419d9f' AS "TRUE";

select md5('12345678901234567890123456789012345678901234567890123456789012345678901234567890') = '57edf4a22be3c955ac49da2e2107b67a' AS "TRUE";
