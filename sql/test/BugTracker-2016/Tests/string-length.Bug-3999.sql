SELECT length('123 ') as "four";
SELECT length('123       ') as "ten";
SELECT length('1234567890') as "ten";
SELECT length('     67890') as "ten";
SELECT length(reverse('     67890')) as "ten";

SELECT sys.length('123 ') as "four";
SELECT sys.length('123       ') as "ten";
SELECT sys.length('1234567890') as "ten";
SELECT sys.length('     67890') as "ten";
SELECT sys.length(reverse('     67890')) as "ten";

-- test trailing spaces with VarChar
CREATE TABLE tvarchar (val VARCHAR(9) NOT NULL);
INSERT INTO tvarchar VALUES ('A'), (' BC ');
SELECT val, length(val) FROM tvarchar;
-- returned wrong length for second row

UPDATE tvarchar SET val = val || '    ';
SELECT val, length(val) FROM tvarchar;
-- returned wrong length for both rows

UPDATE tvarchar SET val = (val || 'x');
SELECT val, length(val) FROM tvarchar;

DROP TABLE tvarchar;


-- test trailing spaces with Char
CREATE TABLE tchar (val CHAR(9) NOT NULL);
INSERT INTO tchar VALUES ('A'), (' BC ');
SELECT val, length(val) FROM tchar;
-- returned wrong length for second row

UPDATE tchar SET val = val || '    ';
SELECT val, length(val) FROM tchar;
-- returned wrong length for both rows

UPDATE tchar SET val = (val || 'x');
SELECT val, length(val) FROM tchar;

DROP TABLE tchar;

