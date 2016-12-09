-- test SQL functions: convert(fromType, toType) and cast(fromType as toType) for all SQL data types and data values
-- See also https://www.monetdb.org/bugzilla/show_bug.cgi?id=3460

-- HUGEINT  (for int128 only)
CREATE TABLE T_hugeint (v hugeint);
INSERT into T_hugeint VALUES (1), (0), (-1), (-127), (127), (-32767), (32767), (-2147483647), (2147483647);
INSERT into T_hugeint VALUES (null);
SELECT v FROM T_hugeint ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_hugeint;
SELECT v, convert(v, bit) from T_hugeint; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_hugeint where v between -127 and 127;
SELECT v, convert(v, smallint) from T_hugeint where v between -32767 and 32767;
SELECT v, convert(v, integer) from T_hugeint;
SELECT v, convert(v, bigint) from T_hugeint;
SELECT v, convert(v, hugeint) from T_hugeint;

SELECT v, convert(v, float) from T_hugeint;
SELECT v, convert(v, float(24)) from T_hugeint;
SELECT v, convert(v, real) from T_hugeint;
SELECT v, convert(v, double) from T_hugeint;
SELECT v, convert(v, double precision) from T_hugeint;

SELECT v, convert(v, numeric) from T_hugeint;
SELECT v, convert(v, decimal) from T_hugeint;
SELECT v, convert(v, numeric(10)) from T_hugeint;
SELECT v, convert(v, decimal(11)) from T_hugeint;
SELECT v, convert(v, numeric(12,0)) from T_hugeint;
SELECT v, convert(v, decimal(15,3)) from T_hugeint;

SELECT v, convert(v, char) from T_hugeint where v between 0 and 1;
SELECT v, convert(v, varchar) from T_hugeint; -- missing length specification
SELECT v, convert(v, varchar(16)) from T_hugeint;
SELECT v, convert(v, longvarchar) from T_hugeint; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_hugeint; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_hugeint;
SELECT v, convert(v, Clob) from T_hugeint;

SELECT v, convert(v, Binary) from T_hugeint; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_hugeint; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_hugeint; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_hugeint; -- conversion not supported

SELECT v, convert(v, date) from T_hugeint; -- conversion not supported
SELECT v, convert(v, time) from T_hugeint; -- conversion not supported
SELECT v, convert(v, timestamp) from T_hugeint; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_hugeint; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_hugeint; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_hugeint; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_hugeint; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_hugeint;
SELECT v, cast(v as bit) from T_hugeint; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_hugeint where v between -127 and 127;
SELECT v, cast(v as smallint) from T_hugeint where v between -32767 and 32767;
SELECT v, cast(v as integer) from T_hugeint;
SELECT v, cast(v as bigint) from T_hugeint;
SELECT v, cast(v as hugeint) from T_hugeint;

SELECT v, cast(v as float) from T_hugeint;
SELECT v, cast(v as float(24)) from T_hugeint;
SELECT v, cast(v as real) from T_hugeint;
SELECT v, cast(v as double) from T_hugeint;
SELECT v, cast(v as double precision) from T_hugeint;

SELECT v, cast(v as numeric) from T_hugeint;
SELECT v, cast(v as decimal) from T_hugeint;
SELECT v, cast(v as numeric(10)) from T_hugeint;
SELECT v, cast(v as decimal(11)) from T_hugeint;
SELECT v, cast(v as numeric(12,0)) from T_hugeint;
SELECT v, cast(v as decimal(15,3)) from T_hugeint;

SELECT v, cast(v as char) from T_hugeint where v between 0 and 1;
SELECT v, cast(v as varchar) from T_hugeint; -- missing length specification
SELECT v, cast(v as varchar(16)) from T_hugeint;
SELECT v, cast(v as longvarchar) from T_hugeint; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_hugeint; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_hugeint;
SELECT v, cast(v as Clob) from T_hugeint;

SELECT v, cast(v as Binary) from T_hugeint; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_hugeint; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_hugeint; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_hugeint; -- conversion not supported

SELECT v, cast(v as date) from T_hugeint; -- conversion not supported
SELECT v, cast(v as time) from T_hugeint; -- conversion not supported
SELECT v, cast(v as timestamp) from T_hugeint; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_hugeint; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_hugeint; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_hugeint; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_hugeint; -- conversion not supported

DROP TABLE T_hugeint;

