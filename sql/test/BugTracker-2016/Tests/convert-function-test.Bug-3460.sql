-- test SQL functions: convert(fromType, toType) and cast(fromType as toType) for all SQL data types and data values
-- See also https://www.monetdb.org/bugzilla/show_bug.cgi?id=3460

set optimizer = 'sequential_pipe'; -- to get predictable errors

SET TIME ZONE INTERVAL '+02:00' HOUR TO MINUTE;

-- BOOLEAN (true, false)
CREATE TABLE T_boolean (v boolean);
INSERT into T_boolean VALUES (true), (false);
INSERT into T_boolean VALUES (1), (0);
INSERT into T_boolean VALUES (null);
SELECT v FROM T_boolean ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_BOOLEAN;
SELECT v, convert(v, bit) from T_BOOLEAN; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_BOOLEAN;
SELECT v, convert(v, smallint) from T_BOOLEAN;
SELECT v, convert(v, integer) from T_BOOLEAN;
SELECT v, convert(v, bigint) from T_BOOLEAN;
SELECT v, convert(v, hugeint) from T_BOOLEAN;

SELECT v, convert(v, float) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, float(24)) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, real) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, double) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, double precision) from T_BOOLEAN; -- conversion not supported

SELECT v, convert(v, numeric) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, decimal) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, numeric(7)) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, decimal(9)) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, numeric(12,0)) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, decimal(15,3)) from T_BOOLEAN; -- conversion not supported

SELECT v, convert(v, char) from T_BOOLEAN;
SELECT v, convert(v, varchar) from T_BOOLEAN; -- missing length specification
SELECT v, convert(v, varchar(6)) from T_BOOLEAN;
SELECT v, convert(v, longvarchar) from T_BOOLEAN; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_BOOLEAN; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_BOOLEAN;
SELECT v, convert(v, Clob) from T_BOOLEAN;

SELECT v, convert(v, Binary) from T_BOOLEAN; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_BOOLEAN; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_BOOLEAN; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_BOOLEAN; -- conversion not supported

SELECT v, convert(v, date) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, time) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, timestamp) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_BOOLEAN; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_BOOLEAN; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_BOOLEAN; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_BOOLEAN; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_BOOLEAN;
SELECT v, cast(v as bit) from T_BOOLEAN; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_BOOLEAN;
SELECT v, cast(v as smallint) from T_BOOLEAN;
SELECT v, cast(v as integer) from T_BOOLEAN;
SELECT v, cast(v as bigint) from T_BOOLEAN;
SELECT v, cast(v as hugeint) from T_BOOLEAN;

SELECT v, cast(v as float) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as float(24)) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as real) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as double) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as double precision) from T_BOOLEAN; -- conversion not supported

SELECT v, cast(v as numeric) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as decimal) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as numeric(7)) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as decimal(9)) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as numeric(12,0)) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as decimal(15,3)) from T_BOOLEAN; -- conversion not supported

SELECT v, cast(v as char) from T_BOOLEAN;
SELECT v, cast(v as varchar) from T_BOOLEAN;
SELECT v, cast(v as varchar(6)) from T_BOOLEAN;
SELECT v, cast(v as longvarchar) from T_BOOLEAN; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_BOOLEAN; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_BOOLEAN;
SELECT v, cast(v as Clob) from T_BOOLEAN;

SELECT v, cast(v as Binary) from T_BOOLEAN; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_BOOLEAN; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_BOOLEAN; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_BOOLEAN; -- conversion not supported

SELECT v, cast(v as date) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as time) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as timestamp) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_BOOLEAN; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_BOOLEAN; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_BOOLEAN; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_BOOLEAN; -- conversion not supported

-- some JDBC specific types
SELECT v, convert(v, XML) from T_BOOLEAN; -- XML not valid data type
SELECT v, convert(v, REF) from T_BOOLEAN; -- REF not valid data type
SELECT v, convert(v, ROWID) from T_BOOLEAN; -- ROWID not valid data type
SELECT v, convert(v, STRUCT) from T_BOOLEAN; -- STRUCT not valid data type

SELECT v, cast(v as XML) from T_BOOLEAN; -- XML not valid data type
SELECT v, cast(v as REF) from T_BOOLEAN; -- REF not valid data type
SELECT v, cast(v as ROWID) from T_BOOLEAN; -- ROWID not valid data type
SELECT v, cast(v as STRUCT) from T_BOOLEAN; -- STRUCT not valid data type

DROP TABLE T_BOOLEAN;

-- BINARY data type is NOT supported by MonetDB
-- VARBINARY data type is NOT supported by MonetDB
-- LONGVARBINARY data type is NOT supported by MonetDB

-- BLOB
CREATE TABLE T_blob (v BLOB);
INSERT into T_blob VALUES ('00'), ('11'), ('0123456789'), ('A0B2C3D4F5'), ('');
INSERT into T_blob VALUES (null);
SELECT v FROM T_blob ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_blob;
SELECT v, convert(v, bit) from T_blob; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_blob where v between '00' and '11';
SELECT v, convert(v, smallint) from T_blob where v between '00' and '11';
SELECT v, convert(v, integer) from T_blob where v between '00' and '11';
SELECT v, convert(v, bigint) from T_blob where v between '00' and '11';
SELECT v, convert(v, hugeint) from T_blob where v between '00' and '11';

SELECT v, convert(v, float) from T_blob where v between '00' and '11';
SELECT v, convert(v, float(24)) from T_blob where v between '00' and '11';
SELECT v, convert(v, real) from T_blob where v between '00' and '11';
SELECT v, convert(v, double) from T_blob where v between '00' and '11';
SELECT v, convert(v, double precision) from T_blob where v between '00' and '11';

SELECT v, convert(v, numeric) from T_blob where v between '00' and '11';
SELECT v, convert(v, decimal) from T_blob where v between '00' and '11';
SELECT v, convert(v, numeric(10)) from T_blob where v between '00' and '11';
SELECT v, convert(v, decimal(11)) from T_blob where v between '00' and '11';
SELECT v, convert(v, numeric(12,0)) from T_blob where v between '00' and '11';
SELECT v, convert(v, decimal(15,3)) from T_blob where v between '00' and '11';

SELECT v, convert(v, char) from T_blob where v between '11' and '11';
SELECT v, convert(v, varchar) from T_blob; -- missing length specification
SELECT v, convert(v, varchar(16)) from T_blob;
SELECT v, convert(v, longvarchar) from T_blob; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_blob; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_blob;
SELECT v, convert(v, Clob) from T_blob;

SELECT v, convert(v, Binary) from T_blob; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_blob; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_blob; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_blob;

SELECT v, convert(v, date) from T_blob; -- conversion not supported
SELECT v, convert(v, time) from T_blob; -- conversion not supported
SELECT v, convert(v, timestamp) from T_blob; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_blob; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_blob; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_blob; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_blob; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_blob;
SELECT v, cast(v as bit) from T_blob; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_blob where v between '00' and '11';
SELECT v, cast(v as smallint) from T_blob where v between '00' and '11';
SELECT v, cast(v as integer) from T_blob where v between '00' and '11';
SELECT v, cast(v as bigint) from T_blob where v between '00' and '11';
SELECT v, cast(v as hugeint) from T_blob where v between '00' and '11';

SELECT v, cast(v as float) from T_blob where v between '00' and '11';
SELECT v, cast(v as float(24)) from T_blob where v between '00' and '11';
SELECT v, cast(v as real) from T_blob where v between '00' and '11';
SELECT v, cast(v as double) from T_blob where v between '00' and '11';
SELECT v, cast(v as double precision) from T_blob where v between '00' and '11';

SELECT v, cast(v as numeric) from T_blob where v between '00' and '11';
SELECT v, cast(v as decimal) from T_blob where v between '00' and '11';
SELECT v, cast(v as numeric(10)) from T_blob where v between '00' and '11';
SELECT v, cast(v as decimal(11)) from T_blob where v between '00' and '11';
SELECT v, cast(v as numeric(12,0)) from T_blob where v between '00' and '11';
SELECT v, cast(v as decimal(15,3)) from T_blob where v between '00' and '11';

SELECT v, cast(v as char) from T_blob where v between '11' and '11';
SELECT v, cast(v as varchar) from T_blob; -- missing length specification
SELECT v, cast(v as varchar(16)) from T_blob;
SELECT v, cast(v as longvarchar) from T_blob; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_blob; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_blob;
SELECT v, cast(v as Clob) from T_blob;

SELECT v, cast(v as Binary) from T_blob; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_blob; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_blob; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_blob;

SELECT v, cast(v as date) from T_blob; -- conversion not supported
SELECT v, cast(v as time) from T_blob; -- conversion not supported
SELECT v, cast(v as timestamp) from T_blob; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_blob; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_blob; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_blob; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_blob; -- conversion not supported

DROP TABLE T_blob;


-- BIT data type is NOT supported by MonetDB


-- TINYINT, SMALLINT, INTEGER, BIGINT (0, 1, -127, 127, 12345, 1234567890, )
CREATE TABLE T_tinyint (v tinyint);
INSERT into T_tinyint VALUES (1), (0), (-1), (-127), (127);
INSERT into T_tinyint VALUES (null);
SELECT v FROM T_tinyint ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_tinyint;
SELECT v, convert(v, bit) from T_tinyint; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_tinyint;
SELECT v, convert(v, smallint) from T_tinyint;
SELECT v, convert(v, integer) from T_tinyint;
SELECT v, convert(v, bigint) from T_tinyint;
SELECT v, convert(v, hugeint) from T_tinyint;

SELECT v, convert(v, float) from T_tinyint;
SELECT v, convert(v, float(24)) from T_tinyint;
SELECT v, convert(v, real) from T_tinyint;
SELECT v, convert(v, double) from T_tinyint;
SELECT v, convert(v, double precision) from T_tinyint;

SELECT v, convert(v, numeric) from T_tinyint;
SELECT v, convert(v, decimal) from T_tinyint;
SELECT v, convert(v, numeric(7)) from T_tinyint;
SELECT v, convert(v, decimal(9)) from T_tinyint;
SELECT v, convert(v, numeric(12,0)) from T_tinyint;
SELECT v, convert(v, decimal(15,3)) from T_tinyint;

SELECT v, convert(v, char) from T_tinyint where v between 0 and 1;
SELECT v, convert(v, varchar) from T_tinyint; -- missing length specification
SELECT v, convert(v, varchar(6)) from T_tinyint;
SELECT v, convert(v, longvarchar) from T_tinyint; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_tinyint; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_tinyint;
SELECT v, convert(v, Clob) from T_tinyint;

SELECT v, convert(v, Binary) from T_tinyint; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_tinyint; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_tinyint; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_tinyint; -- conversion not supported

SELECT v, convert(v, date) from T_tinyint; -- conversion not supported
SELECT v, convert(v, time) from T_tinyint; -- conversion not supported
SELECT v, convert(v, timestamp) from T_tinyint; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_tinyint; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_tinyint; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_tinyint; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_tinyint; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_tinyint;
SELECT v, cast(v as bit) from T_tinyint; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_tinyint;
SELECT v, cast(v as smallint) from T_tinyint;
SELECT v, cast(v as integer) from T_tinyint;
SELECT v, cast(v as bigint) from T_tinyint;
SELECT v, cast(v as hugeint) from T_tinyint;

SELECT v, cast(v as float) from T_tinyint;
SELECT v, cast(v as float(24)) from T_tinyint;
SELECT v, cast(v as real) from T_tinyint;
SELECT v, cast(v as double) from T_tinyint;
SELECT v, cast(v as double precision) from T_tinyint;

SELECT v, cast(v as numeric) from T_tinyint;
SELECT v, cast(v as decimal) from T_tinyint;
SELECT v, cast(v as numeric(7)) from T_tinyint;
SELECT v, cast(v as decimal(9)) from T_tinyint;
SELECT v, cast(v as numeric(12,0)) from T_tinyint;
SELECT v, cast(v as decimal(15,3)) from T_tinyint;

SELECT v, cast(v as char) from T_tinyint where v between 0 and 1;
SELECT v, cast(v as varchar) from T_tinyint; -- missing length specification
SELECT v, cast(v as varchar(6)) from T_tinyint;
SELECT v, cast(v as longvarchar) from T_tinyint; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_tinyint; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_tinyint;
SELECT v, cast(v as Clob) from T_tinyint;

SELECT v, cast(v as Binary) from T_tinyint; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_tinyint; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_tinyint; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_tinyint; -- conversion not supported

SELECT v, cast(v as date) from T_tinyint; -- conversion not supported
SELECT v, cast(v as time) from T_tinyint; -- conversion not supported
SELECT v, cast(v as timestamp) from T_tinyint; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_tinyint; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_tinyint; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_tinyint; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_tinyint; -- conversion not supported

DROP TABLE T_tinyint;


-- SMALLINT
CREATE TABLE T_smallint (v smallint);
INSERT into T_smallint VALUES (1), (0), (-1), (-127), (127), (-32767), (32767);
INSERT into T_smallint VALUES (null);
SELECT v FROM T_smallint ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_smallint;
SELECT v, convert(v, bit) from T_smallint; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_smallint where v between -127 and 127;
SELECT v, convert(v, smallint) from T_smallint;
SELECT v, convert(v, integer) from T_smallint;
SELECT v, convert(v, bigint) from T_smallint;
SELECT v, convert(v, hugeint) from T_smallint;

SELECT v, convert(v, float) from T_smallint;
SELECT v, convert(v, float(24)) from T_smallint;
SELECT v, convert(v, real) from T_smallint;
SELECT v, convert(v, double) from T_smallint;
SELECT v, convert(v, double precision) from T_smallint;

SELECT v, convert(v, numeric) from T_smallint;
SELECT v, convert(v, decimal) from T_smallint;
SELECT v, convert(v, numeric(7)) from T_smallint;
SELECT v, convert(v, decimal(9)) from T_smallint;
SELECT v, convert(v, numeric(12,0)) from T_smallint;
SELECT v, convert(v, decimal(15,3)) from T_smallint;

SELECT v, convert(v, char) from T_smallint where v between 0 and 1;
SELECT v, convert(v, varchar) from T_smallint; -- missing length specification
SELECT v, convert(v, varchar(6)) from T_smallint;
SELECT v, convert(v, longvarchar) from T_smallint; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_smallint; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_smallint;
SELECT v, convert(v, Clob) from T_smallint;

SELECT v, convert(v, Binary) from T_smallint; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_smallint; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_smallint; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_smallint; -- conversion not supported

SELECT v, convert(v, date) from T_smallint; -- conversion not supported
SELECT v, convert(v, time) from T_smallint; -- conversion not supported
SELECT v, convert(v, timestamp) from T_smallint; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_smallint; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_smallint; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_smallint; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_smallint; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_smallint;
SELECT v, cast(v as bit) from T_smallint; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_smallint where v between -127 and 127;
SELECT v, cast(v as smallint) from T_smallint;
SELECT v, cast(v as integer) from T_smallint;
SELECT v, cast(v as bigint) from T_smallint;
SELECT v, cast(v as hugeint) from T_smallint;

SELECT v, cast(v as float) from T_smallint;
SELECT v, cast(v as float(24)) from T_smallint;
SELECT v, cast(v as real) from T_smallint;
SELECT v, cast(v as double) from T_smallint;
SELECT v, cast(v as double precision) from T_smallint;

SELECT v, cast(v as numeric) from T_smallint;
SELECT v, cast(v as decimal) from T_smallint;
SELECT v, cast(v as numeric(7)) from T_smallint;
SELECT v, cast(v as decimal(9)) from T_smallint;
SELECT v, cast(v as numeric(12,0)) from T_smallint;
SELECT v, cast(v as decimal(15,3)) from T_smallint;

SELECT v, cast(v as char) from T_smallint where v between 0 and 1;
SELECT v, cast(v as varchar) from T_smallint; -- missing length specification
SELECT v, cast(v as varchar(6)) from T_smallint;
SELECT v, cast(v as longvarchar) from T_smallint; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_smallint; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_smallint;
SELECT v, cast(v as Clob) from T_smallint;

SELECT v, cast(v as Binary) from T_smallint; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_smallint; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_smallint; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_smallint; -- conversion not supported

SELECT v, cast(v as date) from T_smallint; -- conversion not supported
SELECT v, cast(v as time) from T_smallint; -- conversion not supported
SELECT v, cast(v as timestamp) from T_smallint; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_smallint; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_smallint; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_smallint; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_smallint; -- conversion not supported

DROP TABLE T_smallint;


-- INTEGER
CREATE TABLE T_int (v int);
INSERT into T_int VALUES (1), (0), (-1), (-127), (127), (-32767), (32767), (-2147483647), (2147483647);
INSERT into T_int VALUES (null);
SELECT v FROM T_int ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_int;
SELECT v, convert(v, bit) from T_int; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_int where v between -127 and 127;
SELECT v, convert(v, smallint) from T_int where v between -32767 and 32767;
SELECT v, convert(v, integer) from T_int;
SELECT v, convert(v, bigint) from T_int;
SELECT v, convert(v, hugeint) from T_int;

SELECT v, convert(v, float) from T_int;
SELECT v, convert(v, float(24)) from T_int;
SELECT v, convert(v, real) from T_int;
SELECT v, convert(v, double) from T_int;
SELECT v, convert(v, double precision) from T_int;

SELECT v, convert(v, numeric) from T_int;
SELECT v, convert(v, decimal) from T_int;
SELECT v, convert(v, numeric(10)) from T_int;
SELECT v, convert(v, decimal(11)) from T_int;
SELECT v, convert(v, numeric(12,0)) from T_int;
SELECT v, convert(v, decimal(15,3)) from T_int;

SELECT v, convert(v, char) from T_int where v between 0 and 1;
SELECT v, convert(v, varchar) from T_int; -- missing length specification
SELECT v, convert(v, varchar(16)) from T_int;
SELECT v, convert(v, longvarchar) from T_int; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_int; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_int;
SELECT v, convert(v, Clob) from T_int;

SELECT v, convert(v, Binary) from T_int; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_int; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_int; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_int; -- conversion not supported

SELECT v, convert(v, date) from T_int; -- conversion not supported
SELECT v, convert(v, time) from T_int; -- conversion not supported
SELECT v, convert(v, timestamp) from T_int; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_int; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_int; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_int; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_int; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_int;
SELECT v, cast(v as bit) from T_int; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_int where v between -127 and 127;
SELECT v, cast(v as smallint) from T_int where v between -32767 and 32767;
SELECT v, cast(v as integer) from T_int;
SELECT v, cast(v as bigint) from T_int;
SELECT v, cast(v as hugeint) from T_int;

SELECT v, cast(v as float) from T_int;
SELECT v, cast(v as float(24)) from T_int;
SELECT v, cast(v as real) from T_int;
SELECT v, cast(v as double) from T_int;
SELECT v, cast(v as double precision) from T_int;

SELECT v, cast(v as numeric) from T_int;
SELECT v, cast(v as decimal) from T_int;
SELECT v, cast(v as numeric(10)) from T_int;
SELECT v, cast(v as decimal(11)) from T_int;
SELECT v, cast(v as numeric(12,0)) from T_int;
SELECT v, cast(v as decimal(15,3)) from T_int;

SELECT v, cast(v as char) from T_int where v between 0 and 1;
SELECT v, cast(v as varchar) from T_int; -- missing length specification
SELECT v, cast(v as varchar(16)) from T_int;
SELECT v, cast(v as longvarchar) from T_int; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_int; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_int;
SELECT v, cast(v as Clob) from T_int;

SELECT v, cast(v as Binary) from T_int; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_int; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_int; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_int; -- conversion not supported

SELECT v, cast(v as date) from T_int; -- conversion not supported
SELECT v, cast(v as time) from T_int; -- conversion not supported
SELECT v, cast(v as timestamp) from T_int; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_int; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_int; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_int; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_int; -- conversion not supported

DROP TABLE T_int;


-- BIGINT
CREATE TABLE T_bigint (v bigint);
INSERT into T_bigint VALUES (1), (0), (-1), (-127), (127), (-32767), (32767), (-2147483647), (2147483647);
INSERT into T_bigint VALUES (null);
SELECT v FROM T_bigint ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_bigint;
SELECT v, convert(v, bit) from T_bigint; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_bigint where v between -127 and 127;
SELECT v, convert(v, smallint) from T_bigint where v between -32767 and 32767;
SELECT v, convert(v, integer) from T_bigint;
SELECT v, convert(v, bigint) from T_bigint;
SELECT v, convert(v, hugeint) from T_bigint;

SELECT v, convert(v, float) from T_bigint;
SELECT v, convert(v, float(24)) from T_bigint;
SELECT v, convert(v, real) from T_bigint;
SELECT v, convert(v, double) from T_bigint;
SELECT v, convert(v, double precision) from T_bigint;

SELECT v, convert(v, numeric) from T_bigint;
SELECT v, convert(v, decimal) from T_bigint;
SELECT v, convert(v, numeric(10)) from T_bigint;
SELECT v, convert(v, decimal(11)) from T_bigint;
SELECT v, convert(v, numeric(12,0)) from T_bigint;
SELECT v, convert(v, decimal(15,3)) from T_bigint;

SELECT v, convert(v, char) from T_bigint where v between 0 and 1;
SELECT v, convert(v, varchar) from T_bigint; -- missing length specification
SELECT v, convert(v, varchar(16)) from T_bigint;
SELECT v, convert(v, longvarchar) from T_bigint; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_bigint; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_bigint;
SELECT v, convert(v, Clob) from T_bigint;

SELECT v, convert(v, Binary) from T_bigint; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_bigint; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_bigint; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_bigint; -- conversion not supported

SELECT v, convert(v, date) from T_bigint; -- conversion not supported
SELECT v, convert(v, time) from T_bigint; -- conversion not supported
SELECT v, convert(v, timestamp) from T_bigint; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_bigint; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_bigint; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_bigint; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_bigint; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_bigint;
SELECT v, cast(v as bit) from T_bigint; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_bigint where v between -127 and 127;
SELECT v, cast(v as smallint) from T_bigint where v between -32767 and 32767;
SELECT v, cast(v as integer) from T_bigint;
SELECT v, cast(v as bigint) from T_bigint;
SELECT v, cast(v as hugeint) from T_bigint;

SELECT v, cast(v as float) from T_bigint;
SELECT v, cast(v as float(24)) from T_bigint;
SELECT v, cast(v as real) from T_bigint;
SELECT v, cast(v as double) from T_bigint;
SELECT v, cast(v as double precision) from T_bigint;

SELECT v, cast(v as numeric) from T_bigint;
SELECT v, cast(v as decimal) from T_bigint;
SELECT v, cast(v as numeric(10)) from T_bigint;
SELECT v, cast(v as decimal(11)) from T_bigint;
SELECT v, cast(v as numeric(12,0)) from T_bigint;
SELECT v, cast(v as decimal(15,3)) from T_bigint;

SELECT v, cast(v as char) from T_bigint where v between 0 and 1;
SELECT v, cast(v as varchar) from T_bigint; -- missing length specification
SELECT v, cast(v as varchar(16)) from T_bigint;
SELECT v, cast(v as longvarchar) from T_bigint; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_bigint; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_bigint;
SELECT v, cast(v as Clob) from T_bigint;

SELECT v, cast(v as Binary) from T_bigint; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_bigint; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_bigint; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_bigint; -- conversion not supported

SELECT v, cast(v as date) from T_bigint; -- conversion not supported
SELECT v, cast(v as time) from T_bigint; -- conversion not supported
SELECT v, cast(v as timestamp) from T_bigint; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_bigint; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_bigint; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_bigint; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_bigint; -- conversion not supported

DROP TABLE T_bigint;


-- HUGEINT (for int128 only), see separate test: convert-function-test-hge.Bug-3460.sql


-- FLOAT
CREATE TABLE T_float (v FLOAT);
INSERT into T_float VALUES (1.0), (0.0), (-1.0), (-127.0), (127.0), (-32767.0), (32767.0), (-2147483647.0), (2147483647.0);
INSERT into T_float VALUES (null);
SELECT v FROM T_float ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_float;
SELECT v, convert(v, bit) from T_float; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_float where v between -127 and 127;
SELECT v, convert(v, smallint) from T_float where v between -32767 and 32767;
SELECT v, convert(v, integer) from T_float;
SELECT v, convert(v, bigint) from T_float;
SELECT v, convert(v, hugeint) from T_float;

SELECT v, convert(v, float) from T_float;
SELECT v, convert(v, float(24)) from T_float;
SELECT v, convert(v, real) from T_float;
SELECT v, convert(v, double) from T_float;
SELECT v, convert(v, double precision) from T_float;

SELECT v, convert(v, numeric) from T_float;
SELECT v, convert(v, decimal) from T_float;
SELECT v, convert(v, numeric(10)) from T_float;
SELECT v, convert(v, decimal(11)) from T_float;
SELECT v, convert(v, numeric(12,0)) from T_float;
SELECT v, convert(v, decimal(15,3)) from T_float;

SELECT v, convert(v, char) from T_float where v between 0 and 1;
SELECT v, convert(v, varchar) from T_float; -- missing length specification
SELECT v, convert(v, varchar(16)) from T_float;
SELECT v, convert(v, longvarchar) from T_float; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_float; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_float;
SELECT v, convert(v, Clob) from T_float;

SELECT v, convert(v, Binary) from T_float; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_float; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_float; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_float; -- conversion not supported

SELECT v, convert(v, date) from T_float; -- conversion not supported
SELECT v, convert(v, time) from T_float; -- conversion not supported
SELECT v, convert(v, timestamp) from T_float; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_float; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_float; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_float; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_float; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_float;
SELECT v, cast(v as bit) from T_float; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_float where v between -127 and 127;
SELECT v, cast(v as smallint) from T_float where v between -32767 and 32767;
SELECT v, cast(v as integer) from T_float;
SELECT v, cast(v as bigint) from T_float;
SELECT v, cast(v as hugeint) from T_float;

SELECT v, cast(v as float) from T_float;
SELECT v, cast(v as float(24)) from T_float;
SELECT v, cast(v as real) from T_float;
SELECT v, cast(v as double) from T_float;
SELECT v, cast(v as double precision) from T_float;

SELECT v, cast(v as numeric) from T_float;
SELECT v, cast(v as decimal) from T_float;
SELECT v, cast(v as numeric(10)) from T_float;
SELECT v, cast(v as decimal(11)) from T_float;
SELECT v, cast(v as numeric(12,0)) from T_float;
SELECT v, cast(v as decimal(15,3)) from T_float;

SELECT v, cast(v as char) from T_float where v between 0 and 1;
SELECT v, cast(v as varchar) from T_float; -- missing length specification
SELECT v, cast(v as varchar(16)) from T_float;
SELECT v, cast(v as longvarchar) from T_float; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_float; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_float;
SELECT v, cast(v as Clob) from T_float;

SELECT v, cast(v as Binary) from T_float; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_float; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_float; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_float; -- conversion not supported

SELECT v, cast(v as date) from T_float; -- conversion not supported
SELECT v, cast(v as time) from T_float; -- conversion not supported
SELECT v, cast(v as timestamp) from T_float; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_float; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_float; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_float; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_float; -- conversion not supported

DROP TABLE T_float;

-- REAL
CREATE TABLE T_real (v REAL);
INSERT into T_real VALUES (1.0), (0.0), (-1.0), (-127.0), (127.0), (-32767.0), (32767.0), (-2147483647.0), (2147483647.0), (0.12), (-3.1415629);
INSERT into T_real VALUES (null);
SELECT v FROM T_real ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_real;
SELECT v, convert(v, bit) from T_real; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_real where v between -127 and 127;
SELECT v, convert(v, smallint) from T_real where v between -32767 and 32767;
SELECT v, convert(v, integer) from T_real;
SELECT v, convert(v, bigint) from T_real;
SELECT v, convert(v, hugeint) from T_real;

SELECT v, convert(v, float) from T_real;
SELECT v, convert(v, float(24)) from T_real;
SELECT v, convert(v, real) from T_real;
SELECT v, convert(v, double) from T_real;
SELECT v, convert(v, double precision) from T_real;

SELECT v, convert(v, numeric) from T_real;
SELECT v, convert(v, decimal) from T_real;
SELECT v, convert(v, numeric(10)) from T_real;
SELECT v, convert(v, decimal(11)) from T_real;
SELECT v, convert(v, numeric(12,0)) from T_real;
SELECT v, convert(v, decimal(15,3)) from T_real;

SELECT v, convert(v, char) from T_real where v between 0 and 1;
SELECT v, convert(v, varchar) from T_real; -- missing length specification
SELECT v, convert(v, varchar(16)) from T_real;
SELECT v, convert(v, longvarchar) from T_real; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_real; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_real;
SELECT v, convert(v, Clob) from T_real;

SELECT v, convert(v, Binary) from T_real; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_real; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_real; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_real; -- conversion not supported

SELECT v, convert(v, date) from T_real; -- conversion not supported
SELECT v, convert(v, time) from T_real; -- conversion not supported
SELECT v, convert(v, timestamp) from T_real; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_real; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_real; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_real; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_real; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_real;
SELECT v, cast(v as bit) from T_real; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_real where v between -127 and 127;
SELECT v, cast(v as smallint) from T_real where v between -32767 and 32767;
SELECT v, cast(v as integer) from T_real;
SELECT v, cast(v as bigint) from T_real;
SELECT v, cast(v as hugeint) from T_real;

SELECT v, cast(v as float) from T_real;
SELECT v, cast(v as float(24)) from T_real;
SELECT v, cast(v as real) from T_real;
SELECT v, cast(v as double) from T_real;
SELECT v, cast(v as double precision) from T_real;

SELECT v, cast(v as numeric) from T_real;
SELECT v, cast(v as decimal) from T_real;
SELECT v, cast(v as numeric(10)) from T_real;
SELECT v, cast(v as decimal(11)) from T_real;
SELECT v, cast(v as numeric(12,0)) from T_real;
SELECT v, cast(v as decimal(15,3)) from T_real;

SELECT v, cast(v as char) from T_real where v between 0 and 1;
SELECT v, cast(v as varchar) from T_real; -- missing length specification
SELECT v, cast(v as varchar(16)) from T_real;
SELECT v, cast(v as longvarchar) from T_real; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_real; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_real;
SELECT v, cast(v as Clob) from T_real;

SELECT v, cast(v as Binary) from T_real; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_real; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_real; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_real; -- conversion not supported

SELECT v, cast(v as date) from T_real; -- conversion not supported
SELECT v, cast(v as time) from T_real; -- conversion not supported
SELECT v, cast(v as timestamp) from T_real; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_real; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_real; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_real; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_real; -- conversion not supported

DROP TABLE T_real;


-- DOUBLE  (1.234E-02)
CREATE TABLE T_double (v DOUBLE);
INSERT into T_double VALUES (1.0), (0.0), (-1.0), (-127.0), (127.0), (-32767.0), (32767.0), (-2147483647.0), (2147483647.0), (0.12), (-3.1415629);
INSERT into T_double VALUES (null);
SELECT v FROM T_double ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_double;
SELECT v, convert(v, bit) from T_double; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_double where v between -127 and 127;
SELECT v, convert(v, smallint) from T_double where v between -32767 and 32767;
SELECT v, convert(v, integer) from T_double;
SELECT v, convert(v, bigint) from T_double;
SELECT v, convert(v, hugeint) from T_double;

SELECT v, convert(v, float) from T_double;
SELECT v, convert(v, float(24)) from T_double;
SELECT v, convert(v, real) from T_double;
SELECT v, convert(v, double) from T_double;
SELECT v, convert(v, double precision) from T_double;

SELECT v, convert(v, numeric) from T_double;
SELECT v, convert(v, decimal) from T_double;
SELECT v, convert(v, numeric(10)) from T_double;
SELECT v, convert(v, decimal(11)) from T_double;
SELECT v, convert(v, numeric(12,0)) from T_double;
SELECT v, convert(v, decimal(15,3)) from T_double;

SELECT v, convert(v, char) from T_double where v between 0 and 1;
SELECT v, convert(v, varchar) from T_double; -- missing length specification
SELECT v, convert(v, varchar(16)) from T_double;
SELECT v, convert(v, longvarchar) from T_double; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_double; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_double;
SELECT v, convert(v, Clob) from T_double;

SELECT v, convert(v, Binary) from T_double; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_double; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_double; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_double; -- conversion not supported

SELECT v, convert(v, date) from T_double; -- conversion not supported
SELECT v, convert(v, time) from T_double; -- conversion not supported
SELECT v, convert(v, timestamp) from T_double; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_double; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_double; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_double; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_double; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_double;
SELECT v, cast(v as bit) from T_double; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_double where v between -127 and 127;
SELECT v, cast(v as smallint) from T_double where v between -32767 and 32767;
SELECT v, cast(v as integer) from T_double;
SELECT v, cast(v as bigint) from T_double;
SELECT v, cast(v as hugeint) from T_double;

SELECT v, cast(v as float) from T_double;
SELECT v, cast(v as float(24)) from T_double;
SELECT v, cast(v as real) from T_double;
SELECT v, cast(v as double) from T_double;
SELECT v, cast(v as double precision) from T_double;

SELECT v, cast(v as numeric) from T_double;
SELECT v, cast(v as decimal) from T_double;
SELECT v, cast(v as numeric(10)) from T_double;
SELECT v, cast(v as decimal(11)) from T_double;
SELECT v, cast(v as numeric(12,0)) from T_double;
SELECT v, cast(v as decimal(15,3)) from T_double;

SELECT v, cast(v as char) from T_double where v between 0 and 1;
SELECT v, cast(v as varchar) from T_double; -- missing length specification
SELECT v, cast(v as varchar(16)) from T_double;
SELECT v, cast(v as longvarchar) from T_double; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_double; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_double;
SELECT v, cast(v as Clob) from T_double;

SELECT v, cast(v as Binary) from T_double; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_double; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_double; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_double; -- conversion not supported

SELECT v, cast(v as date) from T_double; -- conversion not supported
SELECT v, cast(v as time) from T_double; -- conversion not supported
SELECT v, cast(v as timestamp) from T_double; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_double; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_double; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_double; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_double; -- conversion not supported

DROP TABLE T_double;


-- NUMERIC, DECIMAL
CREATE TABLE T_num (v NUMERIC(10,0));
INSERT into T_num VALUES (1.0), (0.0), (-1.0), (-127.0), (127.0), (-32767.0), (32767.0), (-2147483647.0), (2147483647.0), (0.12), (-3.1415629);
INSERT into T_num VALUES (null);
SELECT v FROM T_num ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_num;
SELECT v, convert(v, bit) from T_num; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_num where v between -127 and 127;
SELECT v, convert(v, smallint) from T_num where v between -32767 and 32767;
SELECT v, convert(v, integer) from T_num;
SELECT v, convert(v, bigint) from T_num;
SELECT v, convert(v, hugeint) from T_num;

SELECT v, convert(v, float) from T_num;
SELECT v, convert(v, float(24)) from T_num;
SELECT v, convert(v, real) from T_num;
SELECT v, convert(v, double) from T_num;
SELECT v, convert(v, double precision) from T_num;

SELECT v, convert(v, numeric) from T_num;
SELECT v, convert(v, decimal) from T_num;
SELECT v, convert(v, numeric(10)) from T_num;
SELECT v, convert(v, decimal(11)) from T_num;
SELECT v, convert(v, numeric(12,0)) from T_num;
SELECT v, convert(v, decimal(15,3)) from T_num;

SELECT v, convert(v, char) from T_num where v between 0 and 1;
SELECT v, convert(v, varchar) from T_num; -- missing length specification
SELECT v, convert(v, varchar(16)) from T_num;
SELECT v, convert(v, longvarchar) from T_num; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_num; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_num;
SELECT v, convert(v, Clob) from T_num;

SELECT v, convert(v, Binary) from T_num; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_num; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_num; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_num; -- conversion not supported

SELECT v, convert(v, date) from T_num; -- conversion not supported
SELECT v, convert(v, time) from T_num; -- conversion not supported
SELECT v, convert(v, timestamp) from T_num; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_num; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_num; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_num; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_num; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_num;
SELECT v, cast(v as bit) from T_num; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_num where v between -127 and 127;
SELECT v, cast(v as smallint) from T_num where v between -32767 and 32767;
SELECT v, cast(v as integer) from T_num;
SELECT v, cast(v as bigint) from T_num;
SELECT v, cast(v as hugeint) from T_num;

SELECT v, cast(v as float) from T_num;
SELECT v, cast(v as float(24)) from T_num;
SELECT v, cast(v as real) from T_num;
SELECT v, cast(v as double) from T_num;
SELECT v, cast(v as double precision) from T_num;

SELECT v, cast(v as numeric) from T_num;
SELECT v, cast(v as decimal) from T_num;
SELECT v, cast(v as numeric(10)) from T_num;
SELECT v, cast(v as decimal(11)) from T_num;
SELECT v, cast(v as numeric(12,0)) from T_num;
SELECT v, cast(v as decimal(15,3)) from T_num;

SELECT v, cast(v as char) from T_num where v between 0 and 1;
SELECT v, cast(v as varchar) from T_num; -- missing length specification
SELECT v, cast(v as varchar(16)) from T_num;
SELECT v, cast(v as longvarchar) from T_num; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_num; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_num;
SELECT v, cast(v as Clob) from T_num;

SELECT v, cast(v as Binary) from T_num; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_num; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_num; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_num; -- conversion not supported

SELECT v, cast(v as date) from T_num; -- conversion not supported
SELECT v, cast(v as time) from T_num; -- conversion not supported
SELECT v, cast(v as timestamp) from T_num; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_num; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_num; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_num; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_num; -- conversion not supported

DROP TABLE T_num;

-- DECIMAL  (0.12, -3.1415629)
CREATE TABLE T_dec (v DECIMAL(17,7));
INSERT into T_dec VALUES (1.0), (0.0), (-1.0), (-127.0), (127.0), (-32767.0), (32767.0), (-2147483647.0), (2147483647.0), (0.12), (-3.1415629);
INSERT into T_dec VALUES (null);
SELECT v FROM T_dec ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_dec;
SELECT v, convert(v, bit) from T_dec; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_dec where v between -127 and 127;
SELECT v, convert(v, smallint) from T_dec where v between -32767 and 32767;
SELECT v, convert(v, integer) from T_dec;
SELECT v, convert(v, bigint) from T_dec;
SELECT v, convert(v, hugeint) from T_dec;

SELECT v, convert(v, float) from T_dec;
SELECT v, convert(v, float(24)) from T_dec;
SELECT v, convert(v, real) from T_dec;
SELECT v, convert(v, double) from T_dec;
SELECT v, convert(v, double precision) from T_dec;

SELECT v, convert(v, numeric) from T_dec;
SELECT v, convert(v, decimal) from T_dec;
SELECT v, convert(v, numeric(10)) from T_dec;
SELECT v, convert(v, decimal(11)) from T_dec;
SELECT v, convert(v, numeric(12,0)) from T_dec;
SELECT v, convert(v, decimal(15,3)) from T_dec;

SELECT v, convert(v, char) from T_dec where v between 0 and 1;
SELECT v, convert(v, varchar) from T_dec; -- missing length specification
SELECT v, convert(v, varchar(20)) from T_dec;
SELECT v, convert(v, longvarchar) from T_dec; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_dec; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_dec;
SELECT v, convert(v, Clob) from T_dec;

SELECT v, convert(v, Binary) from T_dec; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_dec; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_dec; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_dec; -- conversion not supported

SELECT v, convert(v, date) from T_dec; -- conversion not supported
SELECT v, convert(v, time) from T_dec; -- conversion not supported
SELECT v, convert(v, timestamp) from T_dec; -- conversion not supported
SELECT v, convert(v, time with timezone) from T_dec; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_dec; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_dec; -- conversion not supported
SELECT v, convert(v, timestamptz) from T_dec; -- conversion not supported

-- test cast()
SELECT v, cast(v as boolean) from T_dec;
SELECT v, cast(v as bit) from T_dec; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_dec where v between -127 and 127;
SELECT v, cast(v as smallint) from T_dec where v between -32767 and 32767;
SELECT v, cast(v as integer) from T_dec;
SELECT v, cast(v as bigint) from T_dec;
SELECT v, cast(v as hugeint) from T_dec;

SELECT v, cast(v as float) from T_dec;
SELECT v, cast(v as float(24)) from T_dec;
SELECT v, cast(v as real) from T_dec;
SELECT v, cast(v as double) from T_dec;
SELECT v, cast(v as double precision) from T_dec;

SELECT v, cast(v as numeric) from T_dec;
SELECT v, cast(v as decimal) from T_dec;
SELECT v, cast(v as numeric(10)) from T_dec;
SELECT v, cast(v as decimal(11)) from T_dec;
SELECT v, cast(v as numeric(12,0)) from T_dec;
SELECT v, cast(v as decimal(15,3)) from T_dec;

SELECT v, cast(v as char) from T_dec where v between 0 and 1;
SELECT v, cast(v as varchar) from T_dec; -- missing length specification
SELECT v, cast(v as varchar(20)) from T_dec;
SELECT v, cast(v as longvarchar) from T_dec; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_dec; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_dec;
SELECT v, cast(v as Clob) from T_dec;

SELECT v, cast(v as Binary) from T_dec; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_dec; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_dec; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_dec; -- conversion not supported

SELECT v, cast(v as date) from T_dec; -- conversion not supported
SELECT v, cast(v as time) from T_dec; -- conversion not supported
SELECT v, cast(v as timestamp) from T_dec; -- conversion not supported
SELECT v, cast(v as time with timezone) from T_dec; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_dec; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_dec; -- conversion not supported
SELECT v, cast(v as timestamptz) from T_dec; -- conversion not supported

DROP TABLE T_dec;


-- CHAR, VARCHAR, LONGVARCHAR, CLOB
CREATE TABLE T_char (v CHAR(33));
INSERT into T_char VALUES ('0'), ('1'), ('0123456789'), ('AaZz'), ('~!@#$%^&*('')\_/-+=:;"<.,.>?'), ('');
INSERT into T_char VALUES (null);
SELECT v FROM T_char ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_char where v in ('0', '1');
SELECT v, convert(v, bit) from T_char where v in ('0', '1'); -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_char where v in ('0', '1');
SELECT v, convert(v, smallint) from T_char where v in ('0', '1');
SELECT v, convert(v, integer) from T_char where v in ('0', '1');
SELECT v, convert(v, bigint) from T_char where v in ('0', '1');
SELECT v, convert(v, hugeint) from T_char where v in ('0', '1');

SELECT v, convert(v, float) from T_char where v in ('0', '1');
SELECT v, convert(v, float(24)) from T_char where v in ('0', '1');
SELECT v, convert(v, real) from T_char where v in ('0', '1');
SELECT v, convert(v, double) from T_char where v in ('0', '1');
SELECT v, convert(v, double precision) from T_char where v in ('0', '1');

SELECT v, convert(v, numeric) from T_char where v in ('0', '1');
SELECT v, convert(v, decimal) from T_char where v in ('0', '1');
SELECT v, convert(v, numeric(10)) from T_char where v in ('0', '1');
SELECT v, convert(v, decimal(11)) from T_char where v in ('0', '1');
SELECT v, convert(v, numeric(12,0)) from T_char where v in ('0', '1');
SELECT v, convert(v, decimal(15,3)) from T_char where v in ('0', '1');

SELECT v, convert(v, char) from T_char where v in ('0', '1');
SELECT v, convert(v, varchar) from T_char; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_char;
SELECT v, convert(v, longvarchar) from T_char; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_char; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_char;
SELECT v, convert(v, Clob) from T_char;

SELECT v, convert(v, Binary) from T_char; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_char; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_char; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_char where v in ('0123456789');

SELECT v, convert(v, date) from T_char;
SELECT v, convert(v, time) from T_char;
SELECT v, convert(v, timestamp) from T_char;
SELECT v, convert(v, time with timezone) from T_char; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_char; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_char;
SELECT v, convert(v, timestamptz) from T_char;

-- test cast()
SELECT v, cast(v as boolean) from T_char where v in ('0', '1');
SELECT v, cast(v as bit) from T_char where v in ('0', '1'); -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_char where v in ('0', '1');
SELECT v, cast(v as smallint) from T_char where v in ('0', '1');
SELECT v, cast(v as integer) from T_char where v in ('0', '1');
SELECT v, cast(v as bigint) from T_char where v in ('0', '1');
SELECT v, cast(v as hugeint) from T_char where v in ('0', '1');

SELECT v, cast(v as float) from T_char where v in ('0', '1');
SELECT v, cast(v as float(24)) from T_char where v in ('0', '1');
SELECT v, cast(v as real) from T_char where v in ('0', '1');
SELECT v, cast(v as double) from T_char where v in ('0', '1');
SELECT v, cast(v as double precision) from T_char where v in ('0', '1');

SELECT v, cast(v as numeric) from T_char where v in ('0', '1');
SELECT v, cast(v as decimal) from T_char where v in ('0', '1');
SELECT v, cast(v as numeric(10)) from T_char where v in ('0', '1');
SELECT v, cast(v as decimal(11)) from T_char where v in ('0', '1');
SELECT v, cast(v as numeric(12,0)) from T_char where v in ('0', '1');
SELECT v, cast(v as decimal(15,3)) from T_char where v in ('0', '1');

SELECT v, cast(v as char) from T_char where v in ('0', '1');
SELECT v, cast(v as varchar) from T_char; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_char;
SELECT v, cast(v as longvarchar) from T_char; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_char; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_char;
SELECT v, cast(v as Clob) from T_char;

SELECT v, cast(v as Binary) from T_char; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_char; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_char; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_char where v in ('0123456789');

SELECT v, cast(v as date) from T_char;
SELECT v, cast(v as time) from T_char;
SELECT v, cast(v as timestamp) from T_char;
SELECT v, cast(v as time with timezone) from T_char; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_char; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_char;
SELECT v, cast(v as timestamptz) from T_char;

DROP TABLE T_char;

-- VARCHAR
CREATE TABLE T_varchar (v VARCHAR(33));
INSERT into T_varchar VALUES ('0'), ('1'), ('0123456789'), ('AaZz'), ('~!@#$%^&*('')\_/-+=:;"<.,.>?'), ('');
INSERT into T_varchar VALUES (null);
SELECT v FROM T_varchar ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_varchar where v in ('0', '1');
SELECT v, convert(v, bit) from T_varchar where v in ('0', '1'); -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_varchar where v in ('0', '1');
SELECT v, convert(v, smallint) from T_varchar where v in ('0', '1');
SELECT v, convert(v, integer) from T_varchar where v in ('0', '1');
SELECT v, convert(v, bigint) from T_varchar where v in ('0', '1');
SELECT v, convert(v, hugeint) from T_varchar where v in ('0', '1');

SELECT v, convert(v, float) from T_varchar where v in ('0', '1');
SELECT v, convert(v, float(24)) from T_varchar where v in ('0', '1');
SELECT v, convert(v, real) from T_varchar where v in ('0', '1');
SELECT v, convert(v, double) from T_varchar where v in ('0', '1');
SELECT v, convert(v, double precision) from T_varchar where v in ('0', '1');

SELECT v, convert(v, numeric) from T_varchar where v in ('0', '1');
SELECT v, convert(v, decimal) from T_varchar where v in ('0', '1');
SELECT v, convert(v, numeric(10)) from T_varchar where v in ('0', '1');
SELECT v, convert(v, decimal(11)) from T_varchar where v in ('0', '1');
SELECT v, convert(v, numeric(12,0)) from T_varchar where v in ('0', '1');
SELECT v, convert(v, decimal(15,3)) from T_varchar where v in ('0', '1');

SELECT v, convert(v, char) from T_varchar where v in ('0', '1');
SELECT v, convert(v, varchar) from T_varchar; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_varchar;
SELECT v, convert(v, longvarchar) from T_varchar; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_varchar; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_varchar;
SELECT v, convert(v, Clob) from T_varchar;

SELECT v, convert(v, Binary) from T_varchar; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_varchar; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_varchar; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_varchar where v in ('0123456789');

SELECT v, convert(v, date) from T_varchar;
SELECT v, convert(v, time) from T_varchar;
SELECT v, convert(v, timestamp) from T_varchar;
SELECT v, convert(v, time with timezone) from T_varchar; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_varchar; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_varchar;
SELECT v, convert(v, timestamptz) from T_varchar;

-- test cast()
SELECT v, cast(v as boolean) from T_varchar where v in ('0', '1');
SELECT v, cast(v as bit) from T_varchar where v in ('0', '1'); -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_varchar where v in ('0', '1');
SELECT v, cast(v as smallint) from T_varchar where v in ('0', '1');
SELECT v, cast(v as integer) from T_varchar where v in ('0', '1');
SELECT v, cast(v as bigint) from T_varchar where v in ('0', '1');
SELECT v, cast(v as hugeint) from T_varchar where v in ('0', '1');

SELECT v, cast(v as float) from T_varchar where v in ('0', '1');
SELECT v, cast(v as float(24)) from T_varchar where v in ('0', '1');
SELECT v, cast(v as real) from T_varchar where v in ('0', '1');
SELECT v, cast(v as double) from T_varchar where v in ('0', '1');
SELECT v, cast(v as double precision) from T_varchar where v in ('0', '1');

SELECT v, cast(v as numeric) from T_varchar where v in ('0', '1');
SELECT v, cast(v as decimal) from T_varchar where v in ('0', '1');
SELECT v, cast(v as numeric(10)) from T_varchar where v in ('0', '1');
SELECT v, cast(v as decimal(11)) from T_varchar where v in ('0', '1');
SELECT v, cast(v as numeric(12,0)) from T_varchar where v in ('0', '1');
SELECT v, cast(v as decimal(15,3)) from T_varchar where v in ('0', '1');

SELECT v, cast(v as char) from T_varchar where v in ('0', '1');
SELECT v, cast(v as varchar) from T_varchar; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_varchar;
SELECT v, cast(v as longvarchar) from T_varchar; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_varchar; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_varchar;
SELECT v, cast(v as Clob) from T_varchar;

SELECT v, cast(v as Binary) from T_varchar; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_varchar; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_varchar; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_varchar where v in ('0123456789');

SELECT v, cast(v as date) from T_varchar;
SELECT v, cast(v as time) from T_varchar;
SELECT v, cast(v as timestamp) from T_varchar;
SELECT v, cast(v as time with timezone) from T_varchar; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_varchar; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_varchar;
SELECT v, cast(v as timestamptz) from T_varchar;

DROP TABLE T_varchar;

-- LONG VARCHAR is NOT (yet) supported
/* disabled test for now, enable it when it is supported
CREATE TABLE T_longvarchar (v LONG VARCHAR);
INSERT into T_longvarchar VALUES ('0'), ('1'), ('0123456789'), ('AaZz'), ('~!@#$%^&*('')\_/-+=:;"<.,.>?'), ('');
INSERT into T_longvarchar VALUES (null);
SELECT v FROM T_longvarchar ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, bit) from T_longvarchar where v in ('0', '1'); -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, smallint) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, integer) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, bigint) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, hugeint) from T_longvarchar where v in ('0', '1');

SELECT v, convert(v, float) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, float(24)) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, real) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, double) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, double precision) from T_longvarchar where v in ('0', '1');

SELECT v, convert(v, numeric) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, decimal) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, numeric(10)) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, decimal(11)) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, numeric(12,0)) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, decimal(15,3)) from T_longvarchar where v in ('0', '1');

SELECT v, convert(v, char) from T_longvarchar where v in ('0', '1');
SELECT v, convert(v, varchar) from T_longvarchar; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_longvarchar;
SELECT v, convert(v, longvarchar) from T_longvarchar; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_longvarchar; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_longvarchar;
SELECT v, convert(v, Clob) from T_longvarchar;

SELECT v, convert(v, Binary) from T_longvarchar; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_longvarchar; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_longvarchar; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_longvarchar where v in ('0123456789');

SELECT v, convert(v, date) from T_longvarchar;
SELECT v, convert(v, time) from T_longvarchar;
SELECT v, convert(v, timestamp) from T_longvarchar;
SELECT v, convert(v, time with timezone) from T_longvarchar; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_longvarchar; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_longvarchar;
SELECT v, convert(v, timestamptz) from T_longvarchar;

-- test cast()
SELECT v, cast(v as boolean) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as bit) from T_longvarchar where v in ('0', '1'); -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as smallint) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as integer) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as bigint) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as hugeint) from T_longvarchar where v in ('0', '1');

SELECT v, cast(v as float) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as float(24)) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as real) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as double) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as double precision) from T_longvarchar where v in ('0', '1');

SELECT v, cast(v as numeric) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as decimal) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as numeric(10)) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as decimal(11)) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as numeric(12,0)) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as decimal(15,3)) from T_longvarchar where v in ('0', '1');

SELECT v, cast(v as char) from T_longvarchar where v in ('0', '1');
SELECT v, cast(v as varchar) from T_longvarchar; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_longvarchar;
SELECT v, cast(v as longvarchar) from T_longvarchar; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_longvarchar; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_longvarchar;
SELECT v, cast(v as Clob) from T_longvarchar;

SELECT v, cast(v as Binary) from T_longvarchar; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_longvarchar; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_longvarchar; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_longvarchar where v in ('0123456789');

SELECT v, cast(v as date) from T_longvarchar;
SELECT v, cast(v as time) from T_longvarchar;
SELECT v, cast(v as timestamp) from T_longvarchar;
SELECT v, cast(v as time with timezone) from T_longvarchar; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_longvarchar; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_longvarchar;
SELECT v, cast(v as timestamptz) from T_longvarchar;

DROP TABLE T_longvarchar;
*/

-- CLOB
CREATE TABLE T_clob (v CLOB);
INSERT into T_clob VALUES ('0'), ('1'), ('0123456789'), ('AaZz'), ('~!@#$%^&*('')\_/-+=:;"<.,.>?'), ('');
INSERT into T_clob VALUES (null);
SELECT v FROM T_clob ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_clob where v in ('0', '1');
SELECT v, convert(v, bit) from T_clob where v in ('0', '1'); -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_clob where v in ('0', '1');
SELECT v, convert(v, smallint) from T_clob where v in ('0', '1');
SELECT v, convert(v, integer) from T_clob where v in ('0', '1');
SELECT v, convert(v, bigint) from T_clob where v in ('0', '1');
SELECT v, convert(v, hugeint) from T_clob where v in ('0', '1');

SELECT v, convert(v, float) from T_clob where v in ('0', '1');
SELECT v, convert(v, float(24)) from T_clob where v in ('0', '1');
SELECT v, convert(v, real) from T_clob where v in ('0', '1');
SELECT v, convert(v, double) from T_clob where v in ('0', '1');
SELECT v, convert(v, double precision) from T_clob where v in ('0', '1');

SELECT v, convert(v, numeric) from T_clob where v in ('0', '1');
SELECT v, convert(v, decimal) from T_clob where v in ('0', '1');
SELECT v, convert(v, numeric(10)) from T_clob where v in ('0', '1');
SELECT v, convert(v, decimal(11)) from T_clob where v in ('0', '1');
SELECT v, convert(v, numeric(12,0)) from T_clob where v in ('0', '1');
SELECT v, convert(v, decimal(15,3)) from T_clob where v in ('0', '1');

SELECT v, convert(v, char) from T_clob where v in ('0', '1');
SELECT v, convert(v, varchar) from T_clob; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_clob;
SELECT v, convert(v, longvarchar) from T_clob; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_clob; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_clob;
SELECT v, convert(v, Clob) from T_clob;

SELECT v, convert(v, Binary) from T_clob; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_clob; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_clob; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_clob where v in ('0123456789');

SELECT v, convert(v, date) from T_clob;
SELECT v, convert(v, time) from T_clob;
SELECT v, convert(v, timestamp) from T_clob;
SELECT v, convert(v, time with timezone) from T_clob; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_clob; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_clob;
SELECT v, convert(v, timestamptz) from T_clob;

-- test cast()
SELECT v, cast(v as boolean) from T_clob where v in ('0', '1');
SELECT v, cast(v as bit) from T_clob where v in ('0', '1'); -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_clob where v in ('0', '1');
SELECT v, cast(v as smallint) from T_clob where v in ('0', '1');
SELECT v, cast(v as integer) from T_clob where v in ('0', '1');
SELECT v, cast(v as bigint) from T_clob where v in ('0', '1');
SELECT v, cast(v as hugeint) from T_clob where v in ('0', '1');

SELECT v, cast(v as float) from T_clob where v in ('0', '1');
SELECT v, cast(v as float(24)) from T_clob where v in ('0', '1');
SELECT v, cast(v as real) from T_clob where v in ('0', '1');
SELECT v, cast(v as double) from T_clob where v in ('0', '1');
SELECT v, cast(v as double precision) from T_clob where v in ('0', '1');

SELECT v, cast(v as numeric) from T_clob where v in ('0', '1');
SELECT v, cast(v as decimal) from T_clob where v in ('0', '1');
SELECT v, cast(v as numeric(10)) from T_clob where v in ('0', '1');
SELECT v, cast(v as decimal(11)) from T_clob where v in ('0', '1');
SELECT v, cast(v as numeric(12,0)) from T_clob where v in ('0', '1');
SELECT v, cast(v as decimal(15,3)) from T_clob where v in ('0', '1');

SELECT v, cast(v as char) from T_clob where v in ('0', '1');
SELECT v, cast(v as varchar) from T_clob; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_clob;
SELECT v, cast(v as longvarchar) from T_clob; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_clob; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_clob;
SELECT v, cast(v as Clob) from T_clob;

SELECT v, cast(v as Binary) from T_clob; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_clob; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_clob; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_clob where v in ('0123456789');

SELECT v, cast(v as date) from T_clob;
SELECT v, cast(v as time) from T_clob;
SELECT v, cast(v as timestamp) from T_clob;
SELECT v, cast(v as time with timezone) from T_clob; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_clob; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_clob;
SELECT v, cast(v as timestamptz) from T_clob;

DROP TABLE T_clob;


-- DATE, TIME, TIMESTAMP (date '2016-12-31', time '23:59:58', timestamp '2016-12-31 23:59:58')
CREATE TABLE T_date (v DATE);
INSERT into T_date VALUES (date '1999-12-31'), (date '2016-01-01'), (date '2016-02-29'), (date '2016-12-31');
INSERT into T_date VALUES (null);
SELECT v FROM T_date ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_date;
SELECT v, convert(v, bit) from T_date; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_date;
SELECT v, convert(v, smallint) from T_date;
SELECT v, convert(v, integer) from T_date;
SELECT v, convert(v, bigint) from T_date;
SELECT v, convert(v, hugeint) from T_date;

SELECT v, convert(v, float) from T_date;
SELECT v, convert(v, float(24)) from T_date;
SELECT v, convert(v, real) from T_date;
SELECT v, convert(v, double) from T_date;
SELECT v, convert(v, double precision) from T_date;

SELECT v, convert(v, numeric) from T_date;
SELECT v, convert(v, decimal) from T_date;
SELECT v, convert(v, numeric(10)) from T_date;
SELECT v, convert(v, decimal(11)) from T_date;
SELECT v, convert(v, numeric(12,0)) from T_date;
SELECT v, convert(v, decimal(15,3)) from T_date;

SELECT v, convert(v, char) from T_date;
SELECT v, convert(v, varchar) from T_date; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_date;
SELECT v, convert(v, longvarchar) from T_date; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_date; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_date;
SELECT v, convert(v, Clob) from T_date;

SELECT v, convert(v, Binary) from T_date; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_date; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_date; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_date where v in (date '2016-01-01');

SELECT v, convert(v, date) from T_date;
SELECT v, convert(v, time) from T_date;
SELECT v, convert(v, timestamp) from T_date;
SELECT v, convert(v, time with timezone) from T_date; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_date; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_date;
SELECT v, convert(v, timestamptz) from T_date;

-- test cast()
SELECT v, cast(v as boolean) from T_date;
SELECT v, cast(v as bit) from T_date; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_date;
SELECT v, cast(v as smallint) from T_date;
SELECT v, cast(v as integer) from T_date;
SELECT v, cast(v as bigint) from T_date;
SELECT v, cast(v as hugeint) from T_date;

SELECT v, cast(v as float) from T_date;
SELECT v, cast(v as float(24)) from T_date;
SELECT v, cast(v as real) from T_date;
SELECT v, cast(v as double) from T_date;
SELECT v, cast(v as double precision) from T_date;

SELECT v, cast(v as numeric) from T_date;
SELECT v, cast(v as decimal) from T_date;
SELECT v, cast(v as numeric(10)) from T_date;
SELECT v, cast(v as decimal(11)) from T_date;
SELECT v, cast(v as numeric(12,0)) from T_date;
SELECT v, cast(v as decimal(15,3)) from T_date;

SELECT v, cast(v as char) from T_date;
SELECT v, cast(v as varchar) from T_date; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_date;
SELECT v, cast(v as longvarchar) from T_date; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_date; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_date;
SELECT v, cast(v as Clob) from T_date;

SELECT v, cast(v as Binary) from T_date; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_date; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_date; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_date where v in (date '2016-01-01');

SELECT v, cast(v as date) from T_date;
SELECT v, cast(v as time) from T_date;
SELECT v, cast(v as timestamp) from T_date;
SELECT v, cast(v as time with timezone) from T_date; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_date; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_date;
SELECT v, cast(v as timestamptz) from T_date;

DROP TABLE T_date;


-- TIME
CREATE TABLE T_time (v TIME);
INSERT into T_time VALUES (time '00:00:00'), (time '23:59:58');
INSERT into T_time VALUES (null);
SELECT v FROM T_time ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_time;
SELECT v, convert(v, bit) from T_time; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_time;
SELECT v, convert(v, smallint) from T_time;
SELECT v, convert(v, integer) from T_time;
SELECT v, convert(v, bigint) from T_time;
SELECT v, convert(v, hugeint) from T_time;

SELECT v, convert(v, float) from T_time;
SELECT v, convert(v, float(24)) from T_time;
SELECT v, convert(v, real) from T_time;
SELECT v, convert(v, double) from T_time;
SELECT v, convert(v, double precision) from T_time;

SELECT v, convert(v, numeric) from T_time;
SELECT v, convert(v, decimal) from T_time;
SELECT v, convert(v, numeric(10)) from T_time;
SELECT v, convert(v, decimal(11)) from T_time;
SELECT v, convert(v, numeric(12,0)) from T_time;
SELECT v, convert(v, decimal(15,3)) from T_time;

SELECT v, convert(v, char) from T_time;
SELECT v, convert(v, varchar) from T_time; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_time;
SELECT v, convert(v, longvarchar) from T_time; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_time; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_time;
SELECT v, convert(v, Clob) from T_time;

SELECT v, convert(v, Binary) from T_time; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_time; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_time; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_time;

SELECT v, convert(v, date) from T_time;
SELECT v, convert(v, time) from T_time;
SELECT v, convert(v, timestamp) from T_time;
SELECT v, convert(v, time with timezone) from T_time; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_time; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_time;
SELECT v, convert(v, timestamptz) from T_time;

-- test cast()
SELECT v, cast(v as boolean) from T_time;
SELECT v, cast(v as bit) from T_time; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_time;
SELECT v, cast(v as smallint) from T_time;
SELECT v, cast(v as integer) from T_time;
SELECT v, cast(v as bigint) from T_time;
SELECT v, cast(v as hugeint) from T_time;

SELECT v, cast(v as float) from T_time;
SELECT v, cast(v as float(24)) from T_time;
SELECT v, cast(v as real) from T_time;
SELECT v, cast(v as double) from T_time;
SELECT v, cast(v as double precision) from T_time;

SELECT v, cast(v as numeric) from T_time;
SELECT v, cast(v as decimal) from T_time;
SELECT v, cast(v as numeric(10)) from T_time;
SELECT v, cast(v as decimal(11)) from T_time;
SELECT v, cast(v as numeric(12,0)) from T_time;
SELECT v, cast(v as decimal(15,3)) from T_time;

SELECT v, cast(v as char) from T_time;
SELECT v, cast(v as varchar) from T_time; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_time;
SELECT v, cast(v as longvarchar) from T_time; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_time; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_time;
SELECT v, cast(v as Clob) from T_time;

SELECT v, cast(v as Binary) from T_time; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_time; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_time; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_time;

SELECT v, cast(v as date) from T_time;
SELECT v, cast(v as time) from T_time;
SELECT v, cast(v as timestamp) from T_time;
SELECT v, cast(v as time with timezone) from T_time; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_time; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_time;
SELECT v, cast(v as timestamptz) from T_time;

DROP TABLE T_time;


-- TIMESTAMP
CREATE TABLE T_timestamp (v TIMESTAMP);
INSERT into T_timestamp VALUES (timestamp '1999-12-31 23:59:59'), (timestamp '2016-01-01 00:00:00'), (timestamp '2016-02-29 00:00:00'), (timestamp '2016-12-31 23:59:58');
INSERT into T_timestamp VALUES (null);
SELECT v FROM T_timestamp ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_timestamp;
SELECT v, convert(v, bit) from T_timestamp; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_timestamp;
SELECT v, convert(v, smallint) from T_timestamp;
SELECT v, convert(v, integer) from T_timestamp;
SELECT v, convert(v, bigint) from T_timestamp;
SELECT v, convert(v, hugeint) from T_timestamp;

SELECT v, convert(v, float) from T_timestamp;
SELECT v, convert(v, float(24)) from T_timestamp;
SELECT v, convert(v, real) from T_timestamp;
SELECT v, convert(v, double) from T_timestamp;
SELECT v, convert(v, double precision) from T_timestamp;

SELECT v, convert(v, numeric) from T_timestamp;
SELECT v, convert(v, decimal) from T_timestamp;
SELECT v, convert(v, numeric(10)) from T_timestamp;
SELECT v, convert(v, decimal(11)) from T_timestamp;
SELECT v, convert(v, numeric(12,0)) from T_timestamp;
SELECT v, convert(v, decimal(15,3)) from T_timestamp;

SELECT v, convert(v, char) from T_timestamp;
SELECT v, convert(v, varchar) from T_timestamp; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_timestamp;
SELECT v, convert(v, longvarchar) from T_timestamp; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_timestamp; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_timestamp;
SELECT v, convert(v, Clob) from T_timestamp;

SELECT v, convert(v, Binary) from T_timestamp; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_timestamp; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_timestamp; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_timestamp;

SELECT v, convert(v, date) from T_timestamp;
SELECT v, convert(v, time) from T_timestamp;
SELECT v, convert(v, timestamp) from T_timestamp;
SELECT v, convert(v, time with timezone) from T_timestamp; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_timestamp; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_timestamp;
SELECT v, convert(v, timestamptz) from T_timestamp;

-- test cast()
SELECT v, cast(v as boolean) from T_timestamp;
SELECT v, cast(v as bit) from T_timestamp; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_timestamp;
SELECT v, cast(v as smallint) from T_timestamp;
SELECT v, cast(v as integer) from T_timestamp;
SELECT v, cast(v as bigint) from T_timestamp;
SELECT v, cast(v as hugeint) from T_timestamp;

SELECT v, cast(v as float) from T_timestamp;
SELECT v, cast(v as float(24)) from T_timestamp;
SELECT v, cast(v as real) from T_timestamp;
SELECT v, cast(v as double) from T_timestamp;
SELECT v, cast(v as double precision) from T_timestamp;

SELECT v, cast(v as numeric) from T_timestamp;
SELECT v, cast(v as decimal) from T_timestamp;
SELECT v, cast(v as numeric(10)) from T_timestamp;
SELECT v, cast(v as decimal(11)) from T_timestamp;
SELECT v, cast(v as numeric(12,0)) from T_timestamp;
SELECT v, cast(v as decimal(15,3)) from T_timestamp;

SELECT v, cast(v as char) from T_timestamp;
SELECT v, cast(v as varchar) from T_timestamp; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_timestamp;
SELECT v, cast(v as longvarchar) from T_timestamp; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_timestamp; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_timestamp;
SELECT v, cast(v as Clob) from T_timestamp;

SELECT v, cast(v as Binary) from T_timestamp; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_timestamp; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_timestamp; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_timestamp;

SELECT v, cast(v as date) from T_timestamp;
SELECT v, cast(v as time) from T_timestamp;
SELECT v, cast(v as timestamp) from T_timestamp;
SELECT v, cast(v as time with timezone) from T_timestamp; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_timestamp; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_timestamp;
SELECT v, cast(v as timestamptz) from T_timestamp;

DROP TABLE T_timestamp;


-- TIME WITH TIMEZONE
CREATE TABLE T_timetz (v TIMETZ);
INSERT into T_timetz VALUES (timetz '00:00:00'), (timetz '23:59:58');
INSERT into T_timetz VALUES (null);
SELECT v FROM T_timetz ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_timetz;
SELECT v, convert(v, bit) from T_timetz; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_timetz;
SELECT v, convert(v, smallint) from T_timetz;
SELECT v, convert(v, integer) from T_timetz;
SELECT v, convert(v, bigint) from T_timetz;
SELECT v, convert(v, hugeint) from T_timetz;

SELECT v, convert(v, float) from T_timetz;
SELECT v, convert(v, float(24)) from T_timetz;
SELECT v, convert(v, real) from T_timetz;
SELECT v, convert(v, double) from T_timetz;
SELECT v, convert(v, double precision) from T_timetz;

SELECT v, convert(v, numeric) from T_timetz;
SELECT v, convert(v, decimal) from T_timetz;
SELECT v, convert(v, numeric(10)) from T_timetz;
SELECT v, convert(v, decimal(11)) from T_timetz;
SELECT v, convert(v, numeric(12,0)) from T_timetz;
SELECT v, convert(v, decimal(15,3)) from T_timetz;

SELECT v, convert(v, char) from T_timetz;
SELECT v, convert(v, varchar) from T_timetz; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_timetz;
SELECT v, convert(v, longvarchar) from T_timetz; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_timetz; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_timetz;
SELECT v, convert(v, Clob) from T_timetz;

SELECT v, convert(v, Binary) from T_timetz; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_timetz; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_timetz; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_timetz;

SELECT v, convert(v, date) from T_timetz;
SELECT v, convert(v, time) from T_timetz;
SELECT v, convert(v, timestamp) from T_timetz;
SELECT v, convert(v, time with timezone) from T_timetz; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_timetz; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_timetz;
SELECT v, convert(v, timestamptz) from T_timetz;

-- test cast()
SELECT v, cast(v as boolean) from T_timetz;
SELECT v, cast(v as bit) from T_timetz; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_timetz;
SELECT v, cast(v as smallint) from T_timetz;
SELECT v, cast(v as integer) from T_timetz;
SELECT v, cast(v as bigint) from T_timetz;
SELECT v, cast(v as hugeint) from T_timetz;

SELECT v, cast(v as float) from T_timetz;
SELECT v, cast(v as float(24)) from T_timetz;
SELECT v, cast(v as real) from T_timetz;
SELECT v, cast(v as double) from T_timetz;
SELECT v, cast(v as double precision) from T_timetz;

SELECT v, cast(v as numeric) from T_timetz;
SELECT v, cast(v as decimal) from T_timetz;
SELECT v, cast(v as numeric(10)) from T_timetz;
SELECT v, cast(v as decimal(11)) from T_timetz;
SELECT v, cast(v as numeric(12,0)) from T_timetz;
SELECT v, cast(v as decimal(15,3)) from T_timetz;

SELECT v, cast(v as char) from T_timetz;
SELECT v, cast(v as varchar) from T_timetz; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_timetz;
SELECT v, cast(v as longvarchar) from T_timetz; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_timetz; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_timetz;
SELECT v, cast(v as Clob) from T_timetz;

SELECT v, cast(v as Binary) from T_timetz; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_timetz; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_timetz; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_timetz;

SELECT v, cast(v as date) from T_timetz;
SELECT v, cast(v as time) from T_timetz;
SELECT v, cast(v as timestamp) from T_timetz;
SELECT v, cast(v as time with timezone) from T_timetz; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_timetz; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_timetz;
SELECT v, cast(v as timestamptz) from T_timetz;

DROP TABLE T_timetz;


-- TIMESTAMP WITH TIMEZONE
CREATE TABLE T_timestamptz (v TIMESTAMPTZ);
INSERT into T_timestamptz VALUES (timestamptz '1999-12-31 23:59:59'), (timestamptz '2016-01-01 00:00:00'), (timestamptz '2016-02-29 00:00:00'), (timestamptz '2016-12-31 23:59:58');
INSERT into T_timestamptz VALUES (null);
SELECT v FROM T_timestamptz ORDER BY v;

-- test convert()
SELECT v, convert(v, boolean) from T_timestamptz;
SELECT v, convert(v, bit) from T_timestamptz; -- BIT not valid data type
SELECT v, convert(v, tinyint) from T_timestamptz;
SELECT v, convert(v, smallint) from T_timestamptz;
SELECT v, convert(v, integer) from T_timestamptz;
SELECT v, convert(v, bigint) from T_timestamptz;
SELECT v, convert(v, hugeint) from T_timestamptz;

SELECT v, convert(v, float) from T_timestamptz;
SELECT v, convert(v, float(24)) from T_timestamptz;
SELECT v, convert(v, real) from T_timestamptz;
SELECT v, convert(v, double) from T_timestamptz;
SELECT v, convert(v, double precision) from T_timestamptz;

SELECT v, convert(v, numeric) from T_timestamptz;
SELECT v, convert(v, decimal) from T_timestamptz;
SELECT v, convert(v, numeric(10)) from T_timestamptz;
SELECT v, convert(v, decimal(11)) from T_timestamptz;
SELECT v, convert(v, numeric(12,0)) from T_timestamptz;
SELECT v, convert(v, decimal(15,3)) from T_timestamptz;

SELECT v, convert(v, char) from T_timestamptz;
SELECT v, convert(v, varchar) from T_timestamptz; -- missing length specification
SELECT v, convert(v, varchar(36)) from T_timestamptz;
SELECT v, convert(v, longvarchar) from T_timestamptz; -- LONGVARCHAR not valid data type
SELECT v, convert(v, long varchar) from T_timestamptz; -- LONG VARCHAR not valid data type
SELECT v, convert(v, CHARACTER LARGE OBJECT) from T_timestamptz;
SELECT v, convert(v, Clob) from T_timestamptz;

SELECT v, convert(v, Binary) from T_timestamptz; -- BINARY not valid data type
SELECT v, convert(v, varBinary) from T_timestamptz; -- VARBINARY not valid data type
SELECT v, convert(v, longvarBinary) from T_timestamptz; -- LONGVARBINARY not valid data type
SELECT v, convert(v, Blob) from T_timestamptz;

SELECT v, convert(v, date) from T_timestamptz;
SELECT v, convert(v, time) from T_timestamptz;
SELECT v, convert(v, timestamp) from T_timestamptz;
SELECT v, convert(v, time with timezone) from T_timestamptz; -- data type not supported (parse error)
SELECT v, convert(v, timestamp with timezone) from T_timestamptz; -- data type not supported (parse error)
SELECT v, convert(v, timetz) from T_timestamptz;
SELECT v, convert(v, timestamptz) from T_timestamptz;

-- test cast()
SELECT v, cast(v as boolean) from T_timestamptz;
SELECT v, cast(v as bit) from T_timestamptz; -- BIT not valid data type
SELECT v, cast(v as tinyint) from T_timestamptz;
SELECT v, cast(v as smallint) from T_timestamptz;
SELECT v, cast(v as integer) from T_timestamptz;
SELECT v, cast(v as bigint) from T_timestamptz;
SELECT v, cast(v as hugeint) from T_timestamptz;

SELECT v, cast(v as float) from T_timestamptz;
SELECT v, cast(v as float(24)) from T_timestamptz;
SELECT v, cast(v as real) from T_timestamptz;
SELECT v, cast(v as double) from T_timestamptz;
SELECT v, cast(v as double precision) from T_timestamptz;

SELECT v, cast(v as numeric) from T_timestamptz;
SELECT v, cast(v as decimal) from T_timestamptz;
SELECT v, cast(v as numeric(10)) from T_timestamptz;
SELECT v, cast(v as decimal(11)) from T_timestamptz;
SELECT v, cast(v as numeric(12,0)) from T_timestamptz;
SELECT v, cast(v as decimal(15,3)) from T_timestamptz;

SELECT v, cast(v as char) from T_timestamptz;
SELECT v, cast(v as varchar) from T_timestamptz; -- missing length specification
SELECT v, cast(v as varchar(36)) from T_timestamptz;
SELECT v, cast(v as longvarchar) from T_timestamptz; -- LONGVARCHAR not valid data type
SELECT v, cast(v as long varchar) from T_timestamptz; -- LONG VARCHAR not valid data type
SELECT v, cast(v as CHARACTER LARGE OBJECT) from T_timestamptz;
SELECT v, cast(v as Clob) from T_timestamptz;

SELECT v, cast(v as Binary) from T_timestamptz; -- BINARY not valid data type
SELECT v, cast(v as varBinary) from T_timestamptz; -- VARBINARY not valid data type
SELECT v, cast(v as longvarBinary) from T_timestamptz; -- LONGVARBINARY not valid data type
SELECT v, cast(v as Blob) from T_timestamptz;

SELECT v, cast(v as date) from T_timestamptz;
SELECT v, cast(v as time) from T_timestamptz;
SELECT v, cast(v as timestamp) from T_timestamptz;
SELECT v, cast(v as time with timezone) from T_timestamptz; -- data type not supported (parse error)
SELECT v, cast(v as timestamp with timezone) from T_timestamptz; -- data type not supported (parse error)
SELECT v, cast(v as timetz) from T_timestamptz;
SELECT v, cast(v as timestamptz) from T_timestamptz;

DROP TABLE T_timestamptz;

