CREATE TABLE t_boolean (val BOOLEAN, valstr VARCHAR(5));
INSERT INTO t_boolean VALUES (true, 'True');
INSERT INTO t_boolean VALUES (false, 'False');
INSERT INTO t_boolean VALUES (Null, 'Null');

SELECT val, valstr, cast(val as bool) as cast2bool FROM t_boolean order by val;
SELECT val, valstr, cast(valstr as bool) as caststr2bool FROM t_boolean where val is not null order by val;

-- conversions to char strings
SELECT val, valstr, cast(val as string) as cast2str FROM t_boolean order by val;
SELECT val, valstr, cast(val as char(5)) as cast2char5 FROM t_boolean order by val;
SELECT val, valstr, cast(val as varchar(5)) as cast2varchar5 FROM t_boolean order by val;
SELECT val, valstr, cast(val as clob) as cast2clob FROM t_boolean order by val;

SELECT val, valstr, convert(val, string) as convert2str FROM t_boolean order by val;
SELECT val, valstr, convert(val, char(5)) as convert2char5 FROM t_boolean order by val;
SELECT val, valstr, convert(val, varchar(5)) as convert2varchar5 FROM t_boolean order by val;
SELECT val, valstr, convert(val, clob) as convert2clob FROM t_boolean order by val;

-- conversions to integer number 0 or 1
SELECT val, valstr, cast(val as int) as cast2int FROM t_boolean order by val;
SELECT val, valstr, cast(val as smallint) as cast2smallint FROM t_boolean order by val;
SELECT val, valstr, cast(val as tinyint) as cast2tinyint FROM t_boolean order by val;
SELECT val, valstr, cast(val as bigint) as cast2bigint FROM t_boolean order by val;
-- SELECT val, valstr, cast(val as hugeint) as cast2hugeint FROM t_boolean order by val;

-- conversions to char(1) resulting in '0' or '1'
SELECT val, valstr, cast(cast(val as tinyint) as char(1)) as cast2char1 FROM t_boolean order by val;

-- next casting should fail as they are not supported
SELECT val, valstr, cast(val as real) as cast2real FROM t_boolean order by val;
SELECT val, valstr, cast(val as float) as cast2float FROM t_boolean order by val;
SELECT val, valstr, cast(val as double) as cast2double FROM t_boolean order by val;
SELECT val, valstr, cast(val as time) as cast2time FROM t_boolean order by val;
SELECT val, valstr, cast(val as date) as cast2date FROM t_boolean order by val;
SELECT val, valstr, cast(val as timestamp) as cast2timestamp FROM t_boolean order by val;

DROP TABLE t_boolean;

