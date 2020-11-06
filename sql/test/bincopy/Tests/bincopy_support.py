
import array
import codecs
import os
import re
import subprocess
import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

NRECS = 1_000_000

# location generated test data files.
BINCOPY_FILES = os.environ.get('BINCOPY_FILES', None) or os.environ['TSTTRGDIR']

class DataMaker:
    def __init__(self, side):
        self.side_clause = 'ON ' + side.upper()
        self.work_list = set()

    def substitute_match(self, match):
        var = match.group(1)
        if var == 'ON':
            return self.side_clause
        base = f'bincopy_{var}.bin'
        dst_filename = os.path.join(BINCOPY_FILES, base)
        tmp_filename = os.path.join(BINCOPY_FILES, 'tmp_' + base)
        if not os.path.isfile(dst_filename):
            cmd = ("bincopydata", var, str(NRECS), tmp_filename)
            self.work_list.add( (cmd, tmp_filename, dst_filename))
        return f"R'{dst_filename}'"

    def generate_files(self):
        processes = []
        # start the generators
        for cmd, tmp, dst in self.work_list:
            proc = subprocess.Popen(cmd)
            processes.append((proc, cmd, tmp, dst))
        # wait for them to complete
        for proc, cmd, tmp, dst in processes:
            returncode = proc.wait()
            if returncode != 0:
                raise Exception(f"Command '{' '.join(cmd)}' exited with status {returncode}")

            os.rename(tmp, dst)



def run_test(side, code):
    # generate the query
    data_maker = DataMaker(side)
    code = re.sub(r'@(\w+)@', data_maker.substitute_match, code)
    code = f"START TRANSACTION;\n{code}\nROLLBACK;"
    open(os.path.join(BINCOPY_FILES, 'test.sql'), "w").write(code)

    # generate the required data files
    data_maker.generate_files()

    with process.client('sql',
                    stdin=process.PIPE, input=code,
                    stdout=process.PIPE, stderr=process.PIPE,
                    log=True) as p:
        out, err = p.communicate()
    sys.stdout.write(out.replace(os.environ['TSTTRGBASE'].replace('\\', '\\\\'),'${TSTTRGBASE}').replace('\\\\','/'))
    sys.stderr.write(err.replace(os.environ['TSTTRGBASE'].replace('\\', '\\\\'),'${TSTTRGBASE}').replace('\\\\','/'))



INTS = """
CREATE TABLE foo(id INT NOT NULL);
COPY BINARY INTO foo(id) FROM @ints@ @ON@;
SELECT COUNT(DISTINCT id) FROM foo;
"""

MORE_INTS = """
CREATE TABLE foo(id INT NOT NULL, i INT);
COPY BINARY INTO foo(id, i) FROM @ints@, @more_ints@ @ON@;
SELECT COUNT(id) FROM foo WHERE i = id + 1;
"""

STRINGS = """
CREATE TABLE foo(id INT NOT NULL, s VARCHAR(20));
COPY BINARY INTO foo(id, s) FROM @ints@, @strings@ @ON@;
SELECT COUNT(id) FROM foo WHERE s = ('int' || id);
"""

NULL_INTS = """
CREATE TABLE foo(id INT NOT NULL, i INT);
COPY BINARY INTO foo(id, i) FROM @ints@, @null_ints@ @ON@;
SELECT COUNT(id) FROM foo
WHERE (id % 2 = 0 AND i IS NULL)
OR    (id % 2 = 1 AND i = id);
"""

LARGE_STRINGS = """
CREATE TABLE foo(id INT NOT NULL, s TEXT);
COPY BINARY INTO foo(id, s) FROM @ints@, @large_strings@ @ON@;
SELECT COUNT(id) FROM foo
WHERE (id % 10000 <> 0 AND LENGTH(s) = 9)
OR    (id % 10000 = 0 AND LENGTH(s) = 280000 + 9);
"""

BROKEN_STRINGS = """
CREATE TABLE foo(id INT NOT NULL, s TEXT);
COPY BINARY INTO foo(id, s) FROM @ints@, @broken_strings@ @ON@;
-- should fail!
"""

# note that the \r\n has been normalized to \n but the lone \r has been
# left alone.
NEWLINE_STRINGS = r"""
CREATE TABLE foo(id INT NOT NULL, s TEXT);
COPY BINARY INTO foo(id, s) FROM @ints@, @newline_strings@ @ON@;
SELECT COUNT(id) FROM foo WHERE s = (E'rn\nr\r' || id);
"""

NULL_STRINGS = """
CREATE TABLE foo(id INT NOT NULL, s TEXT);
COPY BINARY INTO foo(id, s) FROM @ints@, @null_strings@ @ON@;
SELECT COUNT(id) FROM foo
WHERE (id % 2 = 0 AND s IS NULL)
OR    (id % 2 = 1 AND s = 'banana');
"""

TIMESTAMPS = """
CREATE TABLE foo(
    id INT NOT NULL,
    ts TIMESTAMP,
    dt DATE,
    tm TIME,
    "year" SMALLINT,
    "month" TINYINT,
    "day" TINYINT,
    "hour" TINYINT,
    "minute" TINYINT,
    "second" TINYINT,
    ms INTEGER
);
COPY BINARY INTO foo(id, ts, dt, tm, "year", "month", "day", "hour", "minute", "second", ms)
FROM @ints@,
     @timestamps@,
     @timestamp_dates@,
     @timestamp_times@,
     @timestamp_years@,
     @timestamp_months@,
     @timestamp_days@,
     @timestamp_hours@,
     @timestamp_minutes@,
     @timestamp_seconds@,
     @timestamp_ms@
     @ON@;

SELECT * FROM foo
    WHERE EXTRACT(YEAR FROM ts) <> "year"
    LIMIT 4;
SELECT * FROM foo
    WHERE EXTRACT(MONTH FROM ts) <> "month"
    LIMIT 4;
SELECT * FROM foo
    WHERE EXTRACT(DAY FROM ts) <> "day"
    LIMIT 4;
SELECT * FROM foo
    WHERE EXTRACT(HOUR FROM ts) <> "hour"
    LIMIT 4;
SELECT * FROM foo
    WHERE EXTRACT(MINUTE FROM ts) <> "minute"
    LIMIT 4;
SELECT * FROM foo
    WHERE 1000000 * EXTRACT(SECOND FROM ts) <> 1000000 * "second" + ms
    LIMIT 4;

SELECT * FROM foo
    WHERE EXTRACT(YEAR FROM dt) <> "year"
    LIMIT 4;
SELECT * FROM foo
    WHERE EXTRACT(MONTH FROM dt) <> "month"
    LIMIT 4;
SELECT * FROM foo
    WHERE EXTRACT(DAY FROM dt) <> "day"
    LIMIT 4;

SELECT * FROM foo
    WHERE EXTRACT(HOUR FROM tm) <> "hour"
    LIMIT 4;
SELECT * FROM foo
    WHERE EXTRACT(MINUTE FROM tm) <> "minute"
    LIMIT 4;
SELECT * FROM foo
    WHERE 1000000 * EXTRACT(SECOND FROM tm) <> 1000000 * "second" + ms
    LIMIT 4;

"""

PARTIAL = """
CREATE TABLE foo(id INT NOT NULL, i INT, j INT NULL);
COPY BINARY INTO foo(id, i) FROM @ints@, @more_ints@ @ON@;
SELECT COUNT(id) FROM foo WHERE i = id + 1 AND j IS NULL;
"""

BOOLS = """
CREATE TABLE foo(id INT NOT NULL, b BOOL);
COPY BINARY INTO foo(id, b) FROM @ints@, @bools@ @ON@;
SELECT COUNT(id) FROM foo WHERE b = (id % 2 <> 0);
"""

INCONSISTENT_LENGTH = """
CREATE TABLE foo(id INT NOT NULL, i INT);
-- the bools file is much shorter so this will give an error:
COPY BINARY INTO foo(id, i) FROM @ints@, @bools@ @ON@;
SELECT COUNT(id) FROM foo WHERE i = id + 1;
"""

FLOATS = """
CREATE TABLE foo(id INT NOT NULL, r REAL);
COPY BINARY INTO foo(id, r) FROM @ints@, @floats@ @ON@;
SELECT COUNT(id) FROM foo WHERE CAST(id AS REAL) + 0.5 = r;
"""

DOUBLES = """
CREATE TABLE foo(id INT NOT NULL, d DOUBLE);
COPY BINARY INTO foo(id, d) FROM @ints@, @doubles@ @ON@;
SELECT COUNT(id) FROM foo WHERE CAST(id AS REAL) + 0.5 = d;
"""

INTEGER_TYPES = """
CREATE TABLE foo(t TINYINT, s SMALLINT, i INT, b BIGINT);
COPY BINARY INTO foo FROM @tinyints@, @smallints@, @ints@, @bigints@;

WITH
enlarged AS ( -- first go to the largest type
    SELECT
        t, s, i, b,
        CAST(t AS BIGINT) AS tt,
        CAST(s AS BIGINT) AS ss,
        CAST(i AS BIGINT) AS ii,
        b AS bb
    FROM foo
),
denulled AS ( -- 0x80, 0x8000 etc have been interpreted as NULL, fix this
    SELECT
        t, s, i, b,
        COALESCE(tt,        -128) AS tt,
        COALESCE(ss,      -32768) AS ss,
        COALESCE(ii, -2147483648) AS ii,
        bb
    FROM enlarged
),
verified AS (
    SELECT
        t, s, i, b,
        (tt - ss) %        256 = 0 AS t_s,
        (ss - ii) %      65536 = 0 AS s_i,
        (ii - bb) % 2147483648 = 0 AS i_b
    FROM denulled
)
SELECT t_s, s_i, i_b, COUNT(*)
FROM verified
GROUP BY t_s, s_i, i_b
ORDER BY t_s, s_i, i_b
;
"""

HUGE_INTS = """
CREATE TABLE foo(b BIGINT, h HUGEINT);
COPY BINARY INTO foo FROM @bigints@, @hugeints@;

WITH
enlarged AS (
    SELECT
        b, h,
        CAST(b AS HUGEINT) AS bb,
        h AS hh
    FROM foo
),
denulled AS (
    SELECT
        b, h,
        COALESCE(bb, -9223372036854775808) AS bb,
        hh
    FROM enlarged
),
verified AS (
    SELECT
        b, h,
        bb, hh,
        (bb - hh) % 9223372036854775808 = 0 AS b_h
    FROM denulled
)
SELECT b_h, COUNT(*)
FROM verified
GROUP BY b_h
ORDER BY b_h
;
"""

DECIMALS = """
-- 1..2 TINYINT
-- 3..4 SMALLINT
-- 5..9 INT
-- 10..18 BIGINT
-- 18.. HUGEINT
CREATE TABLE foo(
    i1 TINYINT,
    d1_1 DECIMAL(1, 1),
    d2_1 DECIMAL(2, 1),
    i2 SMALLINT,
    d3_2 DECIMAL(3, 2),
    d4_2 DECIMAL(4, 2),
    i4 INT,
    d5_2 DECIMAL(5, 2),
    d9_2 DECIMAL(9, 2),
    i8 BIGINT,
    d10_2 DECIMAL(10, 2),
    d18_2 DECIMAL(18, 2)
);
COPY BINARY INTO foo FROM
    -- bte: i1, d1_1, d2_1
    @tinyints@, @tinyints@, @tinyints@,
    -- sht: i2, d3_2, d4_2
    @smallints@, @smallints@, @smallints@,
    -- int: i4, d5_2, d9_2
    @ints@, @ints@, @ints@,
    -- lng: i8, d10_2, d18_2
    @bigints@, @bigints@, @bigints@
    @ON@;
WITH verified AS (
    SELECT
        (d1_1 IS NULL OR 10 * d1_1 = i1) AS d1_1_ok,
        (d2_1 IS NULL OR 10 * d2_1 = i1) AS d2_1_ok,
        --
        (d3_2 IS NULL OR 100 * d3_2 = i2) AS d3_2_ok,
        (d4_2 IS NULL OR 100 * d4_2 = i2) AS d4_2_ok,
        --
        (d5_2 IS NULL OR 100 * d5_2 = i4) AS d5_2_ok,
        (d9_2 IS NULL OR 100 * d9_2 = i4) AS d9_2_ok,
        --
        (d10_2 IS NULL OR 100 * d10_2 = i8) AS d10_2_ok,
        (d18_2 IS NULL OR 100 * d18_2 = i8) AS d18_2_ok
    FROM foo
)
SELECT
    d1_1_ok, d2_1_ok, d3_2_ok, d4_2_ok, d5_2_ok, d9_2_ok, d10_2_ok, d18_2_ok,
    COUNT(*)
FROM verified
GROUP BY d1_1_ok, d2_1_ok, d3_2_ok, d4_2_ok, d5_2_ok, d9_2_ok, d10_2_ok, d18_2_ok
;
"""
