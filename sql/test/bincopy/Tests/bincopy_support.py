
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
        self.work_list = []

    def substitute_match(self, match):
        var = match.group(1)
        if var == 'ON':
            return self.side_clause
        base = f'bincopy_{var}.bin'
        dst_filename = os.path.join(BINCOPY_FILES, base)
        tmp_filename = os.path.join(BINCOPY_FILES, 'tmp_' + base)
        if not os.path.isfile(dst_filename):
            cmd = ["bincopydata", var, str(NRECS), tmp_filename]
            self.work_list.append( (cmd, tmp_filename, dst_filename))
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
