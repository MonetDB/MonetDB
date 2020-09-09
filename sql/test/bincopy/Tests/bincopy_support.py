
import array
import codecs
import os
import re
import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

NRECS = 1_000_000

# location generated test data files.
BINCOPY_FILES = os.environ.get('BINCOPY_FILES', None) or os.environ['TSTTRGDIR']

def make_fill_in(side):
    if side.lower() == 'client':
        on_side = 'ON CLIENT'
    elif side.lower() == 'server':
        on_side = 'ON SERVER'
    else:
        raise Exception("'side' should be client or server")

    def fill_in(match):
        var = match.group(1)
        if var == 'ON':
            return on_side
        generator_name = 'gen_' + var
        if generator_name in globals():
            base = f'bincopy_{var}.bin'
            filename = os.path.join(BINCOPY_FILES, base)
            f = open(filename, 'wb')
            gen = globals()[generator_name]
            gen(f)
            return f"R'{filename}'"
        else:
            raise Exception(f'Unknown substitution {match.group(0)}')

    return fill_in

def run_test(side, code):
    code = re.sub(r'@(\w+)@', make_fill_in(side), code)
    code = f"START TRANSACTION;\n{code}\nROLLBACK;"
    open(os.path.join(BINCOPY_FILES, 'test.sql'), "w").write(code)

    with process.client('sql',
                    stdin=process.PIPE, input=code,
                    stdout=process.PIPE, stderr=process.PIPE,
                    log=True) as p:
        out, err = p.communicate()
    sys.stdout.write(out.replace(os.environ['TSTTRGBASE'].replace('\\', '\\\\'),'${TSTTRGBASE}').replace('\\\\','/'))
    sys.stderr.write(err.replace(os.environ['TSTTRGBASE'].replace('\\', '\\\\'),'${TSTTRGBASE}').replace('\\\\','/'))





def gen_ints(outfile):
    data = range(1_000_000)
    arr = array.array('i', data)
    arr.tofile(outfile)

def gen_more_ints(outfile):
    data = [i + 1 for i in range(1_000_000)]
    arr = array.array('i', data)
    arr.tofile(outfile)

def gen_strings(outfile):
    f = codecs.getwriter('utf-8')(outfile)
    for i in range(1_000_000):
        f.write(f"int{i}\0")

def gen_null_ints(outfile):
    nil = -(1<<31)
    data = [(nil if i % 2 == 0 else i) for i in range(1_000_000)]
    arr = array.array('i', data)
    arr.tofile(outfile)

def gen_large_strings(outfile):
    f = codecs.getwriter('utf-8')(outfile)
    for i in range(1_000_000):
        f.write(f"int{i:06d}")
        if i % 10_000 == 0:
            n = 280_000
            f.write("a" * n)
        f.write("\0")

def gen_broken_strings(outfile):
    good = bytes('bröken\0', 'utf-8')
    bad = bytes('bröken\0', 'latin1')
    for i in range(1_000_000):
        if i == 123_456:
            outfile.write(bad)
        else:
            outfile.write(good)

def gen_newline_strings(outfile):
    f = codecs.getwriter('utf-8')(outfile)
    for i in range(1_000_000):
        f.write(f"rn\r\nr\r{i}\0")

def gen_null_strings(outfile):
    for i in range(1_000_000):
        if i % 2 == 0:
            outfile.write(b"\x80\x00")
        else:
            outfile.write(b"banana\0")


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
