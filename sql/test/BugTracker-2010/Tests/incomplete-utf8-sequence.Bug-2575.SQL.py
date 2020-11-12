import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

# input is a byte string because it contains broken utf-8
INPUT = b"""
CREATE TABLE tbl_bug2575 (
        "documentid" BIGINT        NOT NULL,
        "seq"        SMALLINT      NOT NULL,
        "trigram"    CHAR(3)       NOT NULL
);

copy 2 records into tbl_bug2575 from stdin using delimiters E'\t',E'\n','';
10001160000\t29\t.v.
10001690001\t0\tco\xC3

DROP TABLE tbl_bug2575;
"""

sys.stdout = sys.stderr

retcode = 0
err = out = None
try:

    PIPE = process.PIPE
    with process.client('sql', text=False, stdin=PIPE, stdout=PIPE, stderr=PIPE) as c:
        out, err = c.communicate(INPUT)
        retcode = c.returncode

        if retcode == 0:
            raise SystemExit("Expected nonzero return code")
        if not err or b'input not properly encoded UTF-8' not in err:
            raise SystemExit("Expected stderr to contain 'input not properly encoded UTF-8'")

except BaseException as e:
    print('TEST FAILED, RETURN CODE', retcode)
    print()
    if out:
        print('STDOUT:')
        sys.stderr.flush()
        sys.stderr.buffer.write(out.rstrip())
        print()
        print()
        print()
    if err:
        print('STDERR:')
        sys.stderr.flush()
        sys.stderr.buffer.write(err.rstrip())
        print()
        print()
        print()
    raise e
