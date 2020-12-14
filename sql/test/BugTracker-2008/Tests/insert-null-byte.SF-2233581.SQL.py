import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(b"\x00")
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'NULL byte in input on line 1 of input' not in err:
        sys.stderr.write('Expected error: NULL byte in input on line 1 of input')

with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate('drop table strings2233581;')
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or 'DROP TABLE: no such table \'strings2233581\'' not in str(err):
        sys.stderr.write('Expected error: DROP TABLE: no such table \'strings2233581\'')
