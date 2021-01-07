import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process


# The Unicode BOM (Byte Order Marker) is only a BOM when at the start
# of a file.  Anywhere else it's a ZERO WIDTH NO-BREAK SPACE which we
# shouldn't ignore.

# This line has the BOM character plus the newline
INPUT1 = b"\xEF\xBB\xBF\x0A"
with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(INPUT1)
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'Unexpected character (U+FEFF)' not in err:
        sys.stderr.write("Expected stderr to contain 'Unexpected character (U+FEFF)', instead got '%s'" % (err))

# This line starts with the BOM followed by SELECT 1;\n
INPUT2 = b"\xEF\xBB\xBF\x53\x45\x4C\x45\x43\x54\x20\x31\x3B\x0A"
with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(INPUT2)
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'Unexpected character (U+FEFF)' not in err:
        sys.stderr.write("Expected stderr to contain 'Unexpected character (U+FEFF)', instead got '%s'" % (err))
    if "[ 1\\t]" not in str(out):
        sys.stderr.write("The select 1; after the BOM character should have run and returned the result 1")

# This line has the BOM in the middle of the SELECT 1;\n
INPUT3 = b"\x53\x45\x4C\xEF\xBB\xBF\x45\x43\x54\x20\x31\x3B\x0A"
with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(INPUT3)
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'syntax error' not in err:
        sys.stderr.write("Expected stderr to contain 'syntax error', instead got '%s'" % (err))

# More than one BOM scattered over the entire statement
INPUT4 = b"\x53\x45\xEF\xBB\xBF\x4C\x45\xEF\xBB\xBF\x43\x54\xEF\xBB\xBF\x20\x31\xEF\xBB\xBF\x3B\xEF\xBB\xBF\x0A"
with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(INPUT4)
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'Unexpected character (U+FEFF)' not in err:
        sys.stderr.write("Expected stderr to contain 'Unexpected character (U+FEFF)', instead got '%s'" % (err))

# Using BOM as a SQL identifier
INPUT5 = b"\x53\x45\x4C\x45\x43\x54\x20\x22\xEF\xBB\xBF\x22\x3B\x0A"
with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(INPUT5)
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'Unexpected character (U+FEFF)' not in err:
        sys.stderr.write("Expected stderr to contain 'Unexpected character (U+FEFF)', instead got '%s'" % (err))
