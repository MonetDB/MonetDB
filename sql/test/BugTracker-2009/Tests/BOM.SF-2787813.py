import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process


# This line starts with the BOM followed by SELECT 1;\n
INPUT1 = b"\xEF\xBB\xBF\x53\x45\x4C\x45\x43\x54\x20\x31\x3B\x0A"
with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(INPUT1)
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'Unexpected character (U+FEFF)' not in err:
        sys.stderr.write("Expected stderr to contain 'Unexpected character (U+FEFF)'")
    if "[ 1\\t]" not in str(out):
        sys.stderr.write("The select 1; after the BOM character should have run and returned the result 1")

