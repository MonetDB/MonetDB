import os, re, sys, tempfile
try:
    from MonetDBtesting import process
except ImportError:
    import process

# Use a Python test because we're testing LF / CR LF handling and we don't
# want editors or version control systems messing with our line endings

def r_escape(s):
    return "r'" + s.replace("'", "''") + "' "

def testdata(prefix,line_sep):
    prefix = "crlf_test_" + prefix + "_"
    lines = [ b"1,one", b"3,two", b"5,three" ]
    text = bytes(line_sep, "ascii").join([*lines, b''])
    f = tempfile.NamedTemporaryFile(delete=False, prefix=prefix, suffix=".csv")
    name = f.name
    f.write(text)
    f.close()
    return name, text

def run_test(name, data_delimiter, copy_delimiter):
    file_name, test_data = testdata(name, data_delimiter)
    script = f"""
    DROP TABLE IF EXISTS foo;
    CREATE TABLE foo(i INT, t TEXT);
        COPY INTO foo FROM {r_escape(file_name)}
        USING DELIMITERS ',', '{copy_delimiter}';
    SELECT i, LENGTH(t) FROM foo;
    """
    with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
        out, err = c.communicate(script)
        reduced = "\n".join(re.sub(r"\s+", "", line) for line in out.splitlines() if line.startswith("["))
        expected = "[3]\n[1,3]\n[3,3]\n[5,5]"
        if reduced != expected:
            print("TEST: ", name, file=sys.stderr)
            print("\nLINE DELIMITER: ", repr(data_delimiter), sep='', file=sys.stderr)
            print("\nFILE CONTENTS: ", repr(test_data), sep='', file=sys.stderr)
            print("\nSCRIPT:\n", script, sep='', file=sys.stderr)
            print("\nEXPECTED:\n", expected, sep='', file=sys.stderr)
            print("\nGOT:\n", reduced, sep='', file=sys.stderr)
            print("\nFULL STDERR:\n", err, sep='', file=sys.stderr)
            print("\nFULL OUTPUT:\n", out, sep='', file=sys.stderr)
            raise SystemExit("Test failed")
    os.remove(file_name)

# Load unix endings while asking for Unix endings.
run_test("unix", "\n", r"\n")

# Load dos endings while asking for dos endings
run_test("dos", "\n", r"\n")

# Load dos endings while asking for unix endings
run_test("dos_as_unix", "\r\n", r"\n")

# Load unix endings while asking for dos endings
run_test("unix_as_dos", "\n", r"\r\n")
