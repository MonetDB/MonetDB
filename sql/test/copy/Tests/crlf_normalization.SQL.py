import os, sys, tempfile
import pymonetdb

hdl = pymonetdb.connect(database=os.getenv('TSTDB'), port=os.getenv('MAPIPORT'), autocommit=True)
cur = hdl.cursor()

# Use a Python test because we're testing LF / CR LF handling and we don't
# want editors or version control systems messing with our line endings

def r_escape(s):
    return "r'" + s.replace("'", "''") + "'"

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
    cur.execute('DROP TABLE IF EXISTS foo')
    cur.execute('CREATE TABLE foo(i INT, t TEXT)')
    rows = cur.execute(f"COPY INTO foo FROM {r_escape(file_name)} USING DELIMITERS ',', E'{copy_delimiter}'")
    if rows != 3:
        print("TEST: ", name, file-sys.stderr)
        print("\nLINE DELIMITER: ", repr(data_delimiter), sep='', file=sys.stderr)
        print("\nFILE CONTENTS: ", repr(test_data), sep='', file=sys.stderr)
        print("\nEXPTECTED: 3 affected rows", file=sys.stderr)
        print(f"\nGOT: {rows}", file=sys.stderr)
        raise SystemExit("Test failed")
    cur.execute('SELECT i, LENGTH(t) FROM foo')
    reduced = cur.fetchall()
    expected = [(1, 3), (3, 3), (5, 5)]
    if reduced != expected:
        print("TEST: ", name, file=sys.stderr)
        print("\nLINE DELIMITER: ", repr(data_delimiter), sep='', file=sys.stderr)
        print("\nFILE CONTENTS: ", repr(test_data), sep='', file=sys.stderr)
        print("\nEXPECTED:\n", expected, sep='', file=sys.stderr)
        print("\nGOT:\n", reduced, sep='', file=sys.stderr)
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
