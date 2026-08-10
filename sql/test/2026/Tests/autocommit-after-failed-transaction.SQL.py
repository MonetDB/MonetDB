from os import environ
import re, sys

# import these before importing tpymonetdb as pymonetdb
from pymonetdb import __version__ as pymonetdb_version
from pymonetdb import __file__ as pymonetdb_location

from MonetDBtesting import tpymonetdb as pymonetdb

# Only recent pymonetdb's track autocommit status
print(f'# pymonetdb version: {pymonetdb_version} {pymonetdb_location}', file=sys.stderr)
pymonetdb_version_components = [
    (int(component) if all(ch.isdigit() for ch in component) else component)
    for component in re.findall(r'\d+|[a-zA-Z]+\d*',pymonetdb_version)
]
if pymonetdb_version_components < [1, 9, 1]:
    print(f"# -> old version of pymonetdb which doesn't track autocommit", file=sys.stderr)
    sys.exit(0)


conn = pymonetdb.connect(database=environ['TSTDB'], port=environ['MAPIPORT'], autocommit=True)
cursor = conn.cursor()

def execute(sqlscript, expected_autocommit, expected_exception=None):
    print(f'#\n# Execute: {sqlscript}', file=sys.stderr)
    try:
        cursor.execute(sqlscript)
        if expected_exception is not None:
            print(f"Expected exception containing: {expected_exception}", file=sys.stderr)
            sys.exit(1)
    except pymonetdb.Error as e:
        if expected_exception is not None and expected_exception in str(e):
            print(f"# -> failed as expected: {str(e).strip()}", file=sys.stderr)
        elif isinstance(e, pymonetdb.Error):
            print(f"# -> {str(e).strip()}", file=sys.stderr)
            sys.exit(1)
        else:
            raise
    if conn.autocommit == expected_autocommit:
        print(f"# -> conn.autocommit is {conn.autocommit!r}, as expected", file=sys.stderr)
    else:
        print(f"conn.autocommit is {conn.autocommit!r}, expected {expected_autocommit!r}", file=sys.stderr)
        sys.exit(1)


execute("SELECT 42", True)

# demonstrate happy path
execute("START TRANSACTION", False)
execute("COMMIT", True)

# test auto commit mode is tracked correctly
execute("START TRANSACTION", False)
execute("SELECT SELECT FROM SELECT WHERE SELECT = SELECT", False, expected_exception='syntax error')
execute("COMMIT", True, expected_exception='will ROLLBACK')

