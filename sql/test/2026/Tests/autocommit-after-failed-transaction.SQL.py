from os import environ
import sys
from MonetDBtesting import tpymonetdb as pymonetdb


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

execute("START TRANSACTION", False)
execute("SELECT SELECT FROM SELECT WHERE SELECT = SELECT", False, expected_exception='syntax error')
execute("COMMIT", True, expected_exception='will ROLLBACK')

