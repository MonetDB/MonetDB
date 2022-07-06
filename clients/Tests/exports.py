import sys
import difflib
import MonetDBtesting.listexports
output = MonetDBtesting.listexports.listexports()
stable = open('exports.stable.out').readlines()
try:
    for line in difflib.unified_diff(stable, output):
        sys.stderr.write(line)
except Exception as e:
        sys.stderr.write(str(e))
        raise e

