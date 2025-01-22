import sys
import difflib
import MonetDBtesting.listexports
stable = open('exports.stable.out').readlines()
output = MonetDBtesting.listexports.listexports()
for line in difflib.unified_diff(stable, output):
    sys.stderr.write(line)
