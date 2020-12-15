import sys
import difflib
import MonetDBtesting.listexports
output = MonetDBtesting.listexports.listexports()
stable = open('exports.stable.out').readlines()
for line in difflib.unified_diff(stable, output):
    sys.stderr.write(line)
