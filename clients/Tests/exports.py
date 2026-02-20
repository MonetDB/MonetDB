import sys
import difflib
import MonetDBtesting.listexports
with open('exports.stable.out') as fil:
    stable = fil.readlines()
output = MonetDBtesting.listexports.listexports()
for line in difflib.unified_diff(stable, output,
                                 fromfile='expected', tofile='received'):
    sys.stderr.write(line)
