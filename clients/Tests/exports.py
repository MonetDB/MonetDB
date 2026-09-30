import sys
import os
import difflib
import MonetDBtesting.listexports
with open('exports.stable.out') as fil:
    stable = fil.readlines()
output = MonetDBtesting.listexports.listexports()
for line in difflib.unified_diff(stable, output,
                                 fromfile='expected', tofile='received'):
    sys.stderr.write(line)
approve = os.getenv('MTEST_APPROVE')
if approve:
    with open(os.path.join(os.getenv('TSTTRGDIR'),
                           'exports.stable.out.new'), 'w') as fil:
        for line in output:
            fil.write(line)
    if approve == 'REPLACE':
        with open(os.path.join(os.getenv('TSTSRCDIR'),
                               'exports.stable.out'), 'w') as fil:
            for line in output:
                fil.write(line)
