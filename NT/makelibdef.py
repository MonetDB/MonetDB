from __future__ import print_function

import fileinput, re
p = re.compile('^\s*\d+\s+[0-9A-F]+\s+[0-9A-F]+\s+(\w+)')
print('LIBRARY R')
print('EXPORTS')
for line in fileinput.input():
    m = p.match(line)
    if m:
        print(m.group(1))
