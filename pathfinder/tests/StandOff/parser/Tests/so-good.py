import os
from MonetDBtesting import process

c = process.pf(args = ['-b', '-s1',
                       '%s.xq' % os.path.join(os.environ['TSTSRCDIR'],
                                              os.environ['TST'])])
c.communicate()
print c.returncode
