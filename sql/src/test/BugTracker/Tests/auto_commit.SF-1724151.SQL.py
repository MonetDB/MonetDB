import os, sys
from MonetDBtesting import process

c = process.client('sql',
                   args = [os.path.join(os.getenv('TSTSRCDIR'),
                                        '%s.txt' % sys.argv[1])],
                   stdin = process.PIPE)
c.communicate()
