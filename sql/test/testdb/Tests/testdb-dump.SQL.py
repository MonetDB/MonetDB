import sys
import re
try:
    from MonetDBtesting import process
except ImportError:
    import process

sys.stdout.flush()              # just to be sure
p = process.client('sqldump')
p.communicate()
