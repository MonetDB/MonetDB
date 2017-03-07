import sys
import re
try:
    from MonetDBtesting import process
except ImportError:
    import process

p = process.client('sqldump')
p.communicate()

