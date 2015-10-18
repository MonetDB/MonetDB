import sys
import re
try:
    from MonetDBtesting import process
except ImportError:
    import process

p = process.client('sqldump', stdout = process.PIPE, stderr = process.PIPE)
out, err = p.communicate()

pos = 0
for res in re.finditer(r'\b\d+\.\d{8,}\b', out):
    sys.stdout.write(out[pos:res.start(0)])
    sys.stdout.write('%.8g' % float(res.group(0)))
    pos = res.end(0)
sys.stdout.write(out[pos:])
sys.stderr.write(err)
