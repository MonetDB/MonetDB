import os
import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

query = os.path.join(os.environ['TSTSRCDIR'],'serialization-query.xq')
query_no_attr = os.path.join(os.environ['TSTSRCDIR'],'serialization-query_no_attr.xq')

for mode in ['xml', 'xml-typed', 'xml-noheader', 'xml-noroot', 'xml-root-name', 'dm', 'seq', 'text']:
    sys.stdout.write('\nmode="%s"\n' % mode)
    sys.stderr.write('\nmode="%s"\n' % mode)
    sys.stdout.flush()
    sys.stderr.flush()
    c = process.client(lang = 'xquery', args = ['-f%s' % mode, query],
                       stdout = process.PIPE, stderr = process.PIPE)
    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
    sys.stdout.flush()
    sys.stderr.flush()

mode='text'
sys.stdout.write('\nmode="%s" (w/o top-level attribute)\n' % mode)
sys.stderr.write('\nmode="%s" (w/o top-level attribute)\n' % mode)
sys.stdout.flush()
sys.stderr.flush()
c = process.client(lang = 'xquery', args = ['-f%s' % mode, query_no_attr],
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
sys.stdout.flush()
sys.stderr.flush()

sys.stdout.write('\nmode="" (default)\n')
sys.stderr.write('\nmode="" (default)\n')
sys.stdout.flush()
sys.stderr.flush()
c = process.client(lang = 'xquery', args = [query],
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
sys.stdout.flush()
sys.stderr.flush()
