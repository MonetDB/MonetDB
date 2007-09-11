import os
import sys

XQUERY_CLIENT = os.environ['XQUERY_CLIENT']
query = os.path.join(os.environ['TSTSRCDIR'],'serialization-query.xq')
query_no_attr = os.path.join(os.environ['TSTSRCDIR'],'serialization-query_no_attr.xq')

for mode in ['xml', 'xml-typed', 'xml-noheader', 'xml-noroot', 'xml-root-name', 'dm', 'seq', 'text']:
    sys.stdout.write('\nmode="%s"\n' % mode)
    sys.stderr.write('\nmode="%s"\n' % mode)
    sys.stdout.flush()
    sys.stderr.flush()
    os.system('%s -f%s %s' % (XQUERY_CLIENT, mode, query))
    sys.stdout.flush()
    sys.stderr.flush()

mode='text'
sys.stdout.write('\nmode="%s" (w/o top-level attribute)\n' % mode)
sys.stderr.write('\nmode="%s" (w/o top-level attribute)\n' % mode)
sys.stdout.flush()
sys.stderr.flush()
os.system('%s -f%s %s' % (XQUERY_CLIENT, mode, query_no_attr))
sys.stdout.flush()
sys.stderr.flush()

sys.stdout.write('\nmode="" (default)\n')
sys.stderr.write('\nmode="" (default)\n')
sys.stdout.flush()
sys.stderr.flush()
os.system('%s %s' % (XQUERY_CLIENT, query))
sys.stdout.flush()
sys.stderr.flush()

