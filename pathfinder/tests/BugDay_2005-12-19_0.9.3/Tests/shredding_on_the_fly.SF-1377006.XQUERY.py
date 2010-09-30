import os, sys
from MonetDBtesting import process

fn = os.path.join(os.getenv('TSTSRCDIR'), 'test1377006.xml')
p = process.client('xquery', stdin = process.PIPE, stdout = process.PIPE,
                   stderr = process.PIPE)
out, err = p.communicate('doc("%s")/x' % fn)
sys.stdout.write(out)
sys.stderr.write(err)
