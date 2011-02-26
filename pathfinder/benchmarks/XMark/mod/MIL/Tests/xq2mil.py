import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

TSTSRCDIR = os.environ['TSTSRCDIR']
XQ = os.path.join('XQ','Tests')
MIL = os.path.join('MIL','Tests')

pf = process.pf(args = ['-M', '%s.xq' % os.path.join(TSTSRCDIR.replace(MIL,XQ),
                                                     'xmark')],
                stdout = open('xmark.mil', 'w'),
                stderr = process.PIPE,
                log = True)
out, err = pf.communicate()
sys.stderr.write(err)
