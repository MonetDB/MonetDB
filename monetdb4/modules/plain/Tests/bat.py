import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process
s = process.server(lang = 'mil', args = ['--set', 'gdk_mem_pagebits=16'],
                   stdin = open('%s.milS' % os.environ['TST']),
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
