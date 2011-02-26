import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

f = os.path.join(os.environ['TSTSRCDIR'], 'update-stress.SF-1896755.xml')
c = process.client(lang = 'xquery', args = ['-s', 'pf:add-doc("%s","update-stress.SF-1896755.xml",10)' % f], stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

f = os.path.join(os.environ['TSTSRCDIR'], 'update-stress.SF-1896755-xq')
c1 = process.client(lang = 'xquery', args = [f],
                    stdout = process.PIPE, stderr = process.PIPE)
c2 = process.client(lang = 'xquery', args = [f],
                    stdout = process.PIPE, stderr = process.PIPE)
c3 = process.client(lang = 'xquery', args = [f],
                    stdout = process.PIPE, stderr = process.PIPE)
out, err = c1.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
out, err = c2.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
out, err = c3.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
