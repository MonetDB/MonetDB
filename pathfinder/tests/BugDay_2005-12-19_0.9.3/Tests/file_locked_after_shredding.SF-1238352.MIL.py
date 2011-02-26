import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

# create a temporary document
fn = os.path.join(os.getenv('TSTTRGBASE'), 'testdoc.xml')
f = open(fn,'w')
f.write('<testdoc><content/></testdoc>\n')
f.close()
# shred it
p = process.client('mil', stdin = process.PIPE,
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = p.communicate('module("pathfinder");\n'
                         'shred_doc("%s", "testdoc.xml");\n' % fn.replace('\\', r'\\'))
sys.stdout.write(out)
sys.stderr.write(err)
# Here's the real test: try unlinking the source document while
# the server is still running.  If the unlink fails, we get a
# traceback which should be caught by Mtest
os.unlink(fn)
