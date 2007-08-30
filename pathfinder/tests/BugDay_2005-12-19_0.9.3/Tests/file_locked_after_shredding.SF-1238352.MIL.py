import os

def main():
    # create a temporary document
    fn = os.path.join(os.getenv('TSTTRGBASE'), 'testdoc.xml')
    f = open(fn,'w')
    f.write('<testdoc><content/></testdoc>\n')
    f.close()
    # shred it
    p = os.popen(os.getenv('MIL_CLIENT'), 'w')
    p.write('module("pathfinder");\n')
    p.write('shred_doc("%s", "testdoc.xml");\n' % fn.replace('\\', r'\\'))
    p.close()
    # Here's the real test: try unlinking the source document while
    # the server is still running.  If the unlink fails, we get a
    # traceback which should be caught by Mtest
    os.unlink(fn)

main()
