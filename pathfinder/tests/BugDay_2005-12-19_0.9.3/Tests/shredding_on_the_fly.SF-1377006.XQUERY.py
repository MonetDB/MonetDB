import os

def main():
    fn = os.path.join(os.getenv('TSTSRCDIR'), 'test1377006.xml')
    p = os.popen(os.getenv('XQUERY_CLIENT'), 'w')
    p.write('doc("%s")/x' % fn)
    p.close()

main()
