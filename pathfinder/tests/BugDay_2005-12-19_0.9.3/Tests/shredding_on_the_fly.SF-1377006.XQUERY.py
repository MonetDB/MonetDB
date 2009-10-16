import os
from MonetDBtesting import process

def main():
    fn = os.path.join(os.getenv('TSTSRCDIR'), 'test1377006.xml')
    p = process.client('xquery', stdin = process.PIPE)
    p.communicate('doc("%s")/x' % fn)

main()
