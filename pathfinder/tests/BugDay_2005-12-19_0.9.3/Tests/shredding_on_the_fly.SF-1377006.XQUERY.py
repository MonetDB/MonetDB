import os
try:
    import subprocess
except ImportError:
    # user private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def main():
    fn = os.path.join(os.getenv('TSTSRCDIR'), 'test1377006.xml')
    p = subprocess.Popen(os.getenv('XQUERY_CLIENT'), shell = True, stdin = subprocess.PIPE)
    p.communicate('doc("%s")/x' % fn)

main()
