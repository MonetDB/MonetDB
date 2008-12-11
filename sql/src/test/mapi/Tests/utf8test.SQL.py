import os, sys
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def client(cmd):
    clt = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sys.stdout.write(clt.stdout.read())
    clt.stdout.close()
    sys.stderr.write(clt.stderr.read())
    clt.stderr.close()

def main():
    mclient = os.getenv('SQL_CLIENT')
    client('%s -s "create table utf8test (s varchar(50))"' % mclient)
    client('%s -s "insert into utf8test values (\'value without special characters\')"' % mclient)
    client('%s -s "insert into utf8test values (\'funny characters: \303\240\303\241\303\242\303\243\303\244\303\245\')"' % mclient)
    client('%s -fraw -s "select * from utf8test"' % mclient)
    client('%s -fsql -s "select * from utf8test"' % mclient)
    client('%s -fraw -Eiso-8859-1 -s "select * from utf8test"' % mclient)
    client('%s -fsql -Eiso-8859-1 -s "select * from utf8test"' % mclient)
    client('%s -s "drop table utf8test"' % mclient)

main()
