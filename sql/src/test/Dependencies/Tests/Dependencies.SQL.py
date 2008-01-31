import os, sys
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDB.subprocess26 as subprocess

def client(cmd):
    clt = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sys.stdout.write(clt.stdout.read())
    clt.stdout.close()
    sys.stderr.write(clt.stderr.read())
    clt.stderr.close()



def main():
    clcmd = str(os.getenv('SQL_CLIENT'))
    clcmd1 = str(os.getenv('SQL_CLIENT')) + " -umonet_test -Ppass_test"

    sys.stdout.write('Dependencies between User and Schema\n')
    sys.stdout.flush()
    client(clcmd + "<%s" % ('%s/../dependency_owner_schema_1.sql' % os.getenv('RELSRCDIR')))
    sys.stdout.write('done\n')

    client(clcmd1 + "<%s" % ('%s/../dependency_owner_schema_2.sql' % os.getenv('RELSRCDIR')))
    sys.stdout.write('done\n')

    sys.stdout.write('Dependencies between database objects\n')
    sys.stdout.flush()
    client(clcmd + "<%s" % ('%s/../dependency_DBobjects.sql' % os.getenv('RELSRCDIR')))
    sys.stdout.write('done\n')

    sys.stdout.write('Dependencies between functions with same name\n')
    sys.stdout.flush()
    client(clcmd + "<%s" % ('%s/../dependency_functions.sql' % os.getenv('RELSRCDIR')))
    sys.stdout.write('done\n')

    sys.stdout.write('Cleanup\n')
    sys.stdout.flush()
    client(clcmd + "<%s" % ('%s/../dependency_owner_schema_3.sql' % os.getenv('RELSRCDIR')))
    sys.stdout.write('done\n')

main()
