import os, sys
import copy
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def client(cmd, env=os.environ):
    clt = subprocess.Popen(cmd, env=env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sys.stdout.write(clt.stdout.read())
    clt.stdout.close()
    sys.stderr.write(clt.stderr.read())
    clt.stderr.close()



def main():
    clcmd = str(os.getenv('SQL_CLIENT'))

    env_monet_test = copy.deepcopy(os.environ)
    env_monet_test['DOTMONETDBFILE'] = '.monet_test'
    f = open(env_monet_test['DOTMONETDBFILE'], 'wb')
    f.write('user=monet_test\npassword=pass_test\n')
    f.close()

    sys.stdout.write('Dependencies between User and Schema\n')
    sys.stdout.flush()
    client(clcmd + "<%s" % ('%s/../dependency_owner_schema_1.sql' % os.getenv('RELSRCDIR')))
    sys.stdout.write('done\n')

    client(clcmd + "<%s" % ('%s/../dependency_owner_schema_2.sql' % os.getenv('RELSRCDIR')), env_monet_test)
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

    os.unlink(env_monet_test['DOTMONETDBFILE'])

main()
