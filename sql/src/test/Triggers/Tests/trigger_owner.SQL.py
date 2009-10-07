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
    f = open(env_monet_test['DOTMONETDBFILE'], 'w')
    f.write('user=user_test\npassword=pass\n')
    f.close()

    sys.stdout.write('trigger owner\n')
    client(clcmd + "< %s" % ('%s/../trigger_owner_create.sql' % os.getenv('RELSRCDIR')))
    client(clcmd + "< %s" % ('%s/../trigger_owner.sql' % os.getenv('RELSRCDIR')), env_monet_test)
    client(clcmd + "< %s" % ('%s/../trigger_owner_drop.sql' % os.getenv('RELSRCDIR')))
    sys.stdout.write('done\n')

    os.unlink(env_monet_test['DOTMONETDBFILE'])

main()
