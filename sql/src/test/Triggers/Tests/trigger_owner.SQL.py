import os, sys
import copy
import subprocess

def client(cmd, infile, env=os.environ):
    clt = subprocess.Popen(cmd, env=env, shell=True, universal_newlines=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = clt.communicate(open(infile).read())
    sys.stdout.write(out)
    sys.stderr.write(err)



def main():
    clcmd = os.getenv('SQL_CLIENT')

    env_monet_test = copy.deepcopy(os.environ)
    env_monet_test['DOTMONETDBFILE'] = '.monet_test'
    f = open(env_monet_test['DOTMONETDBFILE'], 'w')
    f.write('user=user_test\npassword=pass\n')
    f.close()

    relsrcdir = os.getenv('RELSRCDIR')
    sys.stdout.write('trigger owner\n')
    client(clcmd, os.path.join(relsrcdir, '..', 'trigger_owner_create.sql'))
    client(clcmd, os.path.join(relsrcdir, '..', 'trigger_owner.sql'), env_monet_test)
    client(clcmd, os.path.join(relsrcdir, '..', 'trigger_owner_drop.sql'))
    sys.stdout.write('done\n')

    os.unlink(env_monet_test['DOTMONETDBFILE'])

main()
