import os, sys
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def client(cmd, env=os.environ):
    clt = subprocess.Popen(cmd, env=env, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    dir = os.getenv('TSTSRCDIR')
    clcmd = str(os.getenv('SQL_CLIENT'))
    skyenv = copy.deepcopy(os.environ)
    skyenv['DOTMONETDBFILE'] = '.skyserver'
    f = open(skyenv['DOTMONETDBFILE'], 'w')
    f.write('user=skyserver\npassword=skyserver\n')
    f.close()
    sys.stdout.write('Create User\n')
    client(clcmd + ' "%s"' % os.path.join(dir, '..','create_user.sql'))
    sys.stdout.write('tables\n')
    client(clcmd + ' "%s"' % os.path.join(dir, '..','..','..','sql','math.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','..','..','sql','cache.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','..','..','sql','skyserver.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_tables.sql'))
    clt1 = subprocess.Popen(clcmd, shell = True, universal_newlines = True, stdin = subprocess.PIPE, stdout = subprocess.PIPE)
    sql = open(os.path.join(dir, '..', 'Skyserver_import.sql')).read().replace('DATA',os.path.join(dir,'..','microsky').replace('\\','\\\\'))
    out,err = clt1.communicate(sql)
    sys.stdout.write(out)
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_constraints.sql'))
    sys.stdout.write('views\n')
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_views.sql'))
    sys.stdout.write('functions\n')
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_functions.sql'))
    sys.stdout.write('Cleanup\n')
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_dropFunctions.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_dropMs_functions.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_dropMath.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_dropCache.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_dropViews.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_dropConstraints.sql'))
    client(clcmd + ' "%s"' % os.path.join(dir, '..','Skyserver_dropTables.sql'))
    sys.stdout.write('Remove User\n')
    client(clcmd + ' "%s"' % os.path.join(dir, '..','drop_user.sql'))
    os.unlink(skyenv['DOTMONETDBFILE'])

main()
