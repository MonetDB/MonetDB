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
    dir = os.getenv('TSTSRCDIR')
    clcmd = str(os.getenv('SQL_CLIENT'))
    skyenv = copy.deepcopy(os.environ)
    skyenv['DOTMONETDBFILE'] = '.skyserver'
    f = open(skyenv['DOTMONETDBFILE'], 'w')
    f.write('user=skyserver\npassword=skyserver\n')
    f.close()
    sys.stdout.write('Create User\n')
    client(clcmd + "<%s" % ('%s/../create_user.sql' % dir))
    sys.stdout.write('tables\n')
    client(clcmd + "<%s" % ('%s/../../../sql/math.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../../../sql/cache.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../../../sql/skyserver.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../Skyserver_tables.sql' % dir), skyenv)
    client("cat %s/../Skyserver_import.sql | sed -e \"s|DATA|%s/../microsky|g\" | %s " % (dir, dir, clcmd), skyenv)
    client(clcmd + "<%s" % ('%s/../Skyserver_constraints.sql' % dir), skyenv)
    sys.stdout.write('views\n')
    client(clcmd + "<%s" % ('%s/../Skyserver_views.sql' % dir), skyenv)
    sys.stdout.write('functions\n')
    client(clcmd + "<%s" % ('%s/../Skyserver_functions.sql' % dir), skyenv)
    sys.stdout.write('Cleanup\n')
    client(clcmd + "<%s" % ('%s/../Skyserver_dropFunctions.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../Skyserver_dropMs_functions.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../Skyserver_dropMath.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../Skyserver_dropCache.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../Skyserver_dropViews.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../Skyserver_dropConstraints.sql' % dir), skyenv)
    client(clcmd + "<%s" % ('%s/../Skyserver_dropTables.sql' % dir), skyenv)
    sys.stdout.write('Remove User\n')
    client(clcmd + "<%s" % ('%s/../drop_user.sql' % dir))

    os.unlink(skyenv['DOTMONETDBFILE'])

main()
