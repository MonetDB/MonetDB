import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def main():
    user = 'skyserver'
    passwd = 'skyserver'
    dir = os.getenv('TSTSRCDIR')
    sys.stdout.write('Create User\n')
    sys.stderr.write('Create User\n')
    with process.client('sql', args=[os.path.join(dir, os.pardir,'create_user.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt:
        out,err = clt.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    sys.stdout.write('tables\n')
    sys.stderr.write('tables\n')
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_tables.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user='monetdb', passwd='monetdb', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as clt1:
        sql = open(os.path.join(dir, os.pardir, 'Skyserver_import.sql')).read().replace('DATA',os.path.join(dir,os.pardir,'microsky').replace('\\','\\\\'))
        out,err = clt1.communicate(sql)
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_constraints.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    sys.stdout.write('views\n')
    sys.stderr.write('views\n')
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_views.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    sys.stdout.write('functions\n')
    sys.stderr.write('functions\n')
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_functions.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    sys.stdout.write('Cleanup\n')
    sys.stderr.write('Cleanup\n')
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_dropFunctions.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_dropViews.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_dropConstraints.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_dropTables.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    sys.stdout.write('Remove User\n')
    sys.stderr.write('Remove User\n')
    with process.client('sql', args=[os.path.join(dir, os.pardir,'drop_user.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt:
        out,err = clt.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)

main()
