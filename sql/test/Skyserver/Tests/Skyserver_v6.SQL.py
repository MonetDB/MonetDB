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
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_tables_v6.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user='monetdb', passwd='monetdb', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as clt1:
        sql = open(os.path.join(dir, os.pardir, 'Skyserver_import_v6.sql')).read().replace('DATA_DIR',os.path.join(dir,os.pardir,'microsky_v6').replace('\\','\\\\'))
        out,err = clt1.communicate(sql)
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_constraints_v6.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    sys.stdout.write('views\n')
    sys.stderr.write('views\n')
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_views_v6.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    sys.stdout.write('functions\n')
    sys.stderr.write('functions\n')
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_functions_v6.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    sys.stdout.write('Cleanup\n')
    sys.stderr.write('Cleanup\n')
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_dropFunctions_v6.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_dropViews_v6.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_dropConstraints_v6.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
        out,err = clt1.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user=user, passwd=passwd, args=[os.path.join(dir, os.pardir,'Skyserver_dropTables_v6.sql')], stdout=process.PIPE, stderr=process.PIPE) as clt1:
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
