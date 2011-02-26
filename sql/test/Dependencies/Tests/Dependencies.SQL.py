import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(cmd, infile, user = 'monetdb', passwd = 'monetdb'):
    clt = process.client(cmd, user = user, passwd = passwd,
                         stdin = open(infile), stdout = process.PIPE,
                         stderr = process.PIPE)
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    sys.stdout.write('Dependencies between User and Schema\n')
    client('sql', os.path.join(os.getenv('RELSRCDIR'), os.pardir, 'dependency_owner_schema_1.sql'))
    sys.stdout.write('done\n')

    client('sql', os.path.join(os.getenv('RELSRCDIR'), os.pardir, 'dependency_owner_schema_2.sql'), user = 'monet_test', passwd = 'pass_test')
    sys.stdout.write('done\n')

    sys.stdout.write('Dependencies between database objects\n')
    client('sql', os.path.join(os.getenv('RELSRCDIR'), os.pardir, 'dependency_DBobjects.sql'))
    sys.stdout.write('done\n')

    sys.stdout.write('Dependencies between functions with same name\n')
    client('sql', os.path.join(os.getenv('RELSRCDIR'), os.pardir, 'dependency_functions.sql'))
    sys.stdout.write('done\n')

    sys.stdout.write('Cleanup\n')
    client('sql', os.path.join(os.getenv('RELSRCDIR'), os.pardir, 'dependency_owner_schema_3.sql'))
    sys.stdout.write('done\n')

main()
