import os, pymonetdb

skyuser = 'skyserver'
skypasswd = 'skyserver'
dir = os.getenv('TSTSRCDIR')
port = int(os.getenv('MAPIPORT'))
database = os.getenv('TSTDB')
host = os.getenv('HOST')

def run_script(file, user, passwd):
    client1 = pymonetdb.connect(port=port, database=database, host=host, username=user, password=passwd, autocommit=True)
    cur1 = client1.cursor()
    f = open(os.path.join(dir, os.pardir, file), 'r')
    q = f.read()
    f.close()
    cur1.execute(q)
    cur1.close()
    client1.close()

run_script('create_user.sql', 'monetdb', 'monetdb')
run_script('Skyserver_tables.sql', skyuser, skypasswd)

client1 = pymonetdb.connect(port=port, database=database, host=host, username='monetdb', password='monetdb', autocommit=True)
cur1 = client1.cursor()
f = open(os.path.join(dir, os.pardir, 'Skyserver_import.sql'), 'r')
q = f.read().replace('DATA',os.path.join(dir,os.pardir,'microsky').replace('\\','\\\\'))
f.close()
cur1.execute(q)
cur1.close()
client1.close()

run_script('Skyserver_constraints.sql', skyuser, skypasswd)
run_script('Skyserver_views.sql', skyuser, skypasswd)
run_script('Skyserver_functions.sql', skyuser, skypasswd)
run_script('Skyserver_dropFunctions.sql', skyuser, skypasswd)
run_script('Skyserver_dropViews.sql', skyuser, skypasswd)
run_script('Skyserver_dropConstraints.sql', skyuser, skypasswd)
run_script('Skyserver_dropTables.sql', skyuser, skypasswd)
run_script('drop_user.sql', 'monetdb', 'monetdb')
