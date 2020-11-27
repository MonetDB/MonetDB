import os, socket, sys, tempfile, pymonetdb
try:
    from MonetDBtesting import process
except ImportError:
    import process

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

myport = freeport()


with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client1 = pymonetdb.connect(database='db1', port=myport, autocommit=True)
        cur1 = client1.cursor()
        cur1.execute('''
        create table t (a int, b int, c int);
        alter table t add unique (b);
        ''')
        cur1.close()
        client1.close()
        s.communicate()

    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client1 = pymonetdb.connect(database='db1', port=myport, autocommit=True)
        cur1 = client1.cursor()
        cur1.execute('alter table t drop column c;')
        cur1.close()
        client1.close()
        s.communicate()

    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client1 = pymonetdb.connect(database='db1', port=myport, autocommit=True)
        cur1 = client1.cursor()
        try:
            cur1.execute('alter table t drop column b; --error, b has a depenency')
            sys.stderr.write("Exception expected")
        except pymonetdb.DatabaseError as e:
            if "cannot drop column 'b': there are database objects which depend on it" not in str(e):
                sys.stderr.write("Error: cannot drop column 'b': there are database objects which depend on it expected")
        cur1.close()
        client1.close()
        s.communicate()

    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client1 = pymonetdb.connect(database='db1', port=myport, autocommit=True)
        cur1 = client1.cursor()
        cur1.execute('select count(*) from sys.objects inner join sys.dependencies on objects.id = dependencies.depend_id inner join sys.columns on dependencies.id = columns.id inner join sys.tables on columns.table_id = tables.id where tables.name = \'t\';')
        if cur1.fetchall() != [(2,)]:
            sys.stderr.write("2 expected")
        cur1.execute('select count(*) from sys.dependencies inner join sys.columns on dependencies.id = columns.id inner join sys.tables on columns.table_id = tables.id where tables.name = \'t\';')
        if cur1.fetchall() != [(2,)]:
            sys.stderr.write("2 expected")
        cur1.execute('select keys.type, keys.name, keys.rkey, keys.action from sys.keys inner join sys.tables on tables.id = keys.table_id where tables.name = \'t\';')
        if cur1.fetchall() != [(1,"t_b_unique",-1,-1)]:
            sys.stderr.write('[(1,\"t_b_unique\",-1,-1)] expected')
        cur1.execute('select idxs.type, idxs.name from sys.idxs inner join tables on tables.id = idxs.table_id where tables.name = \'t\';')
        if cur1.fetchall() != [(0,"t_b_unique")]:
            sys.stderr.write('[(0,"t_b_unique")] expected')
        cur1.execute('alter table t drop column b cascade;')
        cur1.execute('select count(*) from sys.objects inner join sys.dependencies on objects.id = dependencies.depend_id inner join sys.columns on dependencies.id = columns.id inner join sys.tables on columns.table_id = tables.id where tables.name = \'t\';')
        if cur1.fetchall() != [(0,)]:
            sys.stderr.write("0 expected")
        cur1.execute('select count(*) from sys.dependencies inner join sys.columns on dependencies.id = columns.id inner join sys.tables on columns.table_id = tables.id where tables.name = \'t\';')
        if cur1.fetchall() != [(0,)]:
            sys.stderr.write("0 expected")
        cur1.execute('select keys.type, keys.name, keys.rkey, keys.action from sys.keys inner join sys.tables on tables.id = keys.table_id where tables.name = \'t\';')
        if cur1.fetchall() != []:
            sys.stderr.write('[] expected')
        cur1.execute('select idxs.type, idxs.name from sys.idxs inner join tables on tables.id = idxs.table_id where tables.name = \'t\';')
        if cur1.fetchall() != []:
            sys.stderr.write('[] expected')
        cur1.close()
        client1.close()
        s.communicate()

    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client1 = pymonetdb.connect(database='db1', port=myport, autocommit=True)
        cur1 = client1.cursor()
        cur1.execute('drop table t;')
        cur1.execute('''
        start transaction;
        create table t (a int, b int, c int);
        alter table t add unique (b);
        ''')
        cur1.execute('select * from t;')
        if cur1.fetchall() != []:
            sys.stderr.write('[] expected')
        cur1.execute('select count(*) from sys.objects inner join sys.dependencies on objects.id = dependencies.depend_id inner join sys.columns on dependencies.id = columns.id inner join sys.tables on columns.table_id = tables.id where tables.name = \'t\';')
        if cur1.fetchall() != [(2,)]:
            sys.stderr.write("2 expected")
        cur1.execute('select count(*) from sys.dependencies inner join sys.columns on dependencies.id = columns.id inner join sys.tables on columns.table_id = tables.id where tables.name = \'t\';')
        if cur1.fetchall() != [(2,)]:
            sys.stderr.write("2 expected")
        cur1.execute('select keys.type, keys.name, keys.rkey, keys.action from sys.keys inner join sys.tables on tables.id = keys.table_id where tables.name = \'t\';')
        if cur1.fetchall() != [(1,"t_b_unique",-1,-1)]:
            sys.stderr.write('[(1,\"t_b_unique\",-1,-1)] expected')
        cur1.execute('select idxs.type, idxs.name from sys.idxs inner join sys.tables on tables.id = idxs.table_id where tables.name = \'t\';')
        if cur1.fetchall() != [(0,"t_b_unique")]:
            sys.stderr.write('[(0,"t_b_unique")] expected')
        cur1.execute('alter table t drop column b cascade;')
        cur1.execute('select count(*) from sys.objects inner join sys.dependencies on objects.id = dependencies.depend_id inner join sys.columns on dependencies.id = columns.id inner join sys.tables on columns.table_id = tables.id where tables.name = \'t\';')
        if cur1.fetchall() != [(0,)]:
            sys.stderr.write("0 expected")
        cur1.execute('select count(*) from sys.dependencies inner join sys.columns on dependencies.id = columns.id inner join sys.tables on columns.table_id = tables.id where tables.name = \'t\';')
        if cur1.fetchall() != [(0,)]:
            sys.stderr.write("0 expected")
        cur1.execute('select keys.type, keys.name, keys.rkey, keys.action from sys.keys inner join sys.tables on tables.id = keys.table_id where tables.name = \'t\';')
        if cur1.fetchall() != []:
            sys.stderr.write('[] expected')
        cur1.execute('select idxs.type, idxs.name from sys.idxs inner join sys.tables on tables.id = idxs.table_id where tables.name = \'t\';')
        if cur1.fetchall() != []:
            sys.stderr.write('[] expected')
        cur1.execute('select * from t;')
        if cur1.fetchall() != []:
            sys.stderr.write('[] expected')
        cur1.execute('commit;')
        cur1.execute('select * from t;')
        if cur1.fetchall() != []:
            sys.stderr.write('[] expected')
        cur1.execute('drop table t;')
        cur1.close()
        client1.close()
        s.communicate()
