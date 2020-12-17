import os, socket, tempfile, pymonetdb

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


with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'renames'))
    sport = freeport()
    with process.server(mapiport=sport, dbname='renames', dbfarm=os.path.join(farm_dir, 'renames'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:
        client1 = pymonetdb.connect(database='renames', port=sport, autocommit=True)
        client2 = pymonetdb.connect(database='renames', port=sport, autocommit=True)
        cursor1 = client1.cursor()
        cursor2 = client2.cursor()

        cursor1.execute('CREATE TABLE tab1 (col1 tinyint);')
        cursor1.execute('INSERT INTO tab1 VALUES (1);')

        cursor2.execute('SELECT col1 FROM tab1;')

        cursor1.execute('SELECT col1 FROM tab1;')
        cursor1.execute('ALTER TABLE tab1 RENAME TO tab2;')
        cursor1.execute('SELECT col1 FROM tab2;')
        cursor1.execute('CREATE SCHEMA s2;')
        cursor1.execute('ALTER SCHEMA s2 RENAME TO s3;')
        cursor1.execute('CREATE TABLE s3.tab3 (col1 tinyint);')
        cursor1.execute('INSERT INTO s3.tab3 VALUES (1);')

        cursor2.execute('SELECT col1 FROM s3.tab3;')

        cursor1.execute('SELECT col1 FROM s3.tab3;')
        cursor1.execute('CREATE TABLE tab4 (col1 tinyint, col3 int);')

        cursor2.execute('SELECT col1 FROM tab4;')

        cursor1.execute('ALTER TABLE tab4 RENAME COLUMN col1 TO col2;')
        cursor1.execute('SELECT col2 FROM tab4;')
        cursor1.execute('CREATE SCHEMA s4;')
        cursor1.execute('CREATE TABLE tab5 (col1 int);')
        cursor1.execute('INSERT INTO tab5 VALUES (1);')
        cursor1.execute('ALTER TABLE tab5 SET SCHEMA s4;')

        cursor2.execute('SELECT col1 FROM s4.tab5;')

        cursor1.execute('SELECT col1 FROM s4.tab5;')

        cursor2.execute('SELECT col1 FROM tab2;')
        cursor2.execute('SELECT col1 FROM s3.tab3;')
        cursor2.execute('SELECT col2 FROM tab4;')
        cursor2.execute('SELECT col1 FROM s4.tab5;')

        cursor1.close()
        cursor2.close()
        client1.close()
        client2.close()

        client3 = pymonetdb.connect(database='renames', port=sport, autocommit=True)
        cursor3 = client3.cursor()

        cursor3.execute('SELECT col1 FROM tab2;')
        cursor3.execute('SELECT col1 FROM s3.tab3;')
        cursor3.execute('SELECT col2 FROM tab4;')
        cursor3.execute('SELECT col1 FROM s4.tab5;')

        cursor3.close()
        client3.close()

        client4 = pymonetdb.connect(database='renames', port=sport, autocommit=True)
        cursor4 = client4.cursor()

        cursor4.execute('DROP SCHEMA s3 CASCADE;')
        cursor4.execute('DROP TABLE tab2;')
        cursor4.execute('ALTER TABLE tab4 DROP COLUMN col2;')
        cursor4.execute('DROP TABLE tab4;')
        cursor4.execute('DROP TABLE s4.tab5;')
        cursor4.execute('DROP SCHEMA s4 CASCADE;')

        cursor4.close()
        client4.close()

        s.communicate()
