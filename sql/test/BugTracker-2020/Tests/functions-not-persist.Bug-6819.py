from MonetDBtesting import tpymonetdb as pymonetdb
import os, sys, tempfile
try:
    from MonetDBtesting import process
except ImportError:
    import process

expected = 2

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        client1 = pymonetdb.connect(database=s.usock or 'db1', port=s.dbport, autocommit=True)
        cursor1 = client1.cursor()
        cursor1.execute("""
        CREATE FUNCTION myfunc1(input1 INT, input2 INT) RETURNS INT BEGIN RETURN input1 + input2; END; """)
        cursor1.execute("SELECT CAST(myfunc1(1, 1) AS BIGINT);")
        result = cursor1.fetchall()[0][0]
        if result != expected:
            sys.stderr.write(f"The result should have been {expected}")
        cursor1.close()
        client1.close()
        s.communicate()

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        client1 = pymonetdb.connect(database=s.usock or 'db1', port=s.dbport, autocommit=True)
        cursor1 = client1.cursor()
        cursor1.execute("SELECT CAST(myfunc1(1, 1) AS BIGINT);")
        result = cursor1.fetchall()[0][0]
        if result != expected:
            sys.stderr.write(f"The result should have been {expected}")
        cursor1.execute("""
        DROP FUNCTION myfunc1;
        """)
        cursor1.close()
        client1.close()
        s.communicate()
