from MonetDBtesting import tpymonetdb as pymonetdb
import os, sys, tempfile

try:
    from MonetDBtesting import process
except ImportError:
    import process


with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'node1'))
    os.mkdir(os.path.join(farm_dir, 'node2'))

    with process.server(mapiport='0', dbname='node1',
                        dbfarm=os.path.join(farm_dir, 'node1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as node1_proc:
        node1_conn = pymonetdb.connect(database='node1', port=node1_proc.dbport, autocommit=True)
        node1_cur = node1_conn.cursor()

        node1_cur.execute("create table remote_data (id int, name varchar(2048))")
        if node1_cur.execute("insert into remote_data values (1, 'Name 1')") != 1:
            sys.stderr.write("1 row inserted expected")
        node1_cur.execute("select * from remote_data")
        if node1_cur.fetchall() != [(1, 'Name 1')]:
            sys.stderr.write("Just row (1, 'Name 1') expected")
        node1_cur.execute("select * from remote_data where name like 'N%'")
        if node1_cur.fetchall() != [(1, 'Name 1')]:
            sys.stderr.write("Just row (1, 'Name 1') expected")

        with process.server(mapiport='0', dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as node2_proc:
            node2_conn = pymonetdb.connect(database='node2', port=node2_proc.dbport, autocommit=True)
            node2_cur = node2_conn.cursor()

            node2_cur.execute("create remote table remote_data (id int, name varchar(2048)) on 'mapi:monetdb://localhost:{}/node1/sys/remote_data'".format(node1_proc.dbport))
            node2_cur.execute("select * from remote_data")
            if node2_cur.fetchall() != [(1, 'Name 1')]:
                sys.stderr.write("Just row (1, 'Name 1') expected")
            node2_cur.execute("select * from remote_data where name like 'N%'")
            if node2_cur.fetchall() != [(1, 'Name 1')]:
                sys.stderr.write("Just row (1, 'Name 1') expected")
            node2_cur.execute("select rank() over () from remote_data where name like 'N%'")
            if node2_cur.fetchall() != [(1,)]:
                sys.stderr.write("Just row (1,) expected")
            node2_cur.execute("select name like 'N%' from remote_data")
            if node2_cur.fetchall() != [(True,)]:
                sys.stderr.write("Just row (True,) expected")
            node2_cur.execute("select corr(1,1) from remote_data")
            if node2_cur.fetchall() != [(None,)]:
                sys.stderr.write("Just row (None,) expected")
            node2_cur.execute("select corr(1,1) over () from remote_data")
            if node2_cur.fetchall() != [(None,)]:
                sys.stderr.write("Just row (None,) expected")
            node2_cur.execute("select count(*) over (), max(name) over (), min(name) over (partition by name order by name rows between 3 preceding and 2 preceding) from remote_data")
            if node2_cur.fetchall() != [(1, 'Name 1', None)]:
                sys.stderr.write("Just row (1, 'Name 1', None) expected")
            node2_cur.execute("""select case when id = 1 then 2 when id = 2 then 10 when id = 3 then 3 else 100 end, nullif(id, id), coalesce(id, id + 10, 10),
                                        case id when 1 then 5 when 2 then 10 when 3 then 60 else 4 end, greatest(id - 7, id + 7), lead(1,1,1) over () from remote_data""")
            if node2_cur.fetchall() != [(2, None, 1, 5, 8, 1)]:
                sys.stderr.write("Just row (2, None, 1, 5, 8, 1) expected")
            node2_cur.execute("create function \"myfunc\"(\"myarg\" int) returns int return \"myarg\";")
            try:
                node2_cur.execute("select \"myfunc\"(1) from remote_data")
                sys.stderr.write("Exception expected")
            except pymonetdb.DatabaseError as e:
                pass

            # cleanup: shutdown the monetdb servers and remove tempdir
            node1_cur.close()
            node1_conn.close()
            node2_cur.close()
            node2_conn.close()
            node1_proc.communicate()
            node2_proc.communicate()
