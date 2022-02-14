import sys, os, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process


with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'node1'))
    os.mkdir(os.path.join(farm_dir, 'node2'))
    os.mkdir(os.path.join(farm_dir, 'node3'))

    with process.server(mapiport='0', dbname='node1',
                        dbfarm=os.path.join(farm_dir, 'node1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as node1_proc:

        with process.server(mapiport='0', dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as node2_proc:

            with process.server(mapiport='0', dbname='node3',
                                dbfarm=os.path.join(farm_dir, 'node3'),
                                stdin=process.PIPE, stdout=process.PIPE,
                                stderr=process.PIPE) as node3_proc:

                node1_conn = pymonetdb.connect(database='node1', port=node1_proc.dbport, autocommit=True)
                node1_cur = node1_conn.cursor()
                node2_conn = pymonetdb.connect(database='node2', port=node2_proc.dbport, autocommit=True)
                node2_cur = node2_conn.cursor()
                node3_conn = pymonetdb.connect(database='node3', port=node3_proc.dbport, autocommit=True)
                node3_cur = node3_conn.cursor()

                # Setup local s1 table
                node1_cur.execute("CREATE TABLE s1 (s_pk INT, i INT)")
                node1_cur.execute("ALTER TABLE s1 ADD CONSTRAINT s1_pk PRIMARY KEY (s_pk)")
                node1_cur.execute("INSERT INTO s1 VALUES (0, 23), (1, 42)")
                # Setup local t1 table
                node1_cur.execute("CREATE TABLE t1 (t_pk INT, t_fk INT, s VARCHAR(10))")
                node1_cur.execute("ALTER TABLE t1 ADD CONSTRAINT t1_pk PRIMARY KEY (t_pk)")
                node1_cur.execute("INSERT INTO t1 VALUES (0, 0, 'abc'), (1, 2, 'efg')")

                # Setup local s2 table
                node2_cur.execute("CREATE TABLE s2 (s_pk INT, i INT)")
                node2_cur.execute("ALTER TABLE s2 ADD CONSTRAINT s2_pk PRIMARY KEY (s_pk)")
                node2_cur.execute("INSERT INTO s2 VALUES (2, 100), (3, 77)")
                # Setup local t2 table
                node2_cur.execute("CREATE TABLE t2 (t_pk INT, t_fk INT, s VARCHAR(10))")
                node2_cur.execute("ALTER TABLE t2 ADD CONSTRAINT t2_pk PRIMARY KEY (t_pk)")
                node2_cur.execute("INSERT INTO t2 VALUES (2, 3, 'hij'), (3, 1, 'klm')")

                # Setup remote s2 table
                node1_cur.execute("CREATE REMOTE TABLE s2 (s_pk INT, i INT) on 'mapi:monetdb://localhost:{}/node2'".format(node2_proc.dbport))
                node1_cur.execute("ALTER TABLE s2 ADD CONSTRAINT s2_pk PRIMARY KEY (s_pk)")
                # Setup merge s table
                node1_cur.execute("CREATE MERGE TABLE s_combined (s_pk INT, i INT)")
                node1_cur.execute("ALTER TABLE s_combined ADD CONSTRAINT sc_pk PRIMARY KEY (s_pk)")
                node1_cur.execute("ALTER TABLE s_combined ADD TABLE s1")
                node1_cur.execute("ALTER TABLE s_combined ADD TABLE s2")
                # Setup local t1 table foreign key
                node1_cur.execute("ALTER TABLE t1 ADD CONSTRAINT t1_fk FOREIGN KEY (t_fk) references s_combined")

                # Setup remote s1 table
                node2_cur.execute("CREATE REMOTE TABLE s1 (s_pk INT, i INT) on 'mapi:monetdb://localhost:{}/node1'".format(node1_proc.dbport))
                node2_cur.execute("ALTER TABLE s1 ADD CONSTRAINT s1_pk PRIMARY KEY (s_pk)")
                # Setup merge s table
                node2_cur.execute("CREATE MERGE TABLE s_combined (s_pk INT, i INT)")
                node2_cur.execute("ALTER TABLE s_combined ADD CONSTRAINT sc_pk PRIMARY KEY (s_pk)")
                node2_cur.execute("ALTER TABLE s_combined ADD TABLE s2")
                node2_cur.execute("ALTER TABLE s_combined ADD TABLE s1")
                # Setup local t2 table foreign key
                node2_cur.execute("ALTER TABLE t2 ADD CONSTRAINT t2_fk FOREIGN KEY (t_fk) references s_combined")

                # Add all remote tables for s (make sure they match their counterparts)
                node3_cur.execute("CREATE REMOTE TABLE s1 (s_pk INT, i INT) on 'mapi:monetdb://localhost:{}/node1'".format(node1_proc.dbport))
                node3_cur.execute("ALTER TABLE s1 ADD CONSTRAINT s1_pk PRIMARY KEY (s_pk)")
                node3_cur.execute("CREATE REMOTE TABLE s2 (s_pk INT, i INT) on 'mapi:monetdb://localhost:{}/node2'".format(node2_proc.dbport))
                node3_cur.execute("ALTER TABLE s2 ADD CONSTRAINT s2_pk PRIMARY KEY (s_pk)")
                # Setup the comb s table
                node3_cur.execute("CREATE MERGE TABLE s_combined (s_pk INT, i INT)")
                node3_cur.execute("ALTER TABLE s_combined ADD CONSTRAINT sc_pk PRIMARY KEY (s_pk)")
                node3_cur.execute("ALTER TABLE s_combined ADD TABLE s1")
                node3_cur.execute("ALTER TABLE s_combined ADD TABLE s2")
                # Add all remote tables for t (make sure they match their counterparts)
                node3_cur.execute("CREATE REMOTE TABLE t1 (t_pk INT, t_fk INT, s VARCHAR(10)) on 'mapi:monetdb://localhost:{}/node1'".format(node1_proc.dbport))
                node3_cur.execute("ALTER TABLE t1 ADD CONSTRAINT t1_pk PRIMARY KEY (t_pk)")
                node3_cur.execute("ALTER TABLE t1 ADD CONSTRAINT t1_fk FOREIGN KEY (t_fk) references s_combined")
                node3_cur.execute("CREATE REMOTE TABLE t2 (t_pk INT, t_fk INT, s VARCHAR(10)) on 'mapi:monetdb://localhost:{}/node2'".format(node2_proc.dbport))
                node3_cur.execute("ALTER TABLE t2 ADD CONSTRAINT t2_pk PRIMARY KEY (t_pk)")
                node3_cur.execute("ALTER TABLE t2 ADD CONSTRAINT t2_fk FOREIGN KEY (t_fk) references s_combined")
                # Setup the comb table for t
                node3_cur.execute("CREATE MERGE TABLE t_combined (t_pk INT, t_fk INT, s VARCHAR(10))")
                node3_cur.execute("ALTER TABLE t_combined ADD CONSTRAINT tc_pk PRIMARY KEY (t_pk)")
                node3_cur.execute("ALTER TABLE t_combined ADD CONSTRAINT tc_fk FOREIGN KEY (t_fk) references s_combined")
                node3_cur.execute("ALTER TABLE t_combined ADD TABLE t1")
                node3_cur.execute("ALTER TABLE t_combined ADD TABLE t2")

                node3_cur.execute("SELECT s_pk FROM s_combined, t_combined WHERE s_pk = t_fk ORDER BY s_pk")
                res = node3_cur.fetchall()
                if res != [(0,), (0,), (1,), (1,), (2,), (2,), (3,), (3,)]:
                    sys.stderr.write("[(0,), (0,), (1,), (1,), (2,), (2,), (3,), (3,)] expected, got %s" % (res,))

                # cleanup: shutdown the monetdb servers
                node1_cur.close()
                node1_conn.close()
                node2_cur.close()
                node2_conn.close()
                node3_cur.close()
                node3_conn.close()
                out, err = node1_proc.communicate()
                sys.stderr.write(err)
                out, err = node2_proc.communicate()
                sys.stderr.write(err)
                out, err = node3_proc.communicate()
                sys.stderr.write(err)
