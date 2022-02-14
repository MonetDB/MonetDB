import os, sys, tempfile, pymonetdb

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

        node1_cur.execute("CREATE TABLE mytest (toc_no String null,mesure_de int null)")
        node1_cur.execute("insert into mytest values('A000000009', 20201006), ('A000000010', 20201007), ('A000000011', 20201008), ('A000000012', 20201009), ('A000000013', 20201010), ('A000000014', 20201011), ('A000000015', 20201012), ('A000000016', 20201013)")
        node1_cur.execute("select toc_no, mesure_de from mytest")
        if node1_cur.fetchall() != [('A000000009', 20201006), ('A000000010', 20201007), ('A000000011', 20201008), ('A000000012', 20201009), ('A000000013', 20201010), ('A000000014', 20201011), ('A000000015', 20201012), ('A000000016', 20201013)]:
            sys.stderr.write("[('A000000009', 20201006), ('A000000010', 20201007), ('A000000011', 20201008), ('A000000012', 20201009), ('A000000013', 20201010), ('A000000014', 20201011), ('A000000015', 20201012), ('A000000016', 20201013)] expected")
        node1_cur.execute("select toc_no, mesure_de from mytest where mesure_de >= 20201001 and mesure_de < 20201011 order by mesure_de desc")
        if node1_cur.fetchall() != [('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)]:
            sys.stderr.write("[('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)] expected")
        node1_cur.execute("select toc_no, mesure_de from mytest where mesure_de > 20201006 and mesure_de <= 20201011 order by mesure_de desc")
        if node1_cur.fetchall() != [('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007)]:
            sys.stderr.write("[('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007)] expected")
        node1_cur.execute("select toc_no, mesure_de from mytest where mesure_de > 20201006 and mesure_de < 20201011 order by mesure_de desc")
        if node1_cur.fetchall() != [('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007)]:
            sys.stderr.write("[('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007)] expected")
        node1_cur.execute("select toc_no, mesure_de from mytest where mesure_de >= 20201006 and mesure_de <= 20201011 order by mesure_de desc")
        if node1_cur.fetchall() != [('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)]:
            sys.stderr.write("[('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)] expected")
        node1_cur.execute("select toc_no, mesure_de from mytest where mesure_de > 20201007 and mesure_de < 20201011 order by mesure_de desc")
        if node1_cur.fetchall() != [('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008)]:
            sys.stderr.write("[('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008)] expected")
        node1_cur.execute("select toc_no, mesure_de from mytest where mesure_de BETWEEN 20201001 and 20201011 order by mesure_de desc")
        if node1_cur.fetchall() != [('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)]:
            sys.stderr.write("[('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)] expected")

        with process.server(mapiport='0', dbname='node2',
                            dbfarm=os.path.join(farm_dir, 'node2'),
                            stdin=process.PIPE, stdout=process.PIPE,
                            stderr=process.PIPE) as node2_proc:
            node2_conn = pymonetdb.connect(database='node2', port=node2_proc.dbport, autocommit=True)
            node2_cur = node2_conn.cursor()

            node2_cur.execute("CREATE REMOTE TABLE mytest (toc_no String null,mesure_de int null) on 'mapi:monetdb://localhost:{}/node1/sys/mytest'".format(node1_proc.dbport))
            node2_cur.execute("select toc_no, mesure_de from mytest")
            if node2_cur.fetchall() != [('A000000009', 20201006), ('A000000010', 20201007), ('A000000011', 20201008), ('A000000012', 20201009), ('A000000013', 20201010), ('A000000014', 20201011), ('A000000015', 20201012), ('A000000016', 20201013)]:
                sys.stderr.write("[('A000000009', 20201006), ('A000000010', 20201007), ('A000000011', 20201008), ('A000000012', 20201009), ('A000000013', 20201010), ('A000000014', 20201011), ('A000000015', 20201012), ('A000000016', 20201013)] expected")
            node2_cur.execute("select toc_no, mesure_de from mytest where mesure_de >= 20201001 and mesure_de < 20201011 order by mesure_de desc")
            if node2_cur.fetchall() != [('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)]:
                sys.stderr.write("[('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)] expected")
            node2_cur.execute("select toc_no, mesure_de from mytest where mesure_de > 20201006 and mesure_de <= 20201011 order by mesure_de desc")
            if node2_cur.fetchall() != [('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007)]:
                sys.stderr.write("[('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007)] expected")
            node2_cur.execute("select toc_no, mesure_de from mytest where mesure_de > 20201006 and mesure_de < 20201011 order by mesure_de desc")
            if node2_cur.fetchall() != [('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007)]:
                sys.stderr.write("[('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007)] expected")
            node2_cur.execute("select toc_no, mesure_de from mytest where mesure_de >= 20201006 and mesure_de <= 20201011 order by mesure_de desc")
            if node2_cur.fetchall() != [('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)]:
                sys.stderr.write("[('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)] expected")
            node2_cur.execute("select toc_no, mesure_de from mytest where mesure_de > 20201007 and mesure_de < 20201011 order by mesure_de desc")
            if node2_cur.fetchall() != [('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008)]:
                sys.stderr.write("[('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008)] expected")
            node2_cur.execute("select toc_no, mesure_de from mytest where mesure_de BETWEEN 20201001 and 20201011 order by mesure_de desc")
            if node2_cur.fetchall() != [('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)]:
                sys.stderr.write("[('A000000014', 20201011), ('A000000013', 20201010), ('A000000012', 20201009), ('A000000011', 20201008), ('A000000010', 20201007), ('A000000009', 20201006)] expected")

            # cleanup: shutdown the monetdb servers and remove tempdir
            node1_cur.close()
            node1_conn.close()
            node2_cur.close()
            node2_conn.close()
            node1_proc.communicate()
            node2_proc.communicate()
