import sys, os, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

with tempfile.TemporaryDirectory() as farm_dir:
    mypath = os.path.join(farm_dir, '进起都家', 'myserver','mynode')
    os.makedirs(mypath)

    with process.server(mapiport='0', dbname='mynode', dbfarm=mypath,
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as prc:
        conn = pymonetdb.connect(database='mynode', port=prc.dbport, autocommit=True)
        cur = conn.cursor()

        cur.execute('SELECT \'进起都家\';')
        if cur.fetchall() != [('进起都家',)]:
            sys.stderr.write("'进起都家' expected")

        cur.close()
        conn.close()
        prc.communicate()
