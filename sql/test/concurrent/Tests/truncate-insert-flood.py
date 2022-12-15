import sys, os, tempfile, pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

from concurrent.futures import ThreadPoolExecutor

nr_clients = 16

with tempfile.TemporaryDirectory() as dbfarm:
    os.mkdir(os.path.join(dbfarm, 'db'))

    with open(os.path.join(dbfarm, 'errout'), mode='a+') as errout:
        with process.server(
                        dbname='db',
                        dbfarm=os.path.join(dbfarm, 'db'),
                        args=[ "--set", "gdk_nr_threads=0",  "--forcemito"],
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=errout,
                        mapiport='0') as db_proc:

            def client(id):
                conn = pymonetdb.connect(
                                database='db',
                                port=db_proc.dbport,
                                autocommit=True)
                cursor = conn.cursor()

                init = f'''
                drop table if exists foo_{id};
                create table foo_{id} (c1, c2, c3, c4, c5) AS VALUES
                (10, 20, 30, 40, 50),
                (11, 21, 31, 41, 51),
                (12, 22, 32, 42, 52);
                set optimizer = 'minimal_fast';
                '''
                cursor.execute(init)

                truncate_and_insert_queries = f"""
                truncate foo_{id};
                insert into foo_{id} VALUES
                (10, 20, 30, 40, 50),
                (11, 21, 31, 41, 51),
                (12, 22, 32, 42, 52);
                """
                nr_queries = 1600
                for x in range(0, nr_queries):
                    cursor.execute(truncate_and_insert_queries)

            with ThreadPoolExecutor(nr_clients) as pool:
                pool.map(client, range(nr_clients))
            errout.flush()

        errout.seek(0)

        error = False
        for line in errout:
            line = line.strip()
            if "builtin" not in line and "cmdline" not in line:
                print("UNEXPECTED OUTPUT:", line, file=sys.stderr)
                error = True
        if error:
            exit(1)
