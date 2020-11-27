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
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        s.communicate()
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        client = pymonetdb.connect(database='db1', port=myport, autocommit=True)
        cursor = client.cursor()
        cursor.execute("""
        with describe_all_objects as (
        select s.name as sname,
            t.name,
            s.name || '.' || t.name as fullname,
            cast(case t.type
            when 1 then 2
            else 1
            end as smallint) as ntype,
            (case when t.system then 'SYSTEM ' else '' end) || tt.table_type_name as type,
            t.system
        from sys._tables t
        left outer join sys.schemas s on t.schema_id = s.id
        left outer join sys.table_types tt on t.type = tt.table_type_id )
        select type, fullname from describe_all_objects where (ntype & 3) > 0 and not system and (sname is null or sname = current_schema) order by fullname, type;
        """)
        if cursor.fetchall() != []:
            sys.stderr.write('An empty result set expected')
        cursor.close()
        client.close()

        s.communicate()
