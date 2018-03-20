import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_start(args):
    sys.stderr.write('#mserver: "%s"\n' % ' '.join(args))
    sys.stderr.flush()
    srv = process.server(args = args, stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(lang, file):
    sys.stderr.write('#client: "%s"\n' % file)
    sys.stderr.flush()
    clt = process.client(lang.lower(), stdin = open(file),
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate()

def delete_where_in_sub_query_statement():
    return "delete from table1 where id in (select subtable1.id from subtable1);select count(*) from table1;"

def main():
    srv = server_start([])

    out, err = client('SQL',
                      os.path.join(os.getenv('RELSRCDIR'),
                                   'delete.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)

    c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

    out, err = c.communicate(delete_where_in_sub_query_statement())

    sys.stdout.write(out)
    sys.stderr.write(err)

    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

main()
