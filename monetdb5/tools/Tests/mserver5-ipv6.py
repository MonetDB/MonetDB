import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.server(stdin=process.PIPE, stdout=process.PIPE,
                    stderr=process.PIPE, ipv6=True) as server:
    with process.client('sql', host='::1', server=server,
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as client:
        cout, cerr = client.communicate('''
start transaction;
create table "things" ("col1" int);
insert into "things" values (1); select "col1" from "things";
rollback;
''')

    sout, serr = server.communicate()
    sys.stdout.write(sout)
    sys.stderr.write(serr)

#    sys.stdout.write(cout)
    sys.stderr.write(cerr)
