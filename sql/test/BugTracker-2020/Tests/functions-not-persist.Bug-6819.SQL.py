import os, socket, sys, tempfile, pymonetdb
try:
    from MonetDBtesting import process
except ImportError:
    import process

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('', 0))
port = sock.getsockname()[1]
sock.close()

server_args = ['--set', 'embedded_py=3', '--set', 'embedded_r=true', '--set', 'embedded_c=true']

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))

    with process.server(args = server_args, mapiport=port, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        client1 = pymonetdb.connect(database='db1', port=port, autocommit=True)
        cursor1 = client1.cursor()
        cursor1.execute("""
        CREATE FUNCTION myfunc1(input1 INT, input2 INT) RETURNS INT BEGIN RETURN input1 + input2; END;
        CREATE FUNCTION myfunc2(input1 INT, input2 INT) RETURNS INT LANGUAGE PYTHON {return (input1 + input2)};
        CREATE FUNCTION myfunc3(input1 INT, input2 INT) RETURNS INT LANGUAGE PYTHON_MAP {return (input1 + input2)};
        CREATE FUNCTION myfunc4(input1 INT, input2 INT) RETURNS INT LANGUAGE PYTHON3 {return (input1 + input2)};
        CREATE FUNCTION myfunc5(input1 INT, input2 INT) RETURNS INT LANGUAGE PYTHON3_MAP {return (input1 + input2)};
        CREATE FUNCTION myfunc6(input1 INT, input2 INT) RETURNS INT LANGUAGE R {return (input1 + input2)};
        CREATE FUNCTION myfunc7(input1 INT, input2 INT) RETURNS INTEGER LANGUAGE C {
            result->initialize(result, input1.count);
            for(size_t i = 0; i < input1.count; i++) {
                result->data[i] = input1.data[i] + input2.data[i];
            }
        }; """)
        cursor1.execute("SELECT CAST(myfunc1(1, 1) + myfunc2(1, 1) + myfunc3(1, 1) + myfunc4(1, 1) + myfunc5(1, 1) + myfunc6(1, 1) + myfunc7(1, 1) AS BIGINT);")
        result = cursor1.fetchall()[0][0]
        if result != 14:
            sys.stderr.write("The result should have been 14")
        cursor1.close()
        client1.close()
        s.communicate()

    with process.server(args = server_args, mapiport=port, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        client1 = pymonetdb.connect(database='db1', port=port, autocommit=True)
        cursor1 = client1.cursor()
        cursor1.execute("SELECT CAST(myfunc1(1, 1) + myfunc2(1, 1) + myfunc3(1, 1) + myfunc4(1, 1) + myfunc5(1, 1) + myfunc6(1, 1) + myfunc7(1, 1) AS BIGINT);")
        result = cursor1.fetchall()[0][0]
        if result != 14:
            sys.stderr.write("The result should have been 14")
        cursor1.execute("""
        DROP FUNCTION myfunc1;
        DROP FUNCTION myfunc2;
        DROP FUNCTION myfunc3;
        DROP FUNCTION myfunc4;
        DROP FUNCTION myfunc5;
        DROP FUNCTION myfunc6;
        DROP FUNCTION myfunc7;
        """)
        cursor1.close()
        client1.close()
        s.communicate()
