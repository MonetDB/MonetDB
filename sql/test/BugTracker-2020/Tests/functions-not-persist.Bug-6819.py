import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process


def client(input):
    c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

create = '''\
CREATE FUNCTION myfunc1(input1 INT, input2 INT) RETURNS INT BEGIN RETURN input1 + input2; END;\
CREATE FUNCTION myfunc2(input1 INT, input2 INT) RETURNS INT LANGUAGE PYTHON {return (input1 + input2)};\
CREATE FUNCTION myfunc3(input1 INT, input2 INT) RETURNS INT LANGUAGE PYTHON_MAP {return (input1 + input2)};\
CREATE FUNCTION myfunc4(input1 INT, input2 INT) RETURNS INT LANGUAGE PYTHON3 {return (input1 + input2)};\
CREATE FUNCTION myfunc5(input1 INT, input2 INT) RETURNS INT LANGUAGE PYTHON3_MAP {return (input1 + input2)};\
CREATE FUNCTION myfunc6(input1 INT, input2 INT) RETURNS INT LANGUAGE R {return (input1 + input2)};\
CREATE FUNCTION myfunc7(input1 INT, input2 INT) RETURNS INTEGER LANGUAGE C {\
    result->initialize(result, input1.count);\
    for(size_t i = 0; i < input1.count; i++) {\
        result->data[i] = input1.data[i] + input2.data[i];\
    }\
};
'''
run = 'SELECT CAST(myfunc1(1, 1) + myfunc2(1, 1) + myfunc3(1, 1) + myfunc4(1, 1) + myfunc5(1, 1) + myfunc6(1, 1) + myfunc7(1, 1) AS BIGINT);'

drop = '''
DROP FUNCTION myfunc1;\
DROP FUNCTION myfunc2;\
DROP FUNCTION myfunc3;\
DROP FUNCTION myfunc4;\
DROP FUNCTION myfunc5;\
DROP FUNCTION myfunc6;\
DROP FUNCTION myfunc7;
'''

server_args = ['--set', 'embedded_py=3', '--set', 'embedded_r=true', '--set', 'embedded_c=true']

s = process.server(args = server_args, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client(create + run)
server_stop(s)

s = process.server(args = server_args, stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client(run + drop)
server_stop(s)
