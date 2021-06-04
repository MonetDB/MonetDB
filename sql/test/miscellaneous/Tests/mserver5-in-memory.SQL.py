import socket, subprocess, time
from MonetDBtesting.sqltest import SQLTestCase

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

def wait_server_to_start(port, mserver_process):
    started = False
    mserver_process.poll()
    if mserver_process.returncode is not None:
        raise Exception("The server terminated early")
    retry = 0
    while retry < 3:
        retry += 1
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', port))
            flag = sock.recv(2)
            started = True
        except socket.error:
            pass
        time.sleep(0.4)
    if not started:
        raise Exception("The server did not start?")

mserver_process1 = None
mserver_process2 = None

try:
    prt1 = freeport()
    cmd = ['mserver5', '--in-memory', '--set', 'mapi_port=%d' % (prt1,)]
    mserver_process1 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    wait_server_to_start(prt1, mserver_process1)
    with SQLTestCase() as mdb:
        mdb.connect(database=None, port=prt1, username="monetdb", password="monetdb")
        mdb.execute("""
            start transaction;
            create table iwontpersist (mycol int);
            insert into iwontpersist values (1);
            commit;
        """).assertSucceeded()
        mdb.execute('SELECT mycol FROM iwontpersist;').assertDataResultMatch([(1,)])

    prt2 = freeport()
    cmd = ['mserver5', '--in-memory', '--set', 'mapi_port=%d' % (prt2,)]
    mserver_process2 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    wait_server_to_start(prt2, mserver_process2)
    with SQLTestCase() as mdb:
        mdb.connect(database=None, port=prt2, username="monetdb", password="monetdb")
        mdb.execute('SELECT mycol FROM iwontpersist;').assertFailed(err_code="42S02", err_message="SELECT: no such table 'iwontpersist'")
finally:
    if mserver_process1 is not None:
        mserver_process1.terminate()
    if mserver_process2 is not None:
        mserver_process2.terminate()
