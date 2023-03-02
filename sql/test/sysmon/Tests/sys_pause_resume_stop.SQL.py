import multiprocessing as mp
import os, time
from pymonetdb.exceptions import OperationalError
from MonetDBtesting.sqltest import SQLTestCase

h   = os.getenv('MAPIHOST')
p   = int(os.getenv('MAPIPORT'))
db  = os.getenv('TSTDB')

init    =   '''
            create schema s1;
            create user u1 with password \'u1\' name \'u1\' schema s1;
            create user u2 with password \'u2\' name \'u2\' schema s1;
            create or replace function sleep(msecs int) returns int external name alarm.sleep;
            grant execute on function sleep(int) to u1;
            grant execute on function sleep(int) to u2;
            '''

p_proced  = '''
            create or replace procedure pause_sleep()
            begin
            declare t int;
            set t = (select tag from queue(\'ALL\') where query like \'select sleep%\');
            call sys.pause(t,\'u1\');
            end;
            '''

r_proced  = '''
            create or replace procedure resume_sleep()
            begin
            declare t int;
            set t = (select tag from queue(\'ALL\') where query like \'select sleep%\');
            call sys.resume(t,\'u1\');
            end;
            '''

s_proced  = '''
            create or replace procedure stop_sleep()
            begin
            declare t int;
            set t = (select tag from queue(\'ALL\') where query like \'select sleep%\' and status = \'running\');
            call sys.stop(t,\'u1\');
            end;
            '''

u1_qry  =   '''
            select sleep(2000);
            '''

st_paused = '''
            select status from queue(\'u1\') where query like \'select sleep%\' and status = \'paused\';
            '''

st_aborted = '''
             select status from queue(\'u2\') where query like \'select sleep%\';
             '''

st_aborted2 = '''
             select status from queue() where query like \'select sleep%\';
             '''
# and status = \'aborted\';
def execute_qry_1():
    with SQLTestCase() as client:
        client.connect(username = 'u1', password = 'u1')
        try:
            client.execute(u1_qry)
        except OperationalError as e:
            print(e)
            exit(1)

def execute_qry_2():
    with SQLTestCase() as client:
        client.connect(username = 'u2', password = 'u2')
        try:
            client.execute(u1_qry)
            client.execute(st_aborted2).assertSucceeded().assertDataResultMatch([('aborted',)])
        except OperationalError as e:
            print(e)
            exit(1)

if __name__ == '__main__':
    with SQLTestCase() as mdb:
        mdb.connect(username="monetdb", password="monetdb")
        mdb.execute(init).assertSucceeded()
        mdb.execute(p_proced).assertSucceeded()
        mdb.execute(r_proced).assertSucceeded()
        mdb.execute(s_proced).assertSucceeded()

        client_proc1 = mp.Process(target=execute_qry_1)
        client_proc1.start()

        time.sleep(1)
        mdb.execute('call pause_sleep();').assertSucceeded()
        mdb.execute(st_paused).assertSucceeded().assertDataResultMatch([('paused',)])
        mdb.execute('call resume_sleep();').assertSucceeded()
        client_proc1.join()

        client_proc2 = mp.Process(target=execute_qry_2)
        client_proc2.start()
        time.sleep(1)
        mdb.execute('call stop_sleep();').assertSucceeded()
        mdb.execute(st_aborted).assertSucceeded().assertDataResultMatch([('stopping',)])
        client_proc2.join()

        mdb.execute('drop function sleep;').assertSucceeded()
        mdb.execute('drop procedure pause_sleep;').assertSucceeded()
        mdb.execute('drop procedure resume_sleep;').assertSucceeded()
        mdb.execute('drop procedure stop_sleep;').assertSucceeded()
        mdb.execute('drop user u1;').assertSucceeded()
        mdb.execute('drop user u2;').assertSucceeded()
        mdb.execute('drop schema s1;').assertSucceeded()
