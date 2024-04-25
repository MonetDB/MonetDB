import multiprocessing as mp
import os, time
from pymonetdb.exceptions import OperationalError
from MonetDBtesting.sqltest import SQLTestCase

h   = os.getenv('MAPIHOST')
p   = int(os.getenv('MAPIPORT'))
db  = os.getenv('TSTDB')

init    =   ''' create schema s1;
            create user u1 with password \'u1\' name \'u1\' schema s1;
            create user u2 with password \'u2\' name \'u2\' schema s1;
            create or replace function sleep(msecs int) returns int external name alarm.sleep;
            grant execute on function sleep(int) to u1;
            grant execute on function sleep(int) to u2;

            create or replace procedure pauseid(id string, u string) begin
            declare t int;
            set t = (select tag from queue(\'ALL\') where query like \'/*\'||id||\'*/%\');
            call sys.pause(t, u);
            end;
            create or replace procedure resumeid(id string, u string) begin
            declare t int;
            set t = (select tag from queue(\'ALL\') where query like \'/*\'||id||\'*/%\');
            call sys.resume(t, u);
            end;
            create or replace procedure stopid(id string, u string) begin
            declare t int;
            set t = (select tag from queue(\'ALL\') where query like \'/*\'||id||\'*/%\');
            call sys.stop(t, u);
            end; '''

status1 = 'select status from queue(\'u1\') where query like \'/*id1*/%\';'
status2 = 'select status from queue(\'u1\') where query like \'/*id2*/%\';'

def execute_qry(user, pw, qid):
    with SQLTestCase() as client:
        client.connect(username = user, password = pw)
        try:
            client.execute(qid + 'select sleep(2000);')
        except OperationalError as e:
            print(e)
            exit(1)

if __name__ == '__main__':
    with SQLTestCase() as mdb:
        mdb.connect(username='monetdb', password='monetdb')
        mdb.execute(init).assertSucceeded()

        client_proc1 = mp.Process(target=execute_qry, args=('u1', 'u1', '/*id1*/',))
        client_proc1.start()
        time.sleep(1)
        mdb.execute('call pauseid(\'id1\', \'u1\');').assertSucceeded()
        mdb.execute(status1).assertSucceeded().assertDataResultMatch([('paused',)])
        mdb.execute('call resumeid(\'id1\', \'u1\');').assertSucceeded()
        time.sleep(2)
        mdb.execute(status1).assertSucceeded().assertDataResultMatch([('finished',)])
        client_proc1.join()

        client_proc2 = mp.Process(target=execute_qry, args=('u1', 'u1', '/*id2*/',))
        client_proc2.start()
        time.sleep(1)
        mdb.execute('call stopid(\'id2\', \'u1\');').assertSucceeded()
        time.sleep(2)
        mdb.execute(status2).assertSucceeded().assertDataResultMatch([('aborted',)])
        client_proc2.join()

        mdb.execute('drop function sleep;').assertSucceeded()
        mdb.execute('drop procedure pauseid;').assertSucceeded()
        mdb.execute('drop procedure resumeid;').assertSucceeded()
        mdb.execute('drop procedure stopid;').assertSucceeded()
        mdb.execute('drop user u1;').assertSucceeded()
        mdb.execute('drop user u2;').assertSucceeded()
        mdb.execute('drop schema s1;').assertSucceeded()
