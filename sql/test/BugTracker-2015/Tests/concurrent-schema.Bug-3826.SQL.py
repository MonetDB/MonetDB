import pymonetdb, time, threading, os

def monetSchema(tbl, host = os.getenv('MAPIHOST', 'localhost'),
                port = int(os.getenv('MAPIPORT', '50000')),
                database = os.getenv('TSTDB', 'demo'),
                username = 'monetdb', password = 'monetdb'):
    dbh = pymonetdb.connect(hostname = host, port = port,
                            database = database, username = username,
                            password = password, autocommit = True)
    cursor = dbh.cursor();
    drop = 'drop table %s' % tbl
    create = 'create table %s (' \
             'p01000 char(4) not null, ' \
             'p01001 CHAR(2) NOT NULL, ' \
             'p01002 CHAR(2) NOT NULL, ' \
             'p01003 CHAR(2) NOT NULL, ' \
             'p01004 CHAR(8) NOT NULL, ' \
             'p01005 CHAR(8) NOT NULL, ' \
             'p01006 CHAR(1) NOT NULL, ' \
             'p01007 CHAR(1) NOT NULL, ' \
             'p01008 CHAR(8) NOT NULL, ' \
             'p01009 CHAR(8) NOT NULL, ' \
             'p01010 CHAR(8) NOT NULL, ' \
             'p01011 CHAR(8) NOT NULL, ' \
             'p01012 CHAR(6) NOT NULL, ' \
             'p01013 CHAR(7) NOT NULL, ' \
             'p01014 CHAR(2) NOT NULL, ' \
             'p01015 CHAR(3) NOT NULL, ' \
             'p01016 CHAR(8) NOT NULL, ' \
             'p01017 CHAR(10) NOT NULL, ' \
             'p01018 CHAR(3) NOT NULL, ' \
             'p01019 DECIMAL(11,3) NOT NULL)' % tbl
    for i in range(1000):
        for j in range(100):
            try:
                cursor.execute(drop)
            except pymonetdb.OperationalError as e1:
                if 'no such table' in e1.args[0]:
                    break
            except pymonetdb.ProgrammingError as e2:
                pass
            else:
                break
            time.sleep(0.5)
        for j in range(100):
            try:
                cursor.execute(create)
            except pymonetdb.OperationalError as e1:
                if 'already in use' in e1.args[0]:
                    break
            except pymonetdb.ProgrammingError as e2:
                pass
            else:
                break
            time.sleep(0.5)

class Client(threading.Thread):
    def __init__(self, tbl, host = os.getenv('MAPIHOST', 'localhost'),
                 port = int(os.getenv('MAPIPORT', '50000')),
                 database = os.getenv('TSTDB', 'demo'),
                 username = 'monetdb', password = 'monetdb'):
        self.__tbl = tbl
        self.__host = host
        self.__port = port
        self.__database = database
        self.__username = username
        self.__password = password
        threading.Thread.__init__(self)

    def run(self):
        monetSchema(self.__tbl, host = self.__host, port = self.__port,
                    database = self.__database, username = self.__username,
                    password = self.__password)

c1 = Client('sys.table301')
c2 = Client('sys.table302')
c1.start()
c2.start()
c1.join()
c2.join()
