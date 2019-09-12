# skipif <system>
# onlyif <system>

# statement (ok|error)
# query (I|T|R)+ (nosort|rowsort|valuesort)? [arg]
#       I: integer; T: text (string); R: float
#       nosort: do not sort
#       rowsort: sort rows
#       valuesort: sort individual values
# hash-threshold number
# halt

import pymonetdb
import hashlib
import re
import getopt
import sys

port = 50000
db = "demo"
hostname = 'localhost'

opts, args = getopt.getopt(sys.argv[1:], '', ['host=', 'port=', 'database='])
for o, a in opts:
    if o == '--host':
        hostname = a
    elif o == '--port':
        port = int(a)
    elif o == '--database':
        db = a

skipidx = re.compile(r'create index .* \b(asc|desc)\b', re.I)

class SQLLogicSyntaxError(Exception):
    pass

class SQLLogic:
    def __init__(self):
        pass

    def connect(self, username='monetdb', password='monetdb',
                hostname='localhost', port=None, database='demo'):
        self.__dbh = pymonetdb.connect(username=username,
                                       password=password,
                                       hostname=hostname,
                                       port=port,
                                       database=database,
                                       autocommit=True)
        self.__crs = self.__dbh.cursor()

    def drop(self):
        self.command('select name from tables where not system')
        for row in self.__crs.fetchall():
            self.command('drop table %s cascade' % row[0])

    def command(self, cmd):
        return self.__crs.execute(cmd)

    def exec_statement(self, statement, expectok):
        if skipidx.search(statement) is not None:
            # skip creation of ascending or descending index
            return
        try:
            self.command(statement)
        except pymonetdb.DatabaseError:
            if not expectok:
                return
        else:
            if expectok:
                return
        self.query_error(statement, "statement didn't give expected result", expectok and "statement was expected to succeed but didn't" or "statement was expected to fail bat didn't")

    def convertresult(self, query, columns, data):
        ndata = []
        for row in data:
            if len(row) != len(columns):
                self.query_error(query, 'wrong number of columns received')
                return None
            nrow = []
            for i in range(len(columns)):
                if row[i] is None or row[i] == 'NULL':
                    nrow.append('NULL')
                elif columns[i] == 'I':
                    if row[i] == 'true':
                        nrow.append('1')
                    elif row[i] == 'false':
                        nrow.append('0')
                    else:
                        nrow.append('%d' % row[i])
                elif columns[i] == 'T':
                    if row[i] == '':
                        nrow.append('(empty)')
                    else:
                        nval = []
                        for c in str(row[i]):
                            if ' ' <= c <= '~':
                                nval.append(c)
                            else:
                                nval.append('@')
                        nrow.append(''.join(nval))
                elif columns[i] == 'R':
                    nrow.append('%.3f' % row[i])
                else:
                    raise SQLLogicSyntaxError('incorrect column type indicator')
            ndata.append(tuple(nrow))
        return ndata

    def query_error(self, query, message, exception=None):
        print(message)
        if exception:
            print(exception.rstrip('\n'))
        print("query started on line %d fo file %s" % (self.qline, self.__name))
        print("query text:")
        print(query)
        print('')

    def exec_query(self, query, columns, sorting, args, nresult, hash, expected):
        try:
            rows = self.command(query)
        except pymonetdb.DatabaseError as e:
            self.query_error(query, 'query failed', e.args[0])
            return
        if rows * len(columns) != nresult:
            self.query_error(query, 'wrong number of rows received')
            return
        data = self.__crs.fetchall()
        data = self.convertresult(query, columns, data)
        if data is None:
            return
        m = hashlib.md5()
        i = 0
        if sorting == 'valuesort':
            ndata = []
            for row in data:
                for col in row:
                    ndata.append(col)
            ndata.sort()
            for col in ndata:
                if expected is not None:
                    if col != expected[i]:
                        self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]))
                    i += 1
                m.update(bytes(col, encoding='ascii'))
                m.update(b'\n')
        else:
            if sorting == 'rowsort':
                data.sort()
            for row in data:
                for col in row:
                    if expected is not None:
                        if col != expected[i]:
                            self.query_error(query, 'unexpected value; received "%s", expected "%s"' % (col, expected[i]))
                        i += 1
                    m.update(bytes(col, encoding='ascii'))
                    m.update(b'\n')
        h = m.hexdigest()
        if hash is not None and h != hash:
            self.query_error(query, 'hash mismatch; received: "%s", expected: "%s"' % (h, hash))

    def initfile(self, f):
        self.__name = f
        self.__file = open(f)
        self.__line = 0

    def readline(self):
        self.__line += 1
        return self.__file.readline()

    def parse(self, f):
        self.initfile(f)
        while True:
            skipping = False
            line = self.readline()
            if not line:
                break
            line = line.split()
            if not line:
                continue
            while line[0] == 'skipif' or line[0] == 'onlyif':
                if line[0] == 'skipif' and line[1] == 'MonetDB':
                    skipping = True
                elif line[0] == 'onlyif' and line[1] != 'MonetDB':
                    skipping = True
                line = self.readline().split()
            if line[0] == 'hash-threshold':
                pass
            elif line[0] == 'statement':
                expectok = line[1] == 'ok'
                statement = []
                self.qline = self.__line + 1
                while True:
                    line = self.readline()
                    if not line or line == '\n':
                        break
                    statement.append(line.rstrip('\n'))
                if not skipping:
                    self.exec_statement('\n'.join(statement), expectok)
            elif line[0] == 'query':
                columns = line[1]
                if len(line) > 2:
                    sorting = line[2]  # nosort,rowsort,valuesort
                    args = line[3:]
                else:
                    sorting = 'nosort'
                    args = []
                query = []
                self.qline = self.__line + 1
                while True:
                    line = self.readline()
                    if not line or line == '\n' or line.startswith('----'):
                        break
                    query.append(line.rstrip('\n'))
                if not line.startswith('----'):
                    raise SQLLogicSyntaxError('---- expected')
                line = self.readline()
                if not line:
                    line = '\n'
                if 'values hashing to' in line:
                    line = line.split()
                    hash = line[4]
                    expected = None
                    nresult = int(line[0])
                else:
                    hash = None
                    expected = []
                    while line and line != '\n':
                        expected.append(line.rstrip('\n'))
                        line = self.readline()
                    nresult = len(expected)
                if not skipping:
                    self.exec_query('\n'.join(query), columns, sorting, args, nresult, hash, expected)

sql = SQLLogic()
sql.connect(hostname=hostname, port=port, database=db)

for arg in args:
    sql.drop()
    sql.parse(arg)
