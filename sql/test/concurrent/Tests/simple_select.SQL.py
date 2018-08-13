from __future__ import print_function

import pymonetdb, sys, threading, os

query = 'select 1,2;'

class Client(threading.Thread):

    def __init__(self, client):
        threading.Thread.__init__ (self)
        self.client = client
        self.dbh = pymonetdb.connect(port=int(os.getenv('MAPIPORT')),hostname=os.getenv('MAPIHOST'),database=os.getenv('TSTDB'))

    def run(self):
        cursor = self.dbh.cursor();
        cursor.execute(query)
        self.result = cursor.fetchall()

    def output(self):
        print("(%d, %s)" % (self.client, self.result))

def main():
        C = []
        C.append(Client(0))
        C.append(Client(1))
        C.append(Client(2))
        C.append(Client(3))
        C.append(Client(4))
        for t in C:
               t.start()
        for t in C:
               t.join()
        for t in C:
               t.output()

main()
