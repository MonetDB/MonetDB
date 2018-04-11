from __future__ import print_function

import pymonetdb
import os

import logging
logging.basicConfig()


dbh = pymonetdb.connect(database = os.environ['TSTDB'],
                        port = int(os.environ['MAPIPORT']),
                        hostname = "localhost")

cursor = dbh.cursor()

q = """

create procedure setmallocsuccesscount(count BIGINT)
    external name "io"."setmallocsuccesscount";

call setmallocsuccesscount(%d);
SELECT * FROM tables;
"""
i = 3000

while i > 1000:
    print(i)
    i-=1
    try:
        cursor.execute(q % (i))
    except Exception as e:
      # print(e)
        pass
    finally:
        dbh.rollback()
