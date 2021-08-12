from MonetDBtesting.sqltest import SQLTestCase
import random
import sys

with SQLTestCase() as tc:
    tc.execute("drop table if exists sitest;").assertSucceeded()
    tc.execute("create table sitest(id serial, value int not null);").assertSucceeded()
    # tc.execute('prepare insert into sitest ("value") values (?);').assertSucceeded()
    stmts = ['prepare insert into sitest ("value") values (?);']
    for i in range(100000):
        stmts.append(f'exec 0({random.randint(1000, pow(2, 31))});')
    tc.execute("\n".join(stmts))
    tc.execute("select count(*) from sitest;").assertSucceeded().assertDataResultMatch([(100000)])

