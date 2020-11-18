###
# Create four users.
# Drop four users.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # use default connection
    tc.execute("CREATE SCHEMA newSchema").assertSucceeded()
    tc.execute(\
            """select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id""")\
            .assertSucceeded()\
            .assertValue(0, 0, 'monetdb')\
            .assertValue(1, 0, '.snapshot')
    tc.execute("CREATE USER user1 with password '1' name '1st user' schema newSchema").assertSucceeded()
    tc.execute("CREATE USER user2 with password '2' name '2nd user' schema newSchema").assertSucceeded()
    tc.execute("CREATE USER user3 with password '3' name '3rd user' schema newSchema").assertSucceeded()
    tc.execute("CREATE USER user4 with password '4' name '4th user' schema newSchema").assertSucceeded()
    res = tc.execute("""select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id""")
    res.assertValue(2, 0, 'user1')
    res.assertValue(3, 0, 'user2')
    res.assertValue(4, 0, 'user3')
    res.assertValue(5, 0, 'user4')
    tc.execute("DROP USER user1").assertSucceeded()
    tc.execute("DROP USER user2").assertSucceeded()
    tc.execute("DROP USER user3").assertSucceeded()
    tc.execute("DROP USER user4").assertSucceeded()
    tc.execute("""select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id""").assertSucceeded().assertRowCount(2)



#import os, sys
#import pymonetdb
#
#def error(msg):
#    print(msg)
#    sys.exit(-1)
#
#db=os.getenv("TSTDB")
#port=int(os.getenv("MAPIPORT"))
#client = pymonetdb.connect(database=db, port=port, autocommit=True, user='monetdb', password='monetdb')
#cursor = client.cursor()
#
#cursor.execute("CREATE SCHEMA newSchema");
#
#cursor.execute("""select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id""")
#users=cursor.fetchall()
#if (users[0][0] != 'monetdb'):
#    error('MonetDB user missing')
#if (users[1][0] != '.snapshot'):
#    error('.snapshot user missing')
#
#cursor.execute("CREATE USER user1 with password '1' name '1st user' schema newSchema");
#cursor.execute("CREATE USER user2 with password '2' name '2nd user' schema newSchema");
#cursor.execute("CREATE USER user3 with password '3' name '3rd user' schema newSchema");
#cursor.execute("CREATE USER user4 with password '4' name '4th user' schema newSchema");
#
#cursor.execute("""select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id""")
#users=cursor.fetchall()
#if (users[2][0] != 'user1'):
#    error('user1 user missing')
#if (users[3][0] != 'user2'):
#    error('user2 user missing')
#if (users[4][0] != 'user3'):
#    error('user3 user missing')
#if (users[5][0] != 'user4'):
#    error('user4 user missing')
#
#cursor.execute("DROP USER user1")
#cursor.execute("DROP USER user2")
#cursor.execute("DROP USER user3")
#cursor.execute("DROP USER user4")
#
#cursor.execute("""select u.name, u.fullname, s.name from "sys"."users" u left outer join "sys"."schemas" s on u.default_schema = s.id""")
#if len(cursor.fetchall()) != 2:
#    error('users not correctly dropped')
#
#cursor.close()
#client.close()
