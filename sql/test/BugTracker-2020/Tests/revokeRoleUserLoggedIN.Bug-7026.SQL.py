###
# Assess that when the role of a user, who is currently logged in and has
#   assumed that role, has been revoked, the user immedately lose all
#   privileges associated with that role.
###

import sys, time, pymonetdb, os

def connect(username, password):
    return pymonetdb.connect(database = os.getenv('TSTDB'),
                             hostname = 'localhost',
                             port = int(os.getenv('MAPIPORT')),
                             username = username,
                             password = password,
                             autocommit = True)

def query(conn, sql):
    print(sql)
    cur = conn.cursor()
    try:
        cur.execute(sql)
    except pymonetdb.OperationalError as e:
        print("!", e)
        return
    r = cur.fetchall()
    cur.close()
    print(r)

def run(conn, sql):
    print(sql)
    try:
        r = conn.execute(sql)
    except pymonetdb.OperationalError as e:
        print("!", e)
        return
    print('# OK')


c1 = connect('monetdb', 'monetdb')
# Create a user, schema and role
run(c1, 'CREATE SCHEMA s1;')
run(c1, 'CREATE USER bruce WITH PASSWORD \'bruce\' name \'willis\' schema s1;')
run(c1, 'CREATE TABLE s1.test(d int);')
run(c1, 'CREATE ROLE role1;')
run(c1, 'GRANT ALL ON s1.test to role1;')
run(c1, 'GRANT role1 TO bruce;')

# Login as `bruce` and use `role1`
c2 = connect('bruce', 'bruce')
run(c2, 'SET role role1;')
run(c2, 'INSERT INTO test VALUES (24), (42);')
run(c2, 'UPDATE test SET d = 42 WHERE d <> 42;')
run(c2, 'DELETE FROM test WHERE d = 42;')
query(c2, 'SELECT * FROM test;')

# Revoke `role1` from `bruce`
run(c1, 'REVOKE role1 FROM bruce;')

# `bruce` should not be able to access `test` again:
run(c2, 'INSERT INTO test VALUES (24), (42);')
run(c2, 'UPDATE test SET d = 42 WHERE d <> 42;')
run(c2, 'DELETE FROM test WHERE d = 42;')
query(c2, 'SELECT * FROM test;')
query(c2, 'SET role role1; -- verifies role1 is gone')

c2.close()
run(c1, 'DROP USER bruce;')
run(c1, 'DROP ROLE role1;')
run(c1, 'DROP SCHEMA s1 CASCADE;')
c1.close()
