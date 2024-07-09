
import os
import sys
from typing import List, Tuple
import pymonetdb


def connect(remark: str, **args) -> pymonetdb.Connection:
    dbname = os.environ['TSTDB']
    mapiport = os.environ['MAPIPORT']
    conn = pymonetdb.connect(dbname, port=mapiport, autocommit=True,  **args)
    with conn.cursor() as c:
        c.execute("CALL sys.setclientinfo('ClientRemark', %s)", [remark])
    return conn

def get_remarks(conn: pymonetdb.Connection) -> List[Tuple[int,str]]:
    with conn.cursor() as c:
        c.execute("SELECT sessionid, remark FROM sys.sessions ORDER BY sessionid")
        return c.fetchall()

def assert_equal(left, right):
    if left != right:
        print(f'LEFT:  {left!r}\nRIGHT: {right!r}\n', file=sys.stderr)
        assert left == right


#######################################################################
# Connect as admin

conn0 = connect('admin 0')
assert_equal(get_remarks(conn0), [(0, 'admin 0')])


#######################################################################
# Create a user

c0 = conn0.cursor()
# try:
#     c0.execute('DROP USER nonadmin')    # convenientduring interactive testing
# except pymonetdb.Error:
#     pass
c0.execute("CREATE USER nonadmin WITH PASSWORD 'na' NAME 'Not Admin' SCHEMA sys")


#######################################################################
# Connect as that user, twice

conn1 = connect('user 1', user='nonadmin', password='na')
conn2 = connect('user 2', user='nonadmin', password='na')


#######################################################################
# Check who can see what

# admin can see both
assert_equal(get_remarks(conn0), [(0, 'admin 0'), (1, 'user 1'), (2, 'user 2')])

# users can only see themselves
assert_equal(get_remarks(conn1), [(1, 'user 1'), (2, 'user 2')])
assert_equal(get_remarks(conn2), [(1, 'user 1'), (2, 'user 2')])

