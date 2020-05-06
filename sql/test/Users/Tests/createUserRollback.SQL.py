import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate('''
        START TRANSACTION;

        CREATE USER "1" WITH PASSWORD '1' NAME '1' SCHEMA "sys";
        CREATE SCHEMA "ups" AUTHORIZATION "1";
        ALTER USER "1" SET SCHEMA "ups";

        select * from sys.db_users; --error, doesn't exist

        COMMIT; --it will rollback
    ''')
    sys.stdout.write(out)
    sys.stderr.write(err)

with process.client('sql',user='1',passwd='1', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c: # error should not be possible to login
    out, err = c.communicate('SELECT 1;')
    sys.stdout.write(out)
    sys.stderr.write(err)
