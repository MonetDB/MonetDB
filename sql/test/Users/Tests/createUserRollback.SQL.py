import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate('''
        CREATE TABLE sys.myvar (c BIGINT);
        INSERT INTO sys.myvar VALUES ((SELECT COUNT(*) FROM sys.users));

        START TRANSACTION;
        CREATE USER "1" WITH PASSWORD '1' NAME '1' SCHEMA "sys";
        ROLLBACK;

        SELECT CAST(COUNT(*) - (SELECT c FROM sys.myvar) AS BIGINT) FROM sys.users; -- The MAL authorization is not transaction aware, so the count changes :/
        DROP TABLE sys.myvar;
    ''')
    sys.stdout.write(out)
    sys.stderr.write(err)

with process.client('sql',user='1',passwd='1', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c: # error should not be possible to login
    out, err = c.communicate('SELECT 1;')
    sys.stdout.write(out)
    sys.stderr.write(err)
