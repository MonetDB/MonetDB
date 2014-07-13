import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

c = process.client('sql',
                   args = ['-s', '''CREATE USER "testuser" WITH ENCRYPTED PASSWORD \'e9e633097ab9ceb3e48ec3f70ee2beba41d05d5420efee5da85f97d97005727587fda33ef4ff2322088f4c79e8133cc9cd9f3512f4d3a303cbdb5bc585415a00\' NAME \'Test User\' SCHEMA "sys";
CREATE SCHEMA "testschema" AUTHORIZATION "testuser";
ALTER USER "testuser" SET SCHEMA "testschema"'''],
                   stdout = process.PIPE,
                   stderr = process.PIPE)
out, err = c.communicate()
if out:
    sys.stdout.write(out)
if err:
    sys.stderr.write(err)

c = process.client('sql',
                   args = ['-s', 'CREATE FUNCTION rad(d DOUBLE) RETURNS DOUBLE BEGIN RETURN d * PI() / 180; END'],
                   user = 'testuser', passwd = 'testpassword',
                   stdout = process.PIPE,
                   stderr = process.PIPE)
out, err = c.communicate()
if out:
    sys.stdout.write(out)
if err:
    sys.stderr.write(err)

clients = []
for i in range(50):
    c = process.client('sql', args = ['-s', 'SELECT rad(55.81689)'],
                       user = 'testuser', passwd = 'testpassword',
                       stdout = process.PIPE, stderr = process.PIPE)
    clients.append(c)

for c in clients:
    out, err = c.communicate()
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)

c = process.client('sql',
                   args = ['-s', 'DROP FUNCTION rad'],
                   user = 'testuser', passwd = 'testpassword',
                   stdout = process.PIPE,
                   stderr = process.PIPE)
out, err = c.communicate()
if out:
    sys.stdout.write(out)
if err:
    sys.stderr.write(err)

# undo damage
c = process.client('sql',
                   args = ['-s', '''ALTER USER "testuser" SET SCHEMA "sys";
DROP SCHEMA "testschema";
DROP USER "testuser"'''],
                   stdout = process.PIPE,
                   stderr = process.PIPE)
out, err = c.communicate()
if out:
    sys.stdout.write(out)
if err:
    sys.stderr.write(err)
