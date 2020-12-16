import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

# As super user, create users and schemas owned by these users.

SUPERUSER_SCRIPT = """
CREATE USER user_a WITH PASSWORD 'user_a' NAME 'User A' SCHEMA sys;
CREATE USER user_b WITH PASSWORD 'user_b' NAME 'User B' SCHEMA sys;
CREATE ROLE role_b;
GRANT role_b to user_b;

CREATE SCHEMA schema_a AUTHORIZATION user_a;
CREATE SCHEMA schema_b AUTHORIZATION role_b;

CREATE TABLE schema_a.tab_a(i INTEGER);
CREATE TABLE schema_b.tab_b(i INTEGER);

COMMENT ON SCHEMA schema_a IS 'set by super user';
COMMENT ON SCHEMA schema_b IS 'set by super user';
"""

with process.client('sql',
                    stdin = process.PIPE,
                    stdout = process.PIPE, stderr = process.PIPE) as c:
    out, err = c.communicate(SUPERUSER_SCRIPT)
    if re.search(r'^[^#\n]', out, re.M):
        sys.stdout.write(out)
        sys.exit(1)
    if re.search(r'^[^#\n]', err, re.M):
        sys.stderr.write(err)
        sys.exit(1)


USER_A_SCRIPT = r"""
-- can we see the comments set by the super user?
\dn

-- we cannot change comments on objects we don't own
COMMENT ON SCHEMA schema_b IS 'set by user_a';

-- but we can comment on our own stuff
COMMENT ON SCHEMA schema_a IS 'set by user_a';
\dn
"""

# As one of the users, check that we can only comment on our own objects.

USER_A_STDOUT = '''\
SCHEMA schema_a 'set by super user'
SCHEMA schema_b 'set by super user'
SCHEMA schema_a 'set by user_a'
SCHEMA schema_b 'set by super user'
'''

USER_A_STDERR = r'''\
MAPI = (user_a) /var/tmp/mtest-285315/.s.monetdb.\d+
QUERY = COMMENT ON SCHEMA schema_b IS 'set by user_a';
ERROR = !COMMENT ON: insufficient privileges for user 'user_a' in schema 'schema_b'
CODE = 42000
'''

with process.client('sql',
                    user='user_a', passwd='user_a',
                    stdin=process.PIPE,
                    echo=False,
                    stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(USER_A_SCRIPT)
    out = ' '.join(re.split('[ \t]+', out))
    if out != USER_A_STDOUT:
        sys.stdout.write(out)
        sys.exit(1)
    err = ' '.join(re.split('[ \t]+', err))
    if re.match(USER_A_STDERR, err) is not None:
        sys.stderr.write(err)
        sys.exit(1)
