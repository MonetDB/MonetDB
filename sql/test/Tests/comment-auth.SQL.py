import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

# As super user, create users and schemas owned by these users.
with process.client('sql',
                    stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                              'comment-auth-superuser.sql')),
                    stdout = process.PIPE, stderr = process.PIPE) as c:
    out, err = c.communicate()
    if re.search(r'^[^#\n]', out, re.M):
        sys.stdout.write(out)
    if re.search(r'^[^#\n]', err, re.M):
        sys.stderr.write(err)

dump = '''\
SCHEMA schema_a 'set by super user'
SCHEMA schema_b 'set by super user'
SCHEMA schema_a 'set by user_a'
SCHEMA schema_b 'set by super user'
'''
edump = r'''\
MAPI = (user_a) /var/tmp/mtest-285315/.s.monetdb.\d+
QUERY = COMMENT ON SCHEMA schema_b IS 'set by user_a';
ERROR = !COMMENT ON: insufficient privileges for user 'user_a' in schema 'schema_b'
CODE = 42000
'''
# As one of the users, check that we can only comment on our own objects.
with process.client('sql',
                    user='user_a', passwd='user_a',
                    stdin=open(os.path.join(os.getenv('TSTSRCDIR'),
                                              'comment-auth-a.sql')),
                    echo=False,
                    stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate()
    out = ' '.join(re.split('[ \t]+', out))
    if out != dump:
        sys.stdout.write(out)
    err = ' '.join(re.split('[ \t]+', err))
    if re.match(edump, err) is not None:
        sys.stderr.write(err)
