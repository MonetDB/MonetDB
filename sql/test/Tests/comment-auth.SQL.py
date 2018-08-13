import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

# As super user, create users and schemas owned by these users.
c = process.client('sql',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             'comment-auth-superuser.sql')),
                   stdout = process.PIPE, stderr = process.PIPE,
                   log = True)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

# As one of the users, check that we can only comment on our own objects.
c = process.client('sql',
                   user='user_a', passwd='user_a',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             'comment-auth-a.sql')),
                   stdout = process.PIPE, stderr = process.PIPE,
                   log = True)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
