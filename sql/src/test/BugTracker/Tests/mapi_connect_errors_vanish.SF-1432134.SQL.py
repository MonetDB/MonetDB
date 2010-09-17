from MonetDBtesting import process

c = process.client('sql', user = 'invalid', passwd = 'invalid',
                   stdin = process.PIPE)
c.communicate()
