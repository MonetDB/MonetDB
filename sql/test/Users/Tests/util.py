import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def sql_test_client(user, passwd, input):
    process.client(lang='sql', user = user, passwd = passwd, communicate = True,
                   stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE,
                   input = input, port = int(os.getenv('MAPIPORT')))
