import time, os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_start():
    sys.stderr.write('#mserver\n')
    sys.stderr.flush()
    srv = process.server(stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(lang, user = 'monetdb', passwd = 'monetdb'):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client(lang, user = user, passwd = passwd,
                         stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    currenttime = time.strftime('%H:%M:%S', time.localtime(time.time()))
    #SQL command for checking the localtime
    sqlcommand = "select (localtime() - time '%s' < time '00:00:20') and (time '%s' - localtime() < time '00:00:20');" % (currenttime, currenttime)
    return clt.communicate(sqlcommand)

def main():
    srv = server_start()
    out, err = client('sql')

    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

main()
