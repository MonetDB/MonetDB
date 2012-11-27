import time, os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process


currenttime = time.strftime('%H:%M:%S', time.localtime(time.time()))

#print currenttime

#Change the current time in the sql file

sqlfile = open("currenttime.Bug-2791.sql","w")
sqlfile.write("select (localtime() - time '%s' < time '00:00:20') and (time '%s' - localtime() < time '00:00:20');" %(currenttime, currenttime))
sqlfile.close()


def server_start():
    sys.stderr.write('#mserver\n')
    sys.stderr.flush()
    srv = process.server(stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(lang, file, user = 'monetdb', passwd = 'monetdb'):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client(lang, user = user, passwd = passwd,
                         stdin = open(file),
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate()

def main():
    srv = server_start()
    out, err = client('sql',
                      os.path.join(os.getenv('RELSRCDIR'),
                                   'currenttime.Bug-2791.sql'))
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

main()
