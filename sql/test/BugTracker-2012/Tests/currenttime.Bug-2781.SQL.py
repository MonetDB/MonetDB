import time, os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

def main():
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client('sql', user = 'monetdb', passwd = 'monetdb',
                         stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    currenttime = time.strftime('%H:%M:%S', time.localtime(time.time()))
    #SQL command for checking the localtime
    sqlcommand = "select localtime() between (time '%s' - interval '20' second) and (time '%s' + interval '20' second);" % (currenttime, currenttime)
    out, err = clt.communicate(sqlcommand)
    sys.stdout.write(out)
    sys.stderr.write(err)

main()
