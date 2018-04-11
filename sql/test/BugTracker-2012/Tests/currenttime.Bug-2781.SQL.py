import time, os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

def main():
    if time.daylight and time.gmtime(time.time()).tm_isdst:
        zone = time.altzone
    else:
        zone = time.timezone
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
    clt = process.client('sql', user = 'monetdb', passwd = 'monetdb',
                         stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    out, err = clt.communicate('select localtime();')
    sys.stdout.write('#Python says: %s; current time zone %d\n' % (currenttime, zone))
    for line in out.split('\n'):
        if line:
            sys.stdout.write('#MonetDB says: %s\n' % line)

main()
