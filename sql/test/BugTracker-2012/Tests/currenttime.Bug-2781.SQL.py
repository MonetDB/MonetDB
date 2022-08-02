import time, os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

def main():
    if time.daylight and time.localtime(time.time()).tm_isdst:
        zone = time.altzone
    else:
        zone = time.timezone
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    with process.client('sql', user='monetdb', passwd='monetdb',
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as clt:
        currenttime = time.strftime('%F %T', time.localtime(time.time()))
        #SQL command for checking the localtime
        sqlcommand = "select localtimestamp() between (timestamp '%s' - interval '20' second) and (timestamp '%s' + interval '20' second);" % (currenttime, currenttime)
        out, err = clt.communicate(sqlcommand)
        sys.stdout.write(out)
        sys.stderr.write(err)
    with process.client('sql', user='monetdb', passwd='monetdb',
                         stdin=process.PIPE,
                         stdout=process.PIPE, stderr=process.PIPE) as clt:
        out, err = clt.communicate('select localtimestamp();')
        sys.stdout.write('#Python says: %s; current time zone %d\n' % (currenttime, zone))
        for line in out.split('\n'):
            if line:
                sys.stdout.write('#MonetDB says: %s\n' % line)

main()
