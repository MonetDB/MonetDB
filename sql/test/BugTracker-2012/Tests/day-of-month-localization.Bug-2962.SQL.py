import os, sys, time
try:
    from MonetDBtesting import process
except ImportError:
    import process

c = process.client('sql',
                   stdout = process.PIPE, stderr = process.PIPE, stdin = process.PIPE)
c.stdin.write("set time zone interval -'%d:00' hour to minute;\n" % (time.gmtime().tm_hour+1));
c.stdin.write("SELECT EXTRACT(DAY FROM CURRENT_TIMESTAMP) <> %d;\n" % (time.gmtime().tm_mday));
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
