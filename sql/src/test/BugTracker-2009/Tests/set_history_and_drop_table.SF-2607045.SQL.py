import os, sys


def main():
    dir = os.getenv('TSTSRCDIR')
    clcmd = str(os.getenv('SQL_CLIENT'))
    sys.stdout.write('Load history\n')
    clt1 = os.popen(clcmd + "<%s" % ('%s/../../../sql/history.sql' % dir), 'w')
    clt1.close()
    sys.stdout.write('Run test\n')
    clt1 = os.popen(clcmd + "<%s" % ('%s/../set_history_and_drop_table.SF-2607045.sql' % dir), 'w')
    clt1.close()
    sys.stdout.write('Drop history\n')
    clt1 = os.popen(clcmd + "<%s" % ('%s/../drop_history.sql' % dir), 'w')
    clt1.close()

main()
