import os, sys
from MonetDBtesting import process

def main():
    dir = os.getenv('TSTSRCDIR')
    clcmd = str(os.getenv('SQL_CLIENT'))
    sys.stdout.write('Run test\n')
    clt1 = process.client('sql', args = [os.path.join(dir,'..','set_history_and_drop_table.SF-2607045.sql')], stdout = process.PIPE)
    out, err = clt1.communicate()
    sys.stdout.write(out)
    sys.stdout.write('Drop history\n')
    clt1 = process.client('sql', args = [os.path.join(dir,'..','drop_history.sql')], stdout = process.PIPE)
    out, err = clt1.communicate()
    sys.stdout.write(out)

main()
