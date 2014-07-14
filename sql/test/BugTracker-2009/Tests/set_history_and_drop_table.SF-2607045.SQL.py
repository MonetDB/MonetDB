import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

d = os.getenv('TSTSRCDIR')

sys.stdout.write('Run test\n')
c = process.client('sql',
                   args = [os.path.join(d,os.pardir,'set_history_and_drop_table.SF-2607045.sql')],
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

sys.stdout.write('Drop history\n')
c = process.client('sql',
                   args = [os.path.join(d,os.pardir,'drop_history.sql')],
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
