import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

p = process.client('sqldump', stdout = process.PIPE, stderr = process.PIPE)
out, err = p.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
