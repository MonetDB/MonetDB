import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

sys.stdout.flush()              # just to be sure
p = process.client('sqldump', stderr = process.PIPE)
out, err = p.communicate()
sys.stderr.write(err)
