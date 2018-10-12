import os, sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

def try_remove_file(): # maybe file locks would do better
    try:
        os.remove("/tmp/testing-dump.lz4")
    except:
        pass

try_remove_file()

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

c = process.client('sql', stdin = open(os.path.join(os.getenv('TSTSRCDIR'), 'lz4-dump.sql')),
                   stdout = process.PIPE, stderr = process.PIPE, log = True)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

try_remove_file()
