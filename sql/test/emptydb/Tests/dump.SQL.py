try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sqldump', stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate()

for line in out.splitlines():
    if line.startswith('--') or not line:
        continue
    if line != 'START TRANSACTION;' and line != 'COMMIT;':
        # unexpected output, dump it all
        print(out)
        break
