import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

c = process.client('sql', stdin = process.PIPE,
                   stdout = process.PIPE, stderr = process.PIPE,
                   # these two to make client/server communication
                   # more efficient:
                   interactive = False, echo = False)
q = []
q.append(("create table bug3261 (probeid int, markername varchar(64));\n"
          "copy %d records into bug3261 from stdin using delimiters "
          "'\\t','\\n','' null as 'null';\n") % (1455 * 3916))
for i in xrange(1,1456):
    v = 'rmm%d' % i
    for j in xrange(3916):
        q.append('%d\t%s\n' % (j, v))
out, err = c.communicate(''.join(q))
sys.stdout.write(out)
sys.stderr.write(err)
c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('select * from bug3261 where probeid = 1234 limit 10;\n')
sys.stdout.write(out)
sys.stderr.write(err)
c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('drop table bug3261;\n')
sys.stdout.write(out)
sys.stderr.write(err)
