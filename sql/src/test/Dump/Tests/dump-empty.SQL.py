import sys
from MonetDBtesting import process

p = process.client('sqldump', stdout = process.PIPE)
dump, err = p.communicate()
sys.stdout.write(dump)
