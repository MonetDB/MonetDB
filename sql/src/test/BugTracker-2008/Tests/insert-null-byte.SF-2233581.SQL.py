import os
from MonetDBtesting import process

c = process.client('sql',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             os.path.pardir,
                                             'insert-null-byte.sql')))
c.communicate()

c = process.client('sql', stdin = process.PIPE)
c.communicate('drop table strings2233581;')
