import os
from MonetDBtesting import process

c = process.client('sql',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             os.path.pardir,
                                             'insert-null-byte.sql')))
c.communicate()
