import os
from MonetDBtesting import process

c = process.client('sql',
                   stdin = open(os.path.join(os.getenv('TSTSRCDIR'),
                                             os.path.pardir,
                                             'dumping_tables.SF-2776908.sql')),
                   log = True)
c.communicate()
