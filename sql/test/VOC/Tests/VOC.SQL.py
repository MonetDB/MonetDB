import os, sys

from MonetDBtesting.sqltest import SQLTestCase
d = os.environ['RELSRCDIR']

with SQLTestCase() as tc:
    tc.connect(username='monetdb', password='monetdb')
    with open(os.path.join(d, os.pardir, 'VOCcreate_user.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()
    tc.connect(username='voc', password='voc')
    with open(os.path.join(d, os.pardir, 'VOCschema.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()
    with open(os.path.join(d, os.pardir, 'VOCinsert.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()
    with open(os.path.join(d, os.pardir, 'VOCquery.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()
    with open(os.path.join(d, os.pardir, 'VOCmanual_examples.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()
    with open(os.path.join(d, os.pardir, 'VOCdrop.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()
    tc.connect(username='monetdb', password='monetdb')
    with open(os.path.join(d, os.pardir, 'VOCdrop_user.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()


#import os, sys
#try:
#    from MonetDBtesting import process
#except ImportError:
#    import process
#
#d = os.environ['RELSRCDIR']
#
#def client(file, user, passwd, echo=None):
#    sys.stdout.flush()
#    sys.stderr.flush()
#    with process.client(lang='sql',
#                       user=user, passwd=passwd,
#                       args=[os.path.join(d, os.pardir, file)],
#                       log=True, echo=echo) as c:
#        c.communicate()
#
#client('VOCcreate_user.sql', 'monetdb', 'monetdb')
#client('VOCschema.sql', 'voc', 'voc')
#client('VOCinsert.sql', 'voc', 'voc', echo=False)
#client('VOCquery.sql', 'voc', 'voc')
#client('VOCmanual_examples.sql', 'voc', 'voc')
#client('VOCdrop.sql', 'voc', 'voc')
#client('VOCdrop_user.sql', 'monetdb', 'monetdb')
