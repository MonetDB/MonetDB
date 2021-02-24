import os

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
        tc.execute(query=None, client='mclient', stdin=f).assertMatchStableOut(fout=os.path.join(d,'VOCinsert.stable.out'))
    with open(os.path.join(d, os.pardir, 'VOCquery.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertMatchStableOut(fout=os.path.join(d,'VOCquery.stable.out'))
    with open(os.path.join(d, os.pardir, 'VOCmanual_examples.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertMatchStableOut(fout=os.path.join(d,'VOCmanual_examples.stable.out'))
    with open(os.path.join(d, os.pardir, 'VOCdrop.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()
    tc.connect(username='monetdb', password='monetdb')
    with open(os.path.join(d, os.pardir, 'VOCdrop_user.sql')) as f:
        tc.execute(query=None, client='mclient', stdin=f).assertSucceeded()
