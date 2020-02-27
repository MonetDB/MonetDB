import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

d = os.environ['RELSRCDIR']

def client(file, user, passwd):
    sys.stdout.flush()
    sys.stderr.flush()
    c = process.client(lang = 'sql',
                       user = user, passwd = passwd,
                       args = [os.path.join(d, os.pardir, file)],
                       log = True)
    c.communicate()

client('VOCcreate_user.sql', 'monetdb', 'monetdb')
client('VOCschema.sql', 'voc', 'voc')
client('VOCinsert.sql', 'voc', 'voc')
client('VOCquery.sql', 'voc', 'voc')
client('VOCmanual_examples.sql', 'voc', 'voc')
client('VOCdrop.sql', 'voc', 'voc')
client('VOCdrop_user.sql', 'monetdb', 'monetdb')
