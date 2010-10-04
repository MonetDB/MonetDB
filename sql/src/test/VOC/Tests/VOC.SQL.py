import os, sys
from MonetDBtesting import process

d = os.environ['RELSRCDIR']

def client(file, user, passwd):
    c = process.client(lang = 'sql',
                       user = user, passwd = passwd,
                       args = [os.path.join(d, os.pardir, file)],
                       stdout = process.PIPE, stderr = process.PIPE,
                       log = True)
    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stdout.write(err)

client('VOCcreate_user.sql', 'monetdb', 'monetdb')
client('VOCschema.sql', 'voc', 'voc')
client('VOCinsert.sql', 'voc', 'voc')
client('VOCquery.sql', 'voc', 'voc')
client('VOCmanual_examples.sql', 'voc', 'voc')
client('VOCdrop.sql', 'voc', 'voc')
client('VOCdrop_user.sql', 'monetdb', 'monetdb')

sys.stdout.flush()
sys.stderr.flush()
