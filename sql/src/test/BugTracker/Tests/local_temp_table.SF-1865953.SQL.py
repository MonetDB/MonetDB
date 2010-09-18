import os, sys, time
from MonetDBtesting import process

def client(args):
    clt = process.client('sql', args = args, log = True)
    clt.communicate()

client([os.path.join(os.environ['RELSRCDIR'],
                     'local_temp_table_data.SF-1865953.sql')])
client([os.devnull])
