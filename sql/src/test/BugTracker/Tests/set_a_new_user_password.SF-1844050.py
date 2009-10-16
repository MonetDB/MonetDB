import os, time, sys
from MonetDBtesting import process

def server_start(lang):
    sys.stderr.write('#mserver\n')
    sys.stderr.flush()
    srv = process.server(lang, stdin = process.PIPE)
    time.sleep(5)                      # give server time to start
    return srv

def client(lang, file, user = None, passwd = None):
    sys.stderr.write('#client\n')
    sys.stderr.flush()
    clt = process.client(lang, user = user, passwd = passwd, stdin = open(file))
    clt.communicate()

def main():
    srv = server_start('sql')
    client('sql' , os.path.join(os.getenv('RELSRCDIR'), 'set_a_new_user_password.SF-1844050_create_user.sql'))
    srv.communicate()

    srv = server_start('sql')
    client('sql' , os.path.join(os.getenv('RELSRCDIR'), 'set_a_new_user_password.SF-1844050_select.sql'), "voc2", "new")
    srv.communicate()

    srv = server_start('sql')
    client('sql' , os.path.join(os.getenv('RELSRCDIR'), 'set_a_new_user_password.SF-1844050_drop_user.sql'))
    srv.communicate()

main()
