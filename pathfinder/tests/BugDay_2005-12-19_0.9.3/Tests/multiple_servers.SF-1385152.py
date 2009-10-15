import os, time
from MonetDBtesting import process

def main():
    srv = process.server('mil',
                         dbinit = 'module(pathfinder);module(sql_server);mil_start();',
                         stdin = process.PIPE)
    time.sleep(10)                      # give server time to start
    clt = process.client('mil', stdin = process.PIPE)
    clt.communicate()
    srv.communicate()

main()
