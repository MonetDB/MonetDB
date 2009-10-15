import os, time
from MonetDBtesting import process

def main():
    srv = process.server('sql', stdin = process.PIPE)
    time.sleep(10)                      # give server time to start
    clt = process.client('sql', stdin = process.PIPE)
    clt.communicate('select 1;\n')
    srv.communicate()

main()
