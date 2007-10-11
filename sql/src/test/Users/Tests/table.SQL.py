import os, sys


def main():
	clcmd = str(os.getenv('SQL_CLIENT')) + "-umy_user -Pp1 < %s" % ('%s/../table.sql' % os.getenv('RELSRCDIR'))
	clt = os.popen(clcmd, 'w')
	clt.close() 

main()
