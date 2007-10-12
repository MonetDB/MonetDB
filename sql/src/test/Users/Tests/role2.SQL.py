import os, sys


def main():
	clcmd = str(os.getenv('SQL_CLIENT')) + "-umy_user2 -Pp2 < %s" % ('%s/../role.sql' % os.getenv('RELSRCDIR'))
	clt = os.popen(clcmd, 'w')
	clt.close() 

main()
