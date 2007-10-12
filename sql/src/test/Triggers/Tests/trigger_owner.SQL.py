import os, sys


def main():
	clcmd = str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/../trigger_owner_create.sql' % os.getenv('RELSRCDIR'))
	clcmd1 = str(os.getenv('SQL_CLIENT')) + "-uuser_test -Ppass < %s" % ('%s/../trigger_owner.sql' % os.getenv('RELSRCDIR'))
	clcmd2 = str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/../trigger_owner_drop.sql' % os.getenv('RELSRCDIR'))
	sys.stdout.write('trigger owner\n')
	clt = os.popen(clcmd, 'w')
	clt.close() 
	clt1 = os.popen(clcmd1, 'w')
	clt1.close() 
	clt2 = os.popen(clcmd2, 'w')
	clt2.close() 
	sys.stdout.write('done\n')

main()
