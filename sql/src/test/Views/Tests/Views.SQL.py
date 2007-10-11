import os, sys


def main():
	clcmd = str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/../views_restrictions.sql' % os.getenv('RELSRCDIR'))
	sys.stdout.write('Views Restrictions\n')
	clt = os.popen(clcmd, 'w')
	sys.stdout.write('step 1\n')
	sys.stdout.write('Cleanup\n')
	sys.stdout.write('step2\n')
	clt.close() 

main()
