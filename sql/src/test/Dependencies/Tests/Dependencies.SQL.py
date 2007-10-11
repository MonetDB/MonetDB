import os, sys


def main():
	clcmd = str(os.getenv('SQL_CLIENT'))
	clcmd1 = str(os.getenv('SQL_CLIENT')) + "-umonet_test -Ppass_test"
	sys.stdout.write('Dependencies between User and Schema\n')
	sys.stdout.flush()
	clt = os.popen(clcmd + "<%s" % ('%s/../dependency_owner_schema_1.sql' % os.getenv('RELSRCDIR')), 'w')
	sys.stdout.write('done\n')
	clt.close() 
	clt1 = os.popen(clcmd1 + "<%s" % ('%s/../dependency_owner_schema_2.sql' % os.getenv('RELSRCDIR')), 'w')
	sys.stdout.write('done\n')
	clt1.close() 
	sys.stdout.write('Dependencies between database objects\n')
	sys.stdout.flush()
	clt = os.popen(clcmd + "<%s" % ('%s/../dependency_DBobjects.sql' % os.getenv('RELSRCDIR')), 'w')
	sys.stdout.write('done\n')
	clt.close() 
	sys.stdout.write('Dependencies between functions with same name\n')
	sys.stdout.flush()
	clt = os.popen(clcmd + "<%s" % ('%s/../dependency_functions.sql' % os.getenv('RELSRCDIR')), 'w')
	sys.stdout.write('done\n')
	clt.close() 
	sys.stdout.write('Cleanup\n')
	sys.stdout.flush()
	clt = os.popen(clcmd + "<%s" % ('%s/../dependency_owner_schema_3.sql' % os.getenv('RELSRCDIR')), 'w')
	sys.stdout.write('done\n')
	clt.close() 

main()
