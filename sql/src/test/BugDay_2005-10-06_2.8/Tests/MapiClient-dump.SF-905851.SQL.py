import os, sys


def main():
	clcmd = ("%s/bin/Mlog -x " % os.getenv('MONETDB_PREFIX')) + str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/JdbcClient_create_tables.sql' % os.getenv('RELSRCDIR'))
	clcmd1 = ("%s/bin/Mlog -x " % os.getenv('MONETDB_PREFIX')) + str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/JdbcClient_inserts_selects.sql' % os.getenv('RELSRCDIR'))
	clcmd2 = ("%s/bin/Mlog -x " % os.getenv('MONETDB_PREFIX')) + str(os.getenv('SQL_DUMP'))
	clt = os.popen(clcmd, 'w')
	clt.close() 
	clt1 = os.popen(clcmd1, 'w')
	clt1.close() 
	clt2 = os.popen(clcmd2, 'w')
	clt2.close() 

main()
