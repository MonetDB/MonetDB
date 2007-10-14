import os, sys 
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDB.subprocess26 as subprocess


def client(cmd):
	clt = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	sys.stdout.write(clt.stdout.read())
	clt.stdout.close()
	sys.stderr.write(clt.stderr.read())
	clt.stderr.close()
	


def main():
	clcmd = ("%s/bin/Mlog -x " % os.getenv('MONETDB_PREFIX')) + str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/JdbcClient_create_tables.sql' % os.getenv('RELSRCDIR'))
	clcmd1 = ("%s/bin/Mlog -x " % os.getenv('MONETDB_PREFIX')) + str(os.getenv('SQL_CLIENT')) + "< %s" % ('%s/JdbcClient_inserts_selects.sql' % os.getenv('RELSRCDIR'))
	clcmd2 = ("%s/bin/Mlog -x " % os.getenv('MONETDB_PREFIX')) + str(os.getenv('SQL_DUMP'))
	client(clcmd)
	client(clcmd1)
	client(clcmd2)

main()
