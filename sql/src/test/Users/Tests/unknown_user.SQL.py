import os, sys

cltcmd = "%s -uthis_user_does_not_exist -Pthis_password_does_not_exist" % (os.getenv('SQL_CLIENT'))

os.system(cltcmd);
