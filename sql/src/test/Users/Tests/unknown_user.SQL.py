import os, sys

os.environ['DOTMONETDBFILE'] = '.nouser'
f = open(os.environ['DOTMONETDBFILE'], 'wb')
f.write('user=this_user_does_not_exist\npassword=this_password_does_not_exist\n')
f.close()
os.system(os.getenv('SQL_CLIENT'))
os.unlink(os.environ['DOTMONETDBFILE'])
