from MonetDBtesting import process

clt = process.client('sql', user = 'this_user_does_not_exist', passwd = 'this_password_does_not_exist')
clt.wait()
