import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process


def client(input, user, passwd):
    c = process.client('sql', user=user, passwd=passwd, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)


script1 = '''\
create user "mydummyuser" with password 'mydummyuser' name 'mydummyuser' schema "sys";
'''

script2 = '''\
set role "sysadmin"; --error
'''

script3 = '''\
select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = 'mydummyuser');
grant "sysadmin" to "mydummyuser";
'''

script4 = '''\
set role "sysadmin";
'''

script5 = '''\
select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = 'mydummyuser');
grant "sysadmin" to "mydummyuser"; --error
select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = 'mydummyuser');
revoke "sysadmin" from "mydummyuser";
select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = 'mydummyuser');
revoke "sysadmin" from "mydummyuser"; --error
select count(*) from "user_role" where "login_id" in (select "id" from "sys"."auths" where "name" = 'mydummyuser');
drop user "mydummyuser";
'''


def main():
    s = process.server(stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
    client(script1, 'monetdb', 'monetdb')
    client(script2, 'mydummyuser', 'mydummyuser')
    client(script3, 'monetdb', 'monetdb')
    client(script4, 'mydummyuser', 'mydummyuser')
    client(script5, 'monetdb', 'monetdb')
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)


if __name__ == '__main__':
    main()
