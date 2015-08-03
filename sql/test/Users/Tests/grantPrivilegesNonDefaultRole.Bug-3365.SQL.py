###
# Let a user inherit the rights of monetdb.
# Check that by assuming the monetdb role the user has complete privileges (e.g. select, create, drop).
###


from util import sql_test_client

sql_test_client('monetdb', 'monetdb', input = """\
create role hr_role;
create schema hr authorization hr_role;
create user blake with password 'password' name 'Blake' schema "hr";
create user clark with password 'password' name 'Clark' schema "hr";
grant hr_role to blake;
""")

sql_test_client('blake', 'password', input = """\
set role hr_role;
create table employees (id bigint,name varchar(20));
grant select on employees to clark;
grant select on employees to clark from current_role;
""")

