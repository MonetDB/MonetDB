###
# Check that, if a user has a non-default current_role that is the schema
# authorization for the current_schema, the user will still be able to grant
# object privileges on tables in the current_schema.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("""
        create role hr_role;
        create schema hr authorization hr_role;
        create user blake with password 'password' name 'Blake' schema "hr";
        create user clark with password 'password' name 'Clark' schema "hr";
        grant hr_role to blake;
        """).assertSucceeded()

    tc.connect(username="blake", password="password")
    tc.execute("""
            set role hr_role;
            create table employees (id bigint,name varchar(20));
        """).assertSucceeded()

    tc.execute("grant select on employees to clark;").assertFailed(err_code="01007", err_message="GRANT: Grantor 'blake' is not allowed to grant privileges for table 'employees'")
    tc.execute("grant select on employees to clark from current_role;").assertSucceeded()

