###
# Grant SYSADMIN role to a user.
# Verify that the user can CREATE and DROP SCHEMA.
# Verify that the user can DROP SCHEMA created by both the 'monetdb' user and the created user.
###


from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("""
     CREATE USER user1 WITH PASSWORD 'user1' name 'schema test user1' schema sys;
     CREATE USER user2 WITH PASSWORD 'user2' name 'schema test user2' schema sys;
     GRANT sysadmin TO user1;
     GRANT sysadmin TO user2;
     CREATE SCHEMA schema1 AUTHORIZATION sysadmin;
    """).assertSucceeded()

    tc.connect(username='user1', password='user1')
    tc.execute("""
        SET ROLE sysadmin;
        DROP SCHEMA schema1;
        CREATE SCHEMA schema2;
        DROP SCHEMA schema2;
        CREATE SCHEMA schema3;
     """).assertSucceeded()

    tc.connect(username='user2', password='user2')
    tc.execute("""
         SET ROLE sysadmin;
         DROP SCHEMA schema3;
     """).assertSucceeded()

    tc.connect(username="monetdb", password="monetdb")
    tc.execute("""
         DROP USER user1;
         DROP USER user2;
     """).assertSucceeded()
