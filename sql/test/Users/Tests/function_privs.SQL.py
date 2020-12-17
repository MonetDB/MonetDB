###
# Check direct function privilege:
#   A user with SEL/INS/UPD/DEL privilege on a table and EXEC privilege on the
#   functions SEL/INS/UPD/DEL that table can execute the functions.
# Check indirect function privilege:
#   A user with only(!) EXEC privilege on functions that SEL/INS/UPD/DEL a
#   table can execute the functions.
# Check that if a user has CREATE FUNCTION privilege in one schema (i.e. is an
#   schema owner) but not in another schema, then the user can CREATE FUNCTIONs
#   to operate on tables in this schema but not in the other schema.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")
    mdb.execute("CREATE ROLE my_role;").assertSucceeded()
    mdb.execute("CREATE SCHEMA my_schema AUTHORIZATION my_role;").assertSucceeded()
    mdb.execute("CREATE USER my_user WITH PASSWORD 'p1' NAME 'my_user' SCHEMA sys;").assertSucceeded()
    mdb.execute("CREATE USER my_user2 WITH PASSWORD 'p2' NAME 'my_user2' SCHEMA sys;").assertSucceeded()
    mdb.execute("SET SCHEMA my_schema;").assertSucceeded()
    mdb.execute("CREATE TABLE version (name VARCHAR(10), i INT);").assertSucceeded()
    mdb.execute("INSERT INTO version VALUES ('test1', 1);").assertRowCount(1)

    mdb.execute("""
        create function selectversion() returns table(name varchar(10), i int)
        begin
          return table(select * from version);
        end;
    """).assertSucceeded()
    mdb.execute("""
        create function insertversion(iname varchar(10), ii int) returns int
        begin
          insert into version values (iname, ii);
          return 1;
        end;
    """).assertSucceeded()
    mdb.execute("""
        create function updateversion(iname varchar(10), ii int) returns int
        begin
          update version set i = ii where name = iname;
          return 1;
        end;
    """).assertSucceeded()
    mdb.execute("""
        create function deleteversion(iname varchar(10)) returns int
        begin
          delete from version where name = iname;
          return 1;
        end;
    """).assertSucceeded()
    mdb.execute("""
        create function truncateversion() returns int
        begin
          truncate version;
          return 1;
        end;
    """).assertSucceeded()

    # create a table to which my_user doesn't have access
    mdb.execute("CREATE SCHEMA your_schema;").assertSucceeded()
    mdb.execute("create table your_schema.your_table (name varchar(10), i int);").assertSucceeded()

    # grant table right to my_user but not to my_user2
    mdb.execute("GRANT SELECT on table version to my_user;").assertSucceeded()
    mdb.execute("GRANT INSERT on table version to my_user;").assertSucceeded()
    mdb.execute("GRANT UPDATE on table version to my_user;").assertSucceeded()
    mdb.execute("GRANT DELETE on table version to my_user;").assertSucceeded()

    # grant function rights to both users
    mdb.execute("GRANT EXECUTE on function selectversion to my_user;").assertSucceeded()
    mdb.execute("GRANT EXECUTE on function insertversion to my_user;").assertSucceeded()
    mdb.execute("GRANT EXECUTE on function updateversion to my_user;").assertSucceeded()
    mdb.execute("GRANT EXECUTE on function deleteversion to my_user;").assertSucceeded()
    mdb.execute("GRANT EXECUTE on function truncateversion to my_user;").assertSucceeded()

    mdb.execute("GRANT EXECUTE on function selectversion to my_user2;").assertSucceeded()
    mdb.execute("GRANT EXECUTE on function insertversion to my_user2;").assertSucceeded()
    mdb.execute("GRANT EXECUTE on function updateversion to my_user2;").assertSucceeded()
    mdb.execute("GRANT EXECUTE on function deleteversion to my_user2;").assertSucceeded()
    mdb.execute("GRANT EXECUTE on function truncateversion to my_user2;").assertSucceeded()

    with SQLTestCase() as tc:
        # my_user can SEL/INS/UPD/DEL both directly on the table or through the functions
        tc.connect(username="my_user", password="p1")
        tc.execute("SET SCHEMA my_schema;").assertSucceeded()
        tc.execute("INSERT INTO version (name, i) VALUES ('test2', 2);").assertSucceeded()
        tc.execute("SELECT * FROM version;").assertSucceeded().assertDataResultMatch([('test1', 1), ('test2', 2)])
        tc.execute("UPDATE version SET name = 'test22' WHERE i = 2;").assertSucceeded()
        tc.execute("SELECT * FROM version;").assertSucceeded().assertDataResultMatch([('test1', 1), ('test22', 2)])
        tc.execute("DELETE FROM version WHERE name = 'test22';").assertSucceeded()
        tc.execute("SELECT * FROM version;").assertSucceeded().assertDataResultMatch([('test1', 1)])
        tc.execute("SELECT insertversion('test3', 3);").assertSucceeded()
        tc.execute("SELECT updateversion('test1', 4);").assertSucceeded()
        tc.execute("SELECT deleteversion('test1');").assertSucceeded()
        tc.execute("SELECT * FROM selectversion();").assertSucceeded().assertDataResultMatch([('test3', 3)])
        tc.execute("SELECT truncateversion();").assertSucceeded()
        tc.execute("SELECT * FROM selectversion();").assertSucceeded().assertDataResultMatch([])
        tc.execute("INSERT INTO version VALUES ('test3', 3)").assertSucceeded() # insert for the next user
        # make my_user an owner of my_schema so that my_user can create functions
        # in my_schema
        mdb.execute("GRANT my_role to my_user;").assertSucceeded()
        tc.execute("SET ROLE my_role;").assertSucceeded()
        tc.execute("""
            create function myselect() returns table(name varchar(10), i int)
            begin
              return table(select * from version);
            end;
        """).assertSucceeded()
        tc.execute("""
            create function myinsert(iname varchar(10), ii int) returns int
            begin
              insert into version (name, i) values (iname, ii);
              return 1;
            end;
        """).assertSucceeded()
        tc.execute("""
            create function myupdate(iname varchar(10), ii int) returns int
            begin
              update version set i = ii where name = iname;
              return 1;
            end;
        """).assertSucceeded()
        tc.execute("""
            create function mydelete(iname varchar(10)) returns int
            begin
              delete from version where name = iname;
              return 1;
            end;
        """).assertSucceeded()
        tc.execute("""
            create function mytruncate() returns int
            begin
              truncate version;
              return 1;
            end;
        """).assertSucceeded()
        tc.execute("DROP FUNCTION myselect;").assertSucceeded()
        tc.execute("DROP FUNCTION myinsert;").assertSucceeded()
        tc.execute("DROP FUNCTION myupdate;").assertSucceeded()
        tc.execute("DROP FUNCTION mydelete;").assertSucceeded()
        tc.execute("DROP FUNCTION mytruncate;").assertSucceeded()
        # but my_user cannot create functions to operate on a table to which it
        # doesn't have access
        tc.execute("""
            create function yourselect() returns table(name varchar(10), i int)
            begin
              return table(select * from version union select * from your_schema.your_table);
            end;
        """).assertFailed(err_code="42000", err_message="SELECT: access denied for my_user to table 'your_schema.your_table'")

        # my_user2 can only indirectly SEL/INS/UPD/DEL the table through the functions
        tc.connect(username="my_user2", password="p2")
        tc.execute("SET SCHEMA my_schema;").assertSucceeded()
        tc.execute("INSERT INTO version (name, i) VALUES ('test2', 2);").assertFailed(err_code="42000", err_message="INSERT INTO: insufficient privileges for user 'my_user2' to insert into table 'version'")
        tc.execute("UPDATE version SET name = 'test22' WHERE i = 2;").assertFailed(err_code="42000", err_message="UPDATE: insufficient privileges for user 'my_user2' to update table 'version'")
        tc.execute("DELETE FROM version WHERE name = 'test22';").assertFailed(err_code="42000", err_message="DELETE FROM: insufficient privileges for user 'my_user2' to delete from table 'version'")
        tc.execute("SELECT * FROM version;").assertFailed(err_code="42000", err_message="SELECT: access denied for my_user2 to table 'my_schema.version'")
        tc.execute("SELECT insertversion('test4', 4);").assertSucceeded()
        tc.execute("SELECT * FROM selectversion();").assertSucceeded().assertDataResultMatch([('test3', 3), ('test4', 4)])
        tc.execute("SELECT updateversion('test3', 33);").assertSucceeded()
        tc.execute("SELECT * FROM selectversion();").assertSucceeded().assertDataResultMatch([('test3', 33), ('test4', 4)])
        tc.execute("SELECT deleteversion('test3');").assertSucceeded()
        tc.execute("SELECT * FROM selectversion();").assertSucceeded().assertDataResultMatch([('test4', 4)])
        tc.execute("SELECT truncateversion();").assertSucceeded()
        tc.execute("SELECT * FROM selectversion();").assertSucceeded().assertDataResultMatch([])
        # and my_user2 cannot create functions to operate on the table
        tc.execute("""
            create function myselect2() returns table(name varchar(10), i int)
            begin
              return table(select * from version);
            end;
        """).assertFailed(err_code="42000", err_message="CREATE UNION FUNCTION: insufficient privileges for user 'my_user2' in schema 'my_schema'")
        tc.execute("""
            create function myinsert2(iname varchar(10), ii int) returns int
            begin
              insert into version (name, i) values (iname, ii);
              return 1;
            end;
        """).assertFailed(err_code="42000", err_message="CREATE FUNCTION: insufficient privileges for user 'my_user2' in schema 'my_schema'")
        tc.execute("""
            create function myupdate2(iname varchar(10), ii int) returns int
            begin
              update version set i = ii where name = iname;
              return 1;
            end;
        """).assertFailed(err_code="42000", err_message="CREATE FUNCTION: insufficient privileges for user 'my_user2' in schema 'my_schema'")
        tc.execute("""
            create function mydelete2(iname varchar(10)) returns int
            begin
              delete from version where name = iname;
              return 1;
            end;
        """).assertFailed(err_code="42000", err_message="CREATE FUNCTION: insufficient privileges for user 'my_user2' in schema 'my_schema'")
        tc.execute("""
            create function mytruncate2() returns int
            begin
              truncate version;
              return 1;
            end;
        """).assertFailed(err_code="42000", err_message="CREATE FUNCTION: insufficient privileges for user 'my_user2' in schema 'my_schema'")

        # clean up
        mdb.connect(username="monetdb", password="monetdb")
        mdb.execute("DROP FUNCTION my_schema.selectversion;").assertSucceeded()
        mdb.execute("DROP FUNCTION my_schema.insertversion;").assertSucceeded()
        mdb.execute("DROP FUNCTION my_schema.updateversion;").assertSucceeded()
        mdb.execute("DROP FUNCTION my_schema.deleteversion;").assertSucceeded()
        mdb.execute("DROP FUNCTION my_schema.truncateversion;").assertSucceeded()
        mdb.execute("DROP TABLE my_schema.version;").assertSucceeded()
        mdb.execute("DROP TABLE your_schema.your_table;").assertSucceeded()
        mdb.execute("DROP USER my_user;").assertSucceeded()
        mdb.execute("DROP USER my_user2;").assertSucceeded()
        mdb.execute("DROP ROLE my_role;").assertSucceeded()
        mdb.execute("DROP SCHEMA my_schema;").assertSucceeded()
        mdb.execute("DROP SCHEMA your_schema;").assertSucceeded()

