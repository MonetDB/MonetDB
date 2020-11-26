from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("""
    create table tbl_xy (x int, y int);
    insert into tbl_xy values (0,0);
    insert into tbl_xy values (1,1);
    """).assertSucceeded()

    tc.execute("""
    insert into tbl_xy values (2,2);
    insert into tbl_xy values (3,3);
    insert into tbl_xy values (4,4);
    """).assertSucceeded()

    tc.execute("""
    insert into tbl_xy values (5,5);
    insert into tbl_xy values (6,6);
    insert into tbl_xy values (7,7);
    """).assertSucceeded()

    tc.execute("insert into tbl_xy values (-1, -100);").assertSucceeded().assertRowCount(1)

    tc.execute("""
    insert into tbl_xy values (8,8);
    insert into tbl_xy values (9,9);
    insert into tbl_xy values (10,10);
    """).assertSucceeded()

    tc.execute("select * from tbl_xy;").assertSucceeded().assertRowCount(12).assertDataResultMatch([(0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(-1,-100),(8,8),(9,9),(10,10)])
    tc.execute("drop table tbl_xy;").assertSucceeded()
