from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("create user bar with password 'bar' name 'full name bar'").assertSucceeded()
    with SQLTestCase() as tc:
        tc.connect(username="bar", password="bar")
        tc.execute("select current_user").assertValue('bar')
        tc.execute("select current_role").assertValue('bar')
        tc.execute("select current_schema").assertValue('bar')
        tc.execute("create table beers(name VARCHAR(20))").assertSucceeded()
        tc.execute("insert into beers values ('Amstel'), ('Grolsch'), ('Brand')").assertSucceeded()
    with SQLTestCase() as tc:
        tc.connect(username="monetdb", password="monetdb")
        tc.execute('select count(*) from bar.beers').assertValue(3)


