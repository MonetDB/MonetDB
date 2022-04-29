from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("create role bartender").assertSucceeded()
    tc.execute("create table beers(name VARCHAR(20))").assertSucceeded()
    tc.execute("grant all on beers to bartender").assertSucceeded()
    tc.execute("create user foo with password 'foo' name 'foo' schema sys default role bartender").assertSucceeded()
    with SQLTestCase() as tc:
        tc.connect(username="foo", password="foo")
        tc.execute("select current_user").assertValue('foo')
        tc.execute("select current_role").assertValue('bartender')
        tc.execute("insert into beers values ('Amstel'), ('Grolsch'), ('Brand')").assertSucceeded()
        tc.execute("delete from beers").assertSucceeded()

