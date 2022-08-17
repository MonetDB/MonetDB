from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute('create schema pub').assertSucceeded()
    tc.execute("create role bartender").assertSucceeded()
    tc.execute("create table pub.beers(name VARCHAR(20))").assertSucceeded()
    tc.execute("grant all on pub.beers to bartender").assertSucceeded()
    tc.execute("create user foo with password 'foo' name 'foo' schema pub default role bartender").assertSucceeded()
    with SQLTestCase() as tc:
        tc.connect(username="foo", password="foo")
        tc.execute("select current_user").assertValue('foo')
        tc.execute("select current_role").assertValue('bartender')
        tc.execute("insert into pub.beers values ('Amstel'), ('Grolsch'), ('Brand')").assertSucceeded()
        tc.execute("delete from pub.beers").assertSucceeded()
    # change back to implicitly created role foo
    with SQLTestCase() as tc:
        tc.connect(username="monetdb", password="monetdb")
        tc.execute('alter user foo default role foo').assertSucceeded()
    with SQLTestCase() as tc:
        tc.connect(username="foo", password="foo")
        tc.execute("select current_user").assertValue('foo')
        tc.execute("select current_role").assertValue('foo')


