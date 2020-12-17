from MonetDBtesting.sqltest import SQLTestCase

def zones():
    with SQLTestCase() as tc:
        tc.connect(username="monetdb", password="monetdb")
        tc.execute('select degrees(-0.1)').assertSucceeded().assertValue(0, 0, -5.729577951308233)

zones()
zones()
