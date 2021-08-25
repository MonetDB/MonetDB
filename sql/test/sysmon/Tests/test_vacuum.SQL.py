import sys
from uuid import uuid4
from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.execute('drop table if exists foo;')
    tc.execute("create table foo(a string);").assertSucceeded()
    tc.execute('insert into foo  values (\'hello\');').assertSucceeded()
    for i in range(1000):
        tc.execute(f"update foo set a='{str(uuid4())}';").assertSucceeded()
    res = tc.execute("select heapsize from sys.storage('sys', 'foo', 'a');").assertSucceeded()
    heap_large = int(res.data[0][0])
    tc.execute("call sys.vacuum('sys', 'foo', 'a');").assertSucceeded()
    res = tc.execute("select heapsize from sys.storage('sys', 'foo', 'a');").assertSucceeded()
    heap_small = int(res.data[0][0])
    assert(heap_small < heap_large)





