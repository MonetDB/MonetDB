# Lazy array tests
START TRANSACTION;

CREATE TABLE ival(i integer, j integer);
INSERT INTO ival VALUES (1, 1), (2, 2), (NULL, 3), (4, 4), (5, 5);


CREATE FUNCTION pyapi15(i integer, j integer) returns integer
language P
{
    # type <lazyarray>
    print(type(i))
    # throws exception, trying to access a NULL value
    try: print(i[2])
    except Exception as inst: print inst
    # throws exception, numpy treats lazy array as a list and then accesses the third (NULL) value
    try: print(numpy.array(i))
    except Exception as inst: print inst
    # regular math functions work
    print(i * 3)
    print(i * numpy.array([1,2,3,4,5]))
    # convert to numpy masked array, conversion is cached so no matter how often you call it it only converts once
    print(i.asnumpyarray())
    print(type(i.asnumpyarray()))

    # again, throws an error
    try: print(numpy.mean(i))
    except Exception as inst: print inst

    # works without null values
    print(numpy.mean(j))

    # we can replace the method by something like this
    old_mean = numpy.mean
    def new_mean(a, axis=None, dtype=None, out=None, keepdims=False):
        if type(a).__name__ == "lazyarray":
            return old_mean(a.asnumpyarray(), axis, dtype, out, keepdims)
        return old_mean(a, axis, dtype, out, keepdims)
    numpy.mean = new_mean
    # now it works
    print(numpy.mean(i))

    # return lazy array
    return i;
};

SELECT pyapi15(i, j) FROM ival;
DROP FUNCTION pyapi15;
DROP TABLE strval;

ROLLBACK;

