
START TRANSACTION;

# basic insertion of a set of arrays
CREATE TABLE restable(a1 INTEGER, a2 INTEGER, a3 INTEGER, a4 INTEGER);
CREATE LOADER myloader(nvalues INTEGER) LANGUAGE PYTHON {
    a1 = numpy.arange(nvalues)
    a2 = (a1 * 2).astype(numpy.float64)
    a3 = (a1 * 4 + 20).astype(numpy.float32)
    a4 = ((a3 - a2) * 3).astype(numpy.int64)
    _emit.emit({'a1': a1, 'a2': a2, 'a3': a3, 'a4': a4})
};

COPY LOADER INTO restable FROM myloader(10);

SELECT * FROM restable;

DROP TABLE restable;
DROP LOADER myloader;

# try list insertion and object types
CREATE TABLE restable(a1 INTEGER, a2 BIGINT, a3 SMALLINT, a4 REAL);
CREATE LOADER myloader() LANGUAGE PYTHON {
    a1 = [1,2,3,4,5]
    a2 = (numpy.array(a1) * 2).astype(numpy.object)
    a3 = [42, 43, 44, 45, 46]
    a4 = ["33", "44", "55", "66", "77"]
    _emit.emit({'a1': a1, 'a2': a2, 'a3': a3, 'a4': a4})
};

COPY LOADER INTO restable FROM myloader();

SELECT * FROM restable;

DROP TABLE restable;
DROP LOADER myloader;

# different length arrays (this should fail)
CREATE TABLE restable(a1 INTEGER, a2 BIGINT);
CREATE LOADER myloader() LANGUAGE PYTHON {
    a1 = numpy.arange(100)
    a2 = numpy.arange(200)
    _emit.emit({'a1': a1, 'a2': a2})
};

COPY LOADER INTO restable FROM myloader();

ROLLBACK;

START TRANSACTION;

# test masked arrays and missing values
CREATE TABLE restable(a1 INTEGER, a2 BIGINT, a3 SMALLINT, a4 REAL);
CREATE LOADER myloader(nvalues INTEGER) LANGUAGE PYTHON {
    arr_vals = numpy.arange(nvalues)
    a1 = numpy.ma.masked_array(arr_vals, mask=numpy.mod(arr_vals, 2) == 0)
    a2 = a1 * 2
    a3 = numpy.ma.masked_array(arr_vals, mask=arr_vals < nvalues / 2)
    a4 = [None] * nvalues
    _emit.emit({'a1': a1, 'a2': a2, 'a3': a3, 'a4': a4})
};

COPY LOADER INTO restable FROM myloader(50);

SELECT * FROM restable;

DROP TABLE restable;
DROP LOADER myloader;

# test multiple emits that emit both arrays and scalar values
CREATE TABLE restable(a1 INTEGER, a2 BIGINT, a3 SMALLINT, a4 REAL);
CREATE LOADER myloader() LANGUAGE PYTHON {
    a1 = 1
    a2 = 2
    a3 = 3
    a4 = 4
    _emit.emit({'a1': a1, 'a2': a2, 'a3': a3, 'a4': a4})
    a1 = [1,2,3,4,5]
    a2 = (numpy.array(a1) * 2).astype(numpy.object)
    a3 = [42, 43, 44, 45, 46]
    a4 = ["33", "44", "55", "66", "77"]
    _emit.emit({'a1': a1, 'a2': a2, 'a3': a3, 'a4': a4})
    for i in range(100):
        a1 = numpy.arange(2)
        a2 = (a1 * 2).astype(numpy.float64)
        a3 = (a1 * 4 + 20).astype(numpy.float32)
        a4 = ((a3 - a2) * 3).astype(numpy.int64)
        _emit.emit({'a1': a1, 'a2': a2, 'a3': a3, 'a4': a4})
    a1 = 42
    a2 = 42
    a3 = 42
    a4 = 42
    for i in range(10):
        _emit.emit({'a1': a1, 'a2': a2, 'a3': a3, 'a4': a4})
};

COPY LOADER INTO restable FROM myloader();

SELECT * FROM restable;

DROP TABLE restable;
DROP LOADER myloader;

ROLLBACK;
