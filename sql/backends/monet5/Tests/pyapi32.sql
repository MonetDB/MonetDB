
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

COPY INTO restable FROM LOADER myloader(10);

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

COPY INTO restable FROM LOADER myloader();

SELECT * FROM restable;

DROP TABLE restable;
DROP LOADER myloader;

# different length arrays (this should fail)
CREATE TABLE restable(a1 INTEGER, a2 BIGINT, a3 SMALLINT, a4 REAL);
CREATE LOADER myloader() LANGUAGE PYTHON {
    a1 = numpy.arange(100)
    a2 = numpy.arange(200)
    a3 = numpy.arange(300)
    a4 = numpy.arange(400)
    _emit.emit({'a1': a1, 'a2': a2, 'a3': a3, 'a4': a4})
};

COPY INTO restable FROM LOADER myloader();

ROLLBACK;
