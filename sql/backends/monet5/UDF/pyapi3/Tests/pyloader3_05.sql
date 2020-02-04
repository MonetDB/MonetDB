
# test string returns
START TRANSACTION;
CREATE TABLE pyloader05table(s STRING);
CREATE LOADER pyloader05() LANGUAGE PYTHON {
    _emit.emit({'s': 33});
    _emit.emit({'s': 42.0});
    _emit.emit({'s': 'hello'});
    _emit.emit({'s': u'\u00D6'}); # \u00D6 = O + umlaut
    _emit.emit({'s': [33, 'hello']});
    _emit.emit({'s': [42.0, 33]});
    _emit.emit({'s': numpy.array(['hello', 'hello', 'hello'])});
    _emit.emit({'s': [u'\u00D6', 'hello', 33]});
    _emit.emit({'s': numpy.array([u'\u00D6', 'hello', 33])});
    _emit.emit({'s': numpy.arange(3).astype(numpy.float32)});
    _emit.emit({'s': numpy.arange(3).astype(numpy.float64)});
    _emit.emit({'s': numpy.arange(3).astype(numpy.int8)});
    _emit.emit({'s': numpy.arange(3).astype(numpy.int16)});
    _emit.emit({'s': numpy.arange(3).astype(numpy.int32)});
    _emit.emit({'s': numpy.arange(3).astype(numpy.int64)});
};
COPY LOADER INTO pyloader05table FROM pyloader05();
SELECT * FROM pyloader05table;
DROP TABLE pyloader05table;
DROP LOADER pyloader05;
ROLLBACK;
