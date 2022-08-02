
# test returning of numpy.nan values

START TRANSACTION;

CREATE FUNCTION pyapi32() RETURNS TABLE(i INTEGER, j DOUBLE) LANGUAGE PYTHON {
    return {'i': numpy.nan, 'j': numpy.nan }
};

SELECT * FROM pyapi32();
SELECT * FROM pyapi32() WHERE i IS NULL;
SELECT * FROM pyapi32() WHERE j IS NULL;

DROP FUNCTION pyapi32;

ROLLBACK;
