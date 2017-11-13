START TRANSACTION;

CREATE TABLE test_table(x STRING, y STRING, z STRING);
INSERT INTO test_table VALUES ('test value 1', 'test value 2', 'test value 3');

CREATE FUNCTION test_function(x STRING, y STRING, z STRING) RETURNS TABLE(a STRING, b STRING, c FLOAT) LANGUAGE PYTHON {
    result = dict()
    result['a'] = ['test value a']
    result['b'] = ['test value b']
    result['c'] = [3]

    return result
};

CREATE TABLE results_table AS (
    WITH test_table_tmp AS (
        SELECT * FROM test_table
    )
    SELECT * FROM test_function(test_table_tmp.x, test_table_tmp.y, test_table_tmp.z)
);

SELECT * FROM results_table;

ROLLBACK;
