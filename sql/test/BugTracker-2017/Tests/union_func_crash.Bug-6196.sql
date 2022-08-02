start transaction;
SELECT generate_series(1,4);
rollback;
start transaction;
CREATE FUNCTION ser(i integer, j integer) RETURNS TABLE(foo integer) LANGUAGE PYTHON {
	result = dict()
	result['foo'] = list(range(i, j))
	return result
};
select ser(1, 4);
rollback;
