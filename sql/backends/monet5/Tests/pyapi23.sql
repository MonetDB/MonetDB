
# Strange bug, when loading groups from a table the aggr_group hidden parameter contains incorrect values, which leads to an incorrect result
START TRANSACTION;

CREATE AGGREGATE pyapi23(val integer) RETURNS integer LANGUAGE P {
	if 'aggr_group' in locals():
		unique = numpy.unique(aggr_group)
		x = numpy.zeros(shape=(unique.size))
		for i in range(0,unique.size):
			x[i] = numpy.sum(val[aggr_group==unique[i]])
		return(x)
	else:
		return(numpy.sum(val))
};


CREATE FUNCTION pyapi23datagen() RETURNS TABLE (g int, n int) LANGUAGE PYTHON {
	numpy.random.seed(42)
	result = dict()
	result['g'] = numpy.repeat(numpy.arange(2), 10)
	numpy.random.shuffle(result['g'])
	result['n'] = numpy.repeat(10, len(result['g']))
	return result
};


CREATE TABLE pyapi23table as select * from pyapi23datagen() with data;

# works
select g, pyapi23(n) from pyapi23datagen() group by g;
# doesn't work
select g, pyapi23(n) from pyapi23table group by g;

ROLLBACK;
