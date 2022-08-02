# Parallel aggregates
# Aggregations created with PYTHON_MAP are computed in parallel over GROUP BY statements
# Meaning that manual looping over the 'aggr_group' parameter is not necessary
# instead, you can create a single PYTHON_MAP aggregate and it will be computed once per group in parallel


START TRANSACTION;

CREATE FUNCTION rvalfunc() RETURNS TABLE(groupcol INTEGER, intcol INTEGER, fltcol FLOAT, dblcol DOUBLE, strcol STRING) LANGUAGE PYTHON {
    return {'groupcol': [0,1,2,0,1,2,3], 
            'intcol' : [42,84,42,21,42,21,21],
            'fltcol' : [42,84,42,21,42,21,21],
            'dblcol' : [42,84,42,21,42,21,21],
            'strcol' : [42,84,42,21,42,21,21]
            }  
};

CREATE AGGREGATE aggrmedian_int(val integer) RETURNS integer LANGUAGE PYTHON_MAP { return numpy.median(val) };
CREATE AGGREGATE aggrmedian_flt(val float) RETURNS integer LANGUAGE PYTHON_MAP { return numpy.median(val) };
CREATE AGGREGATE aggrmedian_dbl(val double) RETURNS integer LANGUAGE PYTHON_MAP { return numpy.median(val) };
CREATE AGGREGATE aggrmedian_str(val string) RETURNS integer LANGUAGE PYTHON_MAP { return numpy.median([int(x) for x in val]) };

# Parallel aggregation with integer column
SELECT groupcol,aggrmedian_int(intcol) FROM rvalfunc() GROUP BY groupcol;
SELECT groupcol,aggrmedian_flt(fltcol) FROM rvalfunc() GROUP BY groupcol;
SELECT groupcol,aggrmedian_dbl(dblcol) FROM rvalfunc() GROUP BY groupcol;
SELECT groupcol,aggrmedian_str(strcol) FROM rvalfunc() GROUP BY groupcol;

DROP AGGREGATE aggrmedian_flt;
DROP AGGREGATE aggrmedian_dbl;
DROP AGGREGATE aggrmedian_str;
DROP FUNCTION rvalfunc;

# Parallel aggregates with NULL values
#CREATE FUNCTION rvalfunc() RETURNS TABLE(groupcol INTEGER, datacol INTEGER) LANGUAGE PYTHON {
#    return {'groupcol': [0,1,2,0,1,2,3], 
#            'datacol' : numpy.ma.masked_array([42,84,42,21,42,21,21], mask = [1,0,0,0,0,0,1])
#            }  
#};

#SELECT groupcol,aggrmedian_int(datacol) FROM rvalfunc() GROUP BY groupcol;
DROP AGGREGATE aggrmedian_int;

ROLLBACK;
