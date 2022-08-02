# Create a table with a C UDF
START TRANSACTION;

CREATE FUNCTION capi01(inp INTEGER) RETURNS TABLE (i INTEGER, d DOUBLE)
language C
{
#include <math.h>
    size_t j;
    size_t count = inp.data[0];
    i->initialize(i, count);
    d->initialize(d, count);
    for(j = 0; j < count; j++) {
        i->data[j] = j;
        d->data[j] = round(j > 0 ? 42.0 / j : 42.0);
    }
};

SELECT i,d FROM capi01(42) AS R;
DROP FUNCTION capi01;

ROLLBACK;
