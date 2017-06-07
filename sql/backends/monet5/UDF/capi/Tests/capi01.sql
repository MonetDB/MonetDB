# Create a table with a C UDF
START TRANSACTION;

CREATE FUNCTION capi01(inp INTEGER) RETURNS TABLE (i INTEGER, d DOUBLE)
language C
{
#include <math.h>

	size_t count = inp;
	i->count = count;
	i->data = __malloc(i->count * sizeof(i->null_value));
	d->count = count;
	d->data = __malloc(d->count * sizeof(d->null_value));
	if (!i->data || !d->data) {
		return "Malloc failure";
	}
	for(size_t j = 0; j < count; j++) {
		i->data[j] = j;
		d->data[j] = round(j > 0 ? 42.0 / j : 42.0);
	}
};

SELECT i,d FROM capi01(42) AS R;
DROP FUNCTION capi01;

ROLLBACK;
