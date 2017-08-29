/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @a Lefteris Sidirourgos
 * @d 30/08/2011
 * @+ The sampling facilities
 *
 * In the context of the SciBORQ project, we introduce a number of sampling
 * techniques in the MonetDB software stack. Our goal is to provide methods
 * for performing sampling (uniform and weighted) over a) the result of a
 * query, b) the base tables, and c) the entire database schema. Sampling
 * can be performed during query execution, as well as during data loading in
 * the case of predefined sampling indexes. In addition to the sampling
 * methods, a number of query plan optimisations for sampling are introduced on
 * the SQL and MAL level.
 *
 * Besides the sampling methods, SciBORQ also aims at multi-layered bounded
 * query execution. That is steering query execution over many layers of
 * samples with different size in order to achieve either strict error bounds
 * or limited execution time. For more details see the SciBORQ module.
 *
 * In the following, details are presented on the implementation and the usage
 * of each sampling method.
 */

#include "monetdb_config.h"
#include <gdk.h>
#include <mal_exception.h>
#include "sample.h"

/*
 * @- Uniform Sampling.
 *
 * A new SQL operator has been added to support sampling the result of a query.
 * The syntax for sampling is:
 * SELECT ... FROM ... WHERE ... SAMPLE s
 *
 * where s if is an integer greater than 1, it defines the number of rows to be
 * in the sample. If s is a double between [0.0,1.0] the it refers to the
 * percentage of the result to be sampled. That is if s=0.3 then the sample
 * will be 30% the size of the query result.
 *
 * SAMPLE is been treated as LIMIT, ORDER BY, etc., that means that it can only
 * be in the outer most SELECT clause, i.e., SAMPLE cannot appear in a
 * subquery. However, if this is needed, then one may define a function, for
 * example 
 *
 * CREATE FUNCTION mysample ()
 * RETURNS TABLE(col a,...)
 * BEGIN
 *    RETURN
 *      SELECT a,...
 *      FROM name_table
 *      SAMPLE 100;
 * end;
 *
 * and then use function mysample() for example to populate a new table with
 * the sample. E.g.,
 *
 * INSERT INTO sample_table (SELECT * FROM mysample());
 *
 */

str
SAMPLEuniform(bat *r, bat *b, lng *s) {
	BAT *br, *bb;

	if ((bb = BATdescriptor(*b)) == NULL) {
		throw(MAL, "sample.subuniform", INTERNAL_BAT_ACCESS);
	}
	br = BATsample(bb, (BUN) *s);
	BBPunfix(bb->batCacheid);
	if (br == NULL)
		throw(MAL, "sample.subuniform", OPERATION_FAILED);

	BBPkeepref(*r = br->batCacheid);
	return MAL_SUCCEED;

}

str
SAMPLEuniform_dbl(bat *r, bat *b, dbl *p) {
	BAT *bb;
	double pr = *p;
	lng s;

	if ( pr < 0.0 || pr > 1.0 ) {
		throw(MAL, "sample.subuniform", ILLEGAL_ARGUMENT
				" p should be between 0 and 1.0" );
	} else if (pr == 0) {/* special case */
		s = 0;
		return SAMPLEuniform(r, b, &s);
	}
	if ((bb = BATdescriptor(*b)) == NULL) {
		throw(MAL, "sample.subuniform", INTERNAL_BAT_ACCESS);
	}
	s = (lng) (pr*(double)BATcount(bb));
	BBPunfix(bb->batCacheid);
	return SAMPLEuniform(r, b, &s);
}
