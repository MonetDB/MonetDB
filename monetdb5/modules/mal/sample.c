/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
SAMPLEuniform(bat *r, bat *b, ptr s) {
	BAT *br, *bb;

	if ((bb = BATdescriptor(*b)) == NULL) {
		throw(MAL, "sample.subuniform", INTERNAL_BAT_ACCESS);
	}
	br = BATsample(bb,*(BUN *)s);
	if (br == NULL)
		throw(MAL, "sample.subuniform", OPERATION_FAILED);

	BBPunfix(bb->batCacheid);
	BBPkeepref(*r = br->batCacheid);
	return MAL_SUCCEED;

}

str
SAMPLEuniform_dbl(bat *r, bat *b, ptr p) {
	BAT *bb;
	double pr = *(double *)p;
	wrd s;

	if ( pr < 0.0 || pr > 1.0 ) {
		throw(MAL, "sample.uniform", ILLEGAL_ARGUMENT
				" p should be between 0 and 1.0" );
	} else if (pr == 0) {/* special case */
		s = 0;
		return SAMPLEuniform(r, b, (ptr)&s);
	}
	if ((bb = BATdescriptor(*b)) == NULL) {
		throw(MAL, "sample.uniform", INTERNAL_BAT_ACCESS);
	}
	s = (wrd) (pr*(double)BATcount(bb));
	BBPunfix(bb->batCacheid);
	return SAMPLEuniform(r, b, (ptr) &s);
}
