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
 * Copyright August 2008-2011 MonetDB B.V.
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
 * The implementation of the uniform sampling is based on the algorithm A as
 * described in the paper "Faster Methods for Random Sampling" by Jeffrey Scott
 * Vitter. Algorithm A is not the fastest one, but it only makes s calls in
 * function random() and it is simpler than the other more complex and CPU
 * intensive algorithms in the literature.
 *
 * Algorithm A instead of performing one random experiment for each row to
 * decide if it should be included in the sample or not, it skips S rows
 * and includes the S+1 row. The algorithm scans the input relation
 * sequentially and maintains the unique and sort properties. The sample is
 * without replacement.
 */

sample_export str
SAMPLEuniform(bat *r, bat *b, ptr s) {
	BAT *br, *bb;
	BUN p, sz = *(wrd *)s, top, N, n, jump;
	BATiter iter;
	double v, quot;

	if ((bb = BATdescriptor(*b)) == NULL) {
		throw(MAL, "sample.uniform", INTERNAL_BAT_ACCESS);
	}

	N = BATcount(bb);
	if  (sz > N) { /* if the sample is bigger than the input relation */
		BBPkeepref(*r = bb->batCacheid);
		return MAL_SUCCEED;
	}

	if ((br = BATnew(TYPE_oid, bb->ttype, sz)) == NULL) {
		BBPunfix(bb->batCacheid);
		throw(MAL, "sample.uniform", MAL_MALLOC_FAIL);
	}

	n = sz;
	top = N-n;
	p = BUNfirst(bb)-1;
	iter = bat_iterator(bb);
	while (n-->1) { /* loop until only 1 free spot is left for the sample */
		v = DRAND;
		jump = 0;
		quot = (double)top/(double)N;
		while (quot > v) { /* determine how many positions to jump */
			jump++;
			top--;
			N--;
			quot *= (double)top/(double)N;
		}
		p += (jump+1);
		N--;
		bunfastins(br, BUNhead(iter, p), BUNtail(iter,p));
	}
	/* 1 row left to be added in the sample */
	p += (BUN) rand() % N;
	bunfastins(br, BUNhead(iter, p+1), BUNtail(iter,p+1));

	br->tsorted = bb->tsorted;
	br->hsorted = bb->hsorted;
	br->tkey = bb->tkey;
	br->hkey = bb->hkey;
	br->hdense = FALSE;
	BATseqbase(br, bb->hseqbase);
	BATsetcount(br, sz);

	BBPunfix(bb->batCacheid);
	BBPkeepref(*r = br->batCacheid);
	return MAL_SUCCEED;

	bunins_failed:
	BBPunfix(bb->batCacheid);
	BBPunfix(br->batCacheid);
	throw(MAL, "sample.uniform", OPERATION_FAILED "bunfastins");
}

