/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "gdk.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

// TODO: Go through this documentation and update it with an explanation about seeds.
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

static str
SAMPLEuniform(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {

	bat *r, *b;
	lng sample_size;
	unsigned seed;
	(void) cntxt;

	BAT *br, *bb;

	r = getArgReference_bat(stk, pci, 0);
	b = getArgReference_bat(stk, pci, 1);

	if ((bb = BATdescriptor(*b)) == NULL) {
		throw(MAL, "sample.subuniform", INTERNAL_BAT_ACCESS);
	}

	if (getArgType(mb, pci, 2) == TYPE_dbl)
	{
		dbl pr = *getArgReference_dbl(stk, pci, 2);

		if ( pr < 0.0 || pr > 1.0 ) {
			BBPunfix(bb->batCacheid);
			throw(MAL, "sample.subuniform", ILLEGAL_ARGUMENT
					" p should be between 0 and 1.0" );
		} else if (pr == 0) {/* special case */
			sample_size = 0;
			// TODO: Add special case for pr == 1.0.
		} else {
			sample_size = (lng) (pr*(double)BATcount(bb));
		}
	} else {
		sample_size = *getArgReference_lng(stk, pci, 2);
	}

	if (pci->argc == 4) {
		seed = (unsigned) *getArgReference_int(stk, pci, 3);
		br = BATsample_with_seed(bb, (BUN) sample_size, seed);
	}
	else {
		br = BATsample(bb, (BUN) sample_size);
	}

	BBPunfix(bb->batCacheid);
	if (br == NULL)
		throw(MAL, "sample.subuniform", OPERATION_FAILED);

	BBPkeepref(*r = br->batCacheid);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func sample_init_funcs[] = {
 pattern("sample", "subuniform", SAMPLEuniform, false, "Returns the oids of a uniform sample of size s", args(1,3, batarg("",oid),batargany("b",0),arg("sample_size",lng))),
 pattern("sample", "subuniform", SAMPLEuniform, false, "Returns the oids of a uniform sample of size s and where the prg is seeded with sample_seed", args(1,4, batarg("",oid),batargany("b",0),arg("sample_size",lng),arg("sample_seed",int))),
 pattern("sample", "subuniform", SAMPLEuniform, false, "Returns the oids of a uniform sample of size = (p x count(b)), where 0 <= p <= 1.0", args(1,3, batarg("",oid),batargany("b",0),arg("p",dbl))),
 pattern("sample", "subuniform", SAMPLEuniform, false, "Returns the oids of a uniform sample of size = (p x count(b)), where 0 <= p <= 1.0 and where the prg is seeded with sample_seed", args(1,4, batarg("",oid),batargany("b",0),arg("p",dbl),arg("sample_seed",int))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_sample_mal)
{ mal_module("sample", NULL, sample_init_funcs); }
