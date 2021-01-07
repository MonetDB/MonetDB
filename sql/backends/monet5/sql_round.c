/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "mal_instruction.h"

#define CONCAT_2(a, b)		a##b
#define CONCAT_3(a, b, c)	a##b##c

#define NIL(t)			CONCAT_2(t, _nil)
#define ISNIL(t)		CONCAT_3(is_, t, _nil)
#define TPE(t)			CONCAT_2(TYPE_, t)
#define GDKmin(t)		CONCAT_3(GDK_, t, _min)
#define GDKmax(t)		CONCAT_3(GDK_, t, _max)
#define FUN(a, b)		CONCAT_3(a, _, b)

#define STRING(a)		#a

#define BIG lng			/* a larger type */

static void
finalize_ouput_copy_sorted_property(bat *res, BAT *bn, BAT *b, str msg, bool nils, BUN q, bool try_copy_sorted)
{
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = ((try_copy_sorted && b && b->tsorted) || BATcount(bn) <= 1);
		bn->trevsorted = ((try_copy_sorted && b && b->trevsorted) || BATcount(bn) <= 1);
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
}

static void
unfix_inputs(int nargs, ...)
{
	va_list valist;

	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		BAT *b = va_arg(valist, BAT *);
		if (b)
			BBPunfix(b->batCacheid);
	}
	va_end(valist);
}

#define TYPE bte
#include "sql_round_impl.h"
#undef TYPE

#define TYPE sht
#include "sql_round_impl.h"
#undef TYPE

#define TYPE int
#include "sql_round_impl.h"
#undef TYPE

#define TYPE lng
#include "sql_round_impl.h"
#undef TYPE

#ifdef HAVE_HGE
#undef BIG
#define BIG hge
#define TYPE hge
#include "sql_round_impl.h"
#undef TYPE
#endif
