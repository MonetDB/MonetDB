/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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

static void
finalize_ouput_copy_sorted_property(bat *res, BAT *bn, str msg, bool nils, BUN q, bool istsorted, bool istrevsorted)
{
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = istsorted || BATcount(bn) <= 1;
		bn->trevsorted = istrevsorted || BATcount(bn) <= 1;
		*res = bn->batCacheid;
		BBPkeepref(bn);
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
		BBPreclaim(b);
	}
	va_end(valist);
}

#define TYPE flt
#include "sql_fround_impl.h"
#undef TYPE

#define TYPE dbl
#include "sql_fround_impl.h"
#undef TYPE
