/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @-
 * @+ Implementation
 *
 */
#ifndef _TXTSIM_H
#define _TXTSIM_H
#include "mal.h"
#include <string.h>
#include "gdk.h"
#ifdef HAVE_MALLOC_H
#endif
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define txtsim_export extern __declspec(dllimport)
#else
#define txtsim_export extern __declspec(dllexport)
#endif
#else
#define txtsim_export extern
#endif

txtsim_export str levenshtein_impl(int *result, str *s, str *t, int *insdel_cost, int *replace_cost, int *transpose_cost);
txtsim_export str levenshteinbasic_impl(int *result, str *s, str *t);
txtsim_export str levenshteinbasic2_impl(int *result, str *s, str *t);
txtsim_export str fstrcmp_impl(dbl *ret, str *string1, str *string2, dbl *minimum);
txtsim_export str fstrcmp0_impl(dbl *ret, str *string1, str *string2);
txtsim_export str soundex_impl(str *res, str *Name);
txtsim_export str stringdiff_impl(int *res, str *s1, str*s2);
txtsim_export str CMDqgramnormalize(str *res, str *input);
txtsim_export str CMDqgramselfjoin(bat *res1, bat *res2, bat *qid, bat *bid, bat *pid, bat *lid, flt *c, int *k);
txtsim_export str CMDstr2qgrams(bat *ret, str *val);

#endif /*_TXTSIM_H*/



