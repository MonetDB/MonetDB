/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
#include <limits.h>

mal_export str levenshtein_impl(int *result, str *s, str *t, int *insdel_cost, int *replace_cost, int *transpose_cost);
mal_export str levenshteinbasic_impl(int *result, str *s, str *t);
mal_export str levenshteinbasic2_impl(int *result, str *s, str *t);
mal_export str fstrcmp_impl(dbl *ret, str *string1, str *string2, dbl *minimum);
mal_export str fstrcmp0_impl(dbl *ret, str *string1, str *string2);
mal_export str soundex_impl(str *res, str *Name);
mal_export str stringdiff_impl(int *res, str *s1, str*s2);
mal_export str CMDqgramnormalize(str *res, str *input);
mal_export str CMDqgramselfjoin(bat *res1, bat *res2, bat *qid, bat *bid, bat *pid, bat *lid, flt *c, int *k);
mal_export str CMDstr2qgrams(bat *ret, str *val);

#endif /*_TXTSIM_H*/



