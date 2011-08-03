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
txtsim_export str CMDqgramselfjoin(BAT **res, BAT *qgram, BAT *id, BAT *pos, BAT *len, flt *c, int *k);

#endif /*_TXTSIM_H*/



