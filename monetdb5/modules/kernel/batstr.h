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

#ifndef _BATSTR_H_
#define _BATSTR_H_
#include "monetdb_config.h"
#include <gdk.h>
#include "ctype.h"
#include <string.h>
#include "mal_exception.h"
#include "str.h"

#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif
#ifdef HAVE_ICONV_H
#include <iconv.h>
#endif

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define batstr_export extern __declspec(dllimport)
#else
#define batstr_export extern __declspec(dllexport)
#endif
#else
#define batstr_export extern
#endif

batstr_export str STRbatPrefix(int *ret, int *l, int *r);
batstr_export str STRbatPrefixcst(int *ret, int *l, str *cst);
batstr_export str STRcstPrefixbat(int *ret, str *cst, int *r);
batstr_export str STRbatSuffix(int *ret, int *l, int *r);
batstr_export str STRbatSuffixcst(int *ret, int *l, str *cst);
batstr_export str STRcstSuffixbat(int *ret, str *cst, int *r);
batstr_export str STRbatstrSearch(int *ret, int *l, int *r);
batstr_export str STRbatstrSearchcst(int *ret, int *l, str *cst);
batstr_export str STRcststrSearchbat(int *ret, str *cst, int *r);
batstr_export str STRbatRstrSearch(int *ret, int *l, int *r);
batstr_export str STRbatRstrSearchcst(int *ret, int *l, str *cst);
batstr_export str STRcstRstrSearchbat(int *ret, str *cst, int *r);
batstr_export str STRbatConcat(int *ret, int *l, int *r);
batstr_export str STRbatConcatcst(int *ret, int *l, str *cst);
batstr_export str STRcstConcatbat(int *ret, str *cst, int *r);
batstr_export str STRbatTail(int *ret, int *l, int *r);
batstr_export str STRbatTailcst(int *ret, int *l, int *cst);
batstr_export str STRbatWChrAt(int *ret, int *l, int *r);
batstr_export str STRbatWChrAtcst(int *ret, int *l, int *cst);
batstr_export str STRbatSubstitutecst(int *ret, int *l, str *arg2, str *arg3, bit *rep);

batstr_export str STRbatLower(int *ret, int *l);
batstr_export str STRbatUpper(int *ret, int *l);
batstr_export str STRbatStrip(int *ret, int *l);
batstr_export str STRbatLtrim(int *ret, int *l);
batstr_export str STRbatRtrim(int *ret, int *l);

batstr_export str STRbatLength(int *ret, int *l);
batstr_export str STRbatstringLength(int *ret, int *l);
batstr_export str STRbatBytes(int *ret, int *l);

batstr_export str STRbatSubstitutecst(int *ret, int *l, str *arg2, str *arg3, bit *rep);
batstr_export str STRbatlike_uselect(int *ret, int *bid, str *pat, str *esc);
batstr_export str STRbatlike_uselect2(int *ret, int *bid, str *pat);
batstr_export str STRbatsubstringcst(int *ret, int *bid, int *start, int *length);
batstr_export str STRbatsubstring(int *ret, int *l, int *r, int *t);
batstr_export str STRbatreplace(int *ret, int *l, str *pat, str *s2);

batstr_export str STRbatlike_uselect(int *ret, int *bid, str *pat, str *esc);
batstr_export str STRbatlike_uselect2(int *ret, int *bid, str *pat);
batstr_export str STRbatsubstringcst(int *ret, int *bid, int *start, int *length);
batstr_export str STRbatsubstring(int *ret, int *l, int *r, int *t);
batstr_export str STRbatreplace(int *ret, int *l, str *pat, str *s2);

#endif
