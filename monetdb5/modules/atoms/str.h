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

#ifndef __string_H__
#define __string_H__
#include <gdk.h>
#include "mal.h"
#include "mal_exception.h"
#include "ctype.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define str_export extern __declspec(dllimport)
#else
#define str_export extern __declspec(dllexport)
#endif
#else
#define str_export extern
#endif

str_export bat *strPrelude(void);
str_export str strEpilogue(void);
str_export str STRtostr(str *res, str *src);
str_export str STRConcat(str *res, str *val1, str *val2);
str_export str STRLength(int *res, str *arg1);
/* length of rtrimed string, needed for sql */
str_export str STRstringLength(int *res, str *s);
str_export str STRBytes(int *res, str *arg1);
str_export str STRTail(str *res, str *arg1, int *offset);
str_export str STRSubString(str *res, str *arg1, int *offset, int *length);
str_export str STRFromWChr(str *res, int *at);
str_export str STRWChrAt(int *res, str *arg1, int *at);
str_export str STRcodeset(str *res);
str_export str STRIconv(str *res, str *o, str *fp, str *tp);
str_export str STRPrefix(bit *res, str *arg1, str *arg2);
str_export str STRSuffix(bit *res, str *arg1, str *arg2);
str_export str STRLower(str *res, str *arg1);
str_export str STRUpper(str *res, str *arg1);
str_export str STRstrSearch(int *res, str *arg1, str *arg2);
str_export str STRReverseStrSearch(int *res, str *arg1, str *arg2);
str_export str STRStrip(str *res, str *arg1);
str_export str STRLtrim(str *res, str *arg1);
str_export str STRRtrim(str *res, str *arg1);
str_export str STRStrip2(str *res, str *arg1, str *arg2);
str_export str STRLtrim2(str *res, str *arg1, str *arg2);
str_export str STRRtrim2(str *res, str *arg1, str *arg2);
str_export str STRLpad(str *res, str *arg1, size_t *len);
str_export str STRRpad(str *res, str *arg1, size_t *len);
str_export str STRLpad2(str *res, str *arg1, size_t *len, str *arg2);
str_export str STRRpad2(str *res, str *arg1, size_t *len, str *arg2);
str_export str STRmin_no_nil(str *res, str *left, str *right);
str_export str STRmax_no_nil(str *res, str *left, str *right);
str_export str STRmin(str *res, str *left, str *right);
str_export str STRmax(str *res, str *left, str *right);
str_export str STRSubstitute(str *res, str *arg1, str *arg2, str *arg3, bit *g);

str_export str STRSQLLength(int *res, str *s);
str_export str STRfindUnescapedOccurrence(str b, str c, str esc);
str_export str STRsubstringTail(str *ret, str *s, int *start);
str_export str STRsubstring(str *ret, str *s, int *start, int *l);
str_export str STRlikewrap2(bit *ret, str *s, str *pat);
str_export str STRlikewrap(bit *ret, str *s, str *pat, str *esc);
str_export str STRascii(int *ret, str *s);
str_export str STRprefix(str *ret, str *s, int *l);
str_export str STRsuffix(str *ret, str *s, int *l);
str_export str STRlocate(int *ret, str *s1, str *s2);
str_export str STRlocate2(int *ret, str *s1, str *s2, int *start);
str_export str STRinsert(str *ret, str *s, int *start, int *l, str *s2);
str_export str STRreplace(str *ret, str *s1, str *s2, str *s3);
str_export str STRrepeat(str *ret, str *s, int *c);
str_export str STRspace(str *ret, int *l);

#endif /* __string_H__ */
