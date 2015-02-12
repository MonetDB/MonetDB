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
 * Copyright August 2008-2015 MonetDB B.V.
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

str_export str strPrelude(void *ret);
str_export str strEpilogue(void *ret);
str_export str STRtostr(str *res, const str *src);
str_export str STRConcat(str *res, const str *val1, const str *val2);
str_export str STRLength(int *res, const str *arg1);
/* length of rtrimed string, needed for sql */
str_export str STRstringLength(int *res, const str *s);
str_export str STRBytes(int *res, const str *arg1);
str_export str STRTail(str *res, const str *arg1, const int *offset);
str_export str STRSubString(str *res, const str *arg1, const int *offset, const int *length);
str_export str STRFromWChr(str *res, const int *at);
str_export str STRWChrAt(int *res, const str *arg1, const int *at);
str_export str STRcodeset(str *res);
str_export str STRIconv(str *res, const str *o, const str *fp, const str *tp);
str_export str STRPrefix(bit *res, const str *arg1, const str *arg2);
str_export str STRSuffix(bit *res, const str *arg1, const str *arg2);
str_export str STRLower(str *res, const str *arg1);
str_export str STRUpper(str *res, const str *arg1);
str_export str STRstrSearch(int *res, const str *arg1, const str *arg2);
str_export str STRReverseStrSearch(int *res, const str *arg1, const str *arg2);
str_export str STRStrip(str *res, const str *arg1);
str_export str STRLtrim(str *res, const str *arg1);
str_export str STRRtrim(str *res, const str *arg1);
str_export str STRStrip2(str *res, const str *arg1, const str *arg2);
str_export str STRLtrim2(str *res, const str *arg1, const str *arg2);
str_export str STRRtrim2(str *res, const str *arg1, const str *arg2);
str_export str STRLpad(str *res, const str *arg1, const int *len);
str_export str STRRpad(str *res, const str *arg1, const int *len);
str_export str STRLpad2(str *res, const str *arg1, const int *len, const str *arg2);
str_export str STRRpad2(str *res, const str *arg1, const int *len, const str *arg2);
str_export str STRSubstitute(str *res, const str *arg1, const str *arg2, const str *arg3, const bit *g);

str_export str STRSQLLength(int *res, const str *s);
str_export str STRsubstringTail(str *ret, const str *s, const int *start);
str_export str STRsubstring(str *ret, const str *s, const int *start, const int *l);
str_export str STRlikewrap2(bit *ret, const str *s, const str *pat);
str_export str STRlikewrap(bit *ret, const str *s, const str *pat, const str *esc);
str_export str STRascii(int *ret, const str *s);
str_export str STRprefix(str *ret, const str *s, const int *l);
str_export str STRsuffix(str *ret, const str *s, const int *l);
str_export str STRlocate(int *ret, const str *s1, const str *s2);
str_export str STRlocate2(int *ret, const str *s1, const str *s2, const int *start);
str_export str STRinsert(str *ret, const str *s, const int *start, const int *l, const str *s2);
str_export str STRreplace(str *ret, const str *s1, const str *s2, const str *s3);
str_export str STRrepeat(str *ret, const str *s, const int *c);
str_export str STRspace(str *ret, const int *l);

#endif /* __string_H__ */
