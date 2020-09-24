/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef __string_H__
#define __string_H__
#include "gdk.h"
#include "mal.h"
#include "mal_exception.h"
#include <ctype.h>

#define INITIAL_STR_BUFFER_LENGTH MAX((int) strlen(str_nil) + 1, 1024)

int str_length(const char *s);
int str_bytes(const char *s);

str str_from_wchr(str *buf, int *buflen, int c);
str str_wchr_at(int *res, const char *s, int at);

bit str_is_prefix(const char *s, const char *prefix);
bit str_is_suffix(const char *s, const char *suffix);

str str_tail(str *buf, int *buflen, const char *s, int off);
str str_Sub_String(str *buf, int *buflen, const char *s, int off, int l);
str str_substring_tail(str *buf, int *buflen, const char *s, int start);
str str_sub_string(str *buf, int *buflen, const char *s, int start, int l);
str str_suffix(str *buf, int *buflen, const char *s, int l);
str str_repeat(str *buf, int *buflen, const char *s, int c);

mal_export str STRtostr(str *res, const str *src);
mal_export str STRLength(int *res, const str *arg1);
mal_export str STRBytes(int *res, const str *arg1);
mal_export str STRTail(str *res, const str *arg1, const int *offset);
mal_export str STRSubString(str *res, const str *arg1, const int *offset, const int *length);
mal_export str STRFromWChr(str *res, const int *at);
mal_export str STRWChrAt(int *res, const str *arg1, const int *at);
mal_export str STRPrefix(bit *res, const str *arg1, const str *arg2);
mal_export str STRSuffix(bit *res, const str *arg1, const str *arg2);
mal_export str STRLower(str *res, const str *arg1);
mal_export str STRUpper(str *res, const str *arg1);
mal_export str STRstrSearch(int *res, const str *arg1, const str *arg2);
mal_export str STRReverseStrSearch(int *res, const str *arg1, const str *arg2);
mal_export str STRsplitpart(str *res, str *haystack, str *needle, int *field);
mal_export str STRStrip(str *res, const str *arg1);
mal_export str STRLtrim(str *res, const str *arg1);
mal_export str STRRtrim(str *res, const str *arg1);
mal_export str STRStrip2(str *res, const str *arg1, const str *arg2);
mal_export str STRLtrim2(str *res, const str *arg1, const str *arg2);
mal_export str STRRtrim2(str *res, const str *arg1, const str *arg2);
mal_export str STRLpad(str *res, const str *arg1, const int *len);
mal_export str STRRpad(str *res, const str *arg1, const int *len);
mal_export str STRLpad2(str *res, const str *arg1, const int *len, const str *arg2);
mal_export str STRRpad2(str *res, const str *arg1, const int *len, const str *arg2);
mal_export str STRSubstitute(str *res, const str *arg1, const str *arg2, const str *arg3, const bit *g);

mal_export str STRsubstringTail(str *ret, const str *s, const int *start);
mal_export str STRsubstring(str *ret, const str *s, const int *start, const int *l);
mal_export str STRlikewrap2(bit *ret, const str *s, const str *pat);
mal_export str STRlikewrap(bit *ret, const str *s, const str *pat, const str *esc);
mal_export str STRascii(int *ret, const str *s);
mal_export str STRprefix(str *ret, const str *s, const int *l);
mal_export str STRsuffix(str *ret, const str *s, const int *l);
mal_export str STRlocate(int *ret, const str *s1, const str *s2);
mal_export str STRlocate2(int *ret, const str *s1, const str *s2, const int *start);
mal_export str STRinsert(str *ret, const str *s, const int *start, const int *l, const str *s2);
mal_export str STRreplace(str *ret, const str *s1, const str *s2, const str *s3);
mal_export str STRrepeat(str *ret, const str *s, const int *c);
mal_export str STRspace(str *ret, const int *l);

#endif /* __string_H__ */
