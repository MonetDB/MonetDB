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

/* The batstr module functions use a single buffer to avoid malloc/free overhead.
   Note the buffer should be always large enough to hold null strings, so less testing will be required */
#define INITIAL_STR_BUFFER_LENGTH MAX((int) strlen(str_nil) + 1, 1024)

extern int str_length(const char *s);
extern int str_bytes(const char *s);

extern str str_from_wchr(str *buf, size_t *buflen, int c);
extern str str_wchr_at(int *res, const char *s, int at);

extern bit str_is_prefix(const char *s, const char *prefix);
extern bit str_is_suffix(const char *s, const char *suffix);

extern str str_tail(str *buf, size_t *buflen, const char *s, int off);
extern str str_Sub_String(str *buf, size_t *buflen, const char *s, int off, int l);
extern str str_substring_tail(str *buf, size_t *buflen, const char *s, int start);
extern str str_sub_string(str *buf, size_t *buflen, const char *s, int start, int l);
extern str str_suffix(str *buf, size_t *buflen, const char *s, int l);
extern str str_repeat(str *buf, size_t *buflen, const char *s, int c);

extern str str_lower(str *buf, size_t *buflen, const char *s);
extern str str_upper(str *buf, size_t *buflen, const char *s);
extern str str_strip(str *buf, size_t *buflen, const char *s);
extern str str_ltrim(str *buf, size_t *buflen, const char *s);
extern str str_rtrim(str *buf, size_t *buflen, const char *s);

extern int str_search(const char *s, const char *s2);
extern int str_reverse_str_search(const char *s, const char *s2);
extern int str_locate2(const char *needle, const char *haystack, int start);

extern str str_splitpart(str *buf, size_t *buflen, const char *s, const char *s2, int f);
extern str str_insert(str *buf, size_t *buflen, const char *s, int strt, int l, const char *s2);
extern str str_substitute(str *buf, size_t *buflen, const char *s, const char *src, const char *dst, bit repeat);

mal_export str STRStrip2(str *res, const str *arg1, const str *arg2);
mal_export str STRLtrim2(str *res, const str *arg1, const str *arg2);
mal_export str STRRtrim2(str *res, const str *arg1, const str *arg2);
mal_export str STRLpad(str *res, const str *arg1, const int *len);
mal_export str STRRpad(str *res, const str *arg1, const int *len);
mal_export str STRLpad2(str *res, const str *arg1, const int *len, const str *arg2);
mal_export str STRRpad2(str *res, const str *arg1, const int *len, const str *arg2);

mal_export str STRreplace(str *ret, const str *s1, const str *s2, const str *s3);

#endif /* __string_H__ */
