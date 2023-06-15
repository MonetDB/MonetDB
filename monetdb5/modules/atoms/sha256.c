/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal.h"
#include "mal_exception.h"

#define SHA256_BYTES 32
#define SHA256_HEX 64

typedef struct sha256_t {
		unsigned char bytes[SHA256_BYTES];
} sha256;

/* Sha256 is a no nil support type !*/

/* no nil support, ie nil value == error here */
/*
354b7196c9ba5fb4b21cf615bb6ec4cd5c07503c34229feef033fc081a8c03f4
*/
static ssize_t
SHA256fromString(const char *src, size_t *len, void **RETVAL, bool external)
{
	sha256 **retval = (sha256 **) RETVAL;

	if (*len < sizeof(sha256) || *retval == NULL) {
		GDKfree(*retval);
		*retval = GDKzalloc(sizeof(sha256));
		if( *retval == NULL){
			*len = 0;
			return -1;
		}
		*len = sizeof(sha256);
	}

	/* handle the nil string */
	if ((external && strcmp(src, "nil") == 0) || strNil(src)) {
		GDKerror("Error while parsing, unexpected string '%s'", src);
		return -1;
	}

	/* hex to value */
	unsigned char *ret = (*retval)->bytes;
	int i = 0, cur = 0, err = 0;
    unsigned char res = 0;

	while (err == 0 && i < SHA256_HEX) {
		if (i%2)
			res <<= 4;
		if (isdigit((unsigned char) src[i]))
			res = res + (src[i] - '0');
		else if ('A' <= src[i] && src[i] <= 'F')
			res = res + (src[i] - 'A' + 10);
		else if ('a' <= src[i] && src[i] <= 'f')
			res = res + (src[i] - 'a' + 10);
		else
			err = 1;
		if (i%2) {
			ret[cur++] = res;
			res = 0;
		}
		i++;
	}
	if (err || i < SHA256_HEX || src[i]) {
		GDKerror("Error while parsing, unexpected string '%s'", src);
		return -1;
	}
	return SHA256_HEX;
}

/**
 * Returns the string representation of the given sha256 value.
 * Warning: GDK function
 * Returns the length of the string
 */
static inline char
hex(unsigned char v)
{
	assert(v<=15);
	return (v<10)?'0'+v:'A'+(v-10);
}

static ssize_t
SHA256toString(str *retval, size_t *len, const void *handle, bool external)
{
	(void)external;
	if (*len < (SHA256_HEX+1) || *retval == NULL) {
		GDKfree(*retval);
		*retval = GDKmalloc(sizeof(char) * (*len = (SHA256_HEX+1)));
		if( *retval == NULL)
			return -1;
	}

	const sha256 *src = (const sha256 *)handle;
	const unsigned char *value = src->bytes;

	char *dst = *retval;
	int i = 0;
	for(int cur = 0; cur<SHA256_BYTES; cur++, i+=2) {
		dst[i+0] = hex(value[cur]>>4);
		dst[i+1] = hex(value[cur]&15);
	}
	dst[i] = 0;
	return i;
}

static int
SHA256compare(const void *L, const void *R)
{
	const sha256 *ll = L, *rr = R;
	const unsigned char *l = ll->bytes, *r = rr->bytes;

	for(int i = 0; i<SHA256_BYTES; i++) {
		if (l[i] == r[i])
			continue;
		return l[i] - r[i];
	}
	return 0;
}

static BUN
SHA256hash(const void *L)
{
	const sha256 *ll = L;
	const lng *l = (lng*)ll->bytes;

	return ATOMhash(TYPE_lng, l+0) ^ ATOMhash(TYPE_lng, l+1) ^
		   ATOMhash(TYPE_lng, l+2) ^ ATOMhash(TYPE_lng, l+3);
}

static str
SHA256_fromstr(sha256 *ret, str *s)
{
	size_t len = sizeof(sha256);
	if (SHA256fromString(*s, &len, (void **) &ret, false) < 0)
		throw(MAL, "calc.sha256",  GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/* read sha256 from log */
static void *
SHA256read(void *A, size_t *dstlen, stream *s, size_t cnt)
{
	unsigned char *a = A;

	if (a == NULL || *dstlen < cnt * sizeof(sha256)) {
		if ((a = GDKrealloc(a, cnt * sizeof(sha256))) == NULL)
			return NULL;
		*dstlen = cnt * sizeof(sha256);
	}
	if (mnstr_read(s, a, SHA256_BYTES, cnt) < 0) {
		if (a != A)
			GDKfree(a);
		return NULL;
	}
	return a;
}

/* write sha256 to log */
static gdk_return
SHA256write(const void *C, stream *s, size_t cnt)
{
	const unsigned char *c = C;

	if (mnstr_write(s, c, SHA256_BYTES, cnt) < 0)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

/* TODO
 */
#include "mel.h"
mel_atom sha256_init_atoms[] = {
 { .name="sha256", .size=sizeof(sha256), .cmp=SHA256compare, .fromstr=SHA256fromString, .tostr=SHA256toString, .hash=SHA256hash, .read=SHA256read, .write=SHA256write },  { .cmp=NULL }
};
mel_func sha256_init_funcs[] = {
 command("calc", "sha256", SHA256_fromstr, false, "Convert a string to an sha256", args(1,2, arg("",sha256),arg("s",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_sha256_mal)
{ mal_module("sha256", sha256_init_atoms, sha256_init_funcs); }
