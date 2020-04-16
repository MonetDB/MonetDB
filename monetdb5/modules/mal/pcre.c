/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * N. Nes
 * PCRE library interface
 * The  PCRE library is a set of functions that implement regular
 * expression pattern matching using the same syntax  and  semantics  as  Perl,
 * with  just  a  few  differences.  The  current  implementation of PCRE
 * (release 4.x) corresponds approximately with Perl 5.8, including  support
 * for  UTF-8  encoded  strings.   However,  this support has to be
 * explicitly enabled; it is not the default.
 *
 * ftp://ftp.csx.cam.ac.uk/pub/software/programming/pcre
 */
#include "monetdb_config.h"
#include <string.h>

#include "mal.h"
#include "mal_exception.h"

#include <wchar.h>
#include <wctype.h>

#ifdef HAVE_LIBPCRE
#include <pcre.h>
#ifndef PCRE_STUDY_JIT_COMPILE
/* old library version on e.g. EPEL 6 */
#define pcre_free_study(x)		pcre_free(x)
#define PCRE_STUDY_JIT_COMPILE	0
#endif
#define JIT_COMPILE_MIN	1024	/* when to try JIT compilation of patterns */

#else

#include <regex.h>

typedef regex_t pcre;
#endif

mal_export str pcre_init(void *ret);

mal_export str PCREquote(str *r, const str *v);
mal_export str PCREmatch(bit *ret, const str *val, const str *pat);
mal_export str PCREimatch(bit *ret, const str *val, const str *pat);
mal_export str PCREindex(int *ret, const pcre *pat, const str *val);
mal_export str PCREpatindex(int *ret, const str *pat, const str *val);
mal_export str PCREreplace_wrap(str *res, const str *or, const str *pat, const str *repl, const str *flags);
mal_export str PCREreplace_bat_wrap(bat *res, const bat *or, const str *pat, const str *repl, const str *flags);
mal_export str PCREreplacefirst_wrap(str *res, const str *or, const str *pat, const str *repl, const str *flags);
mal_export str PCREreplacefirst_bat_wrap(bat *res, const bat *or, const str *pat, const str *repl, const str *flags);
mal_export str PCREsql2pcre(str *ret, const str *pat, const str *esc);

mal_export str PCRElike3(bit *ret, const str *s, const str *pat, const str *esc);
mal_export str PCRElike2(bit *ret, const str *s, const str *pat);
mal_export str PCREnotlike3(bit *ret, const str *s, const str *pat, const str *esc);
mal_export str PCREnotlike2(bit *ret, const str *s, const str *pat);
mal_export str BATPCRElike(bat *ret, const bat *b, const str *pat, const str *esc);
mal_export str BATPCRElike2(bat *ret, const bat *b, const str *pat);
mal_export str BATPCREnotlike(bat *ret, const bat *b, const str *pat, const str *esc);
mal_export str BATPCREnotlike2(bat *ret, const bat *b, const str *pat);
mal_export str PCREilike3(bit *ret, const str *s, const str *pat, const str *esc);
mal_export str PCREilike2(bit *ret, const str *s, const str *pat);
mal_export str PCREnotilike3(bit *ret, const str *s, const str *pat, const str *esc);
mal_export str PCREnotilike2(bit *ret, const str *s, const str *pat);
mal_export str BATPCREilike(bat *ret, const bat *b, const str *pat, const str *esc);
mal_export str BATPCREilike2(bat *ret, const bat *b, const str *pat);
mal_export str BATPCREnotilike(bat *ret, const bat *b, const str *pat, const str *esc);
mal_export str BATPCREnotilike2(bat *ret, const bat *b, const str *pat);

mal_export str PCRElikeselect2(bat *ret, const bat *bid, const bat *sid, const str *pat, const str *esc, const bit *caseignore, const bit *anti);
mal_export str PCRElikeselect1(bat *ret, const bat *bid, const bat *cid, const str *pat, const str *esc, const bit *anti);
mal_export str PCRElikeselect3(bat *ret, const bat *bid, const bat *sid, const str *pat, const str *esc, const bit *anti);
mal_export str PCRElikeselect4(bat *ret, const bat *bid, const bat *cid, const str *pat, const bit *anti);
mal_export str PCRElikeselect5(bat *ret, const bat *bid, const bat *sid, const str *pat, const bit *anti);

mal_export str LIKEjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const str *esc, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);
mal_export str LIKEjoin1(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);
mal_export str ILIKEjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const str *esc, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);
mal_export str ILIKEjoin1(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate);

/* current implementation assumes simple %keyword% [keyw%]* */
typedef struct RE {
	char *k;
	uint32_t *w;
	bool search;
	size_t len;
	struct RE *n;
} RE;

/* We cannot use strcasecmp and strncasecmp since they work byte for
 * byte and don't deal with multibyte encodings (such as UTF-8).
 *
 * We implement our own conversion from UTF-8 encoding to Unicode code
 * points which we store in uint32_t.  The reason for this is,
 * functions like mbsrtowcs are locale-dependent (so we need a UTF-8
 * locale to use them), and on Windows, wchar_t is only 2 bytes and
 * therefore cannot hold all Unicode code points.  We do use functions
 * such as towlower to convert a Unicode code point to its lower-case
 * equivalent, but again on Windows, if the code point doesn't fit in
 * 2 bytes, we skip this conversion and compare the unconverted code
 * points.
 *
 * Note, towlower is also locale-dependent, but we don't need a UTF-8
 * locale in order to use it. */

/* helper function to convert a UTF-8 multibyte character to a wide
 * character */
static size_t
utfc8touc(uint32_t *restrict dest, const char *restrict src)
{
	if ((src[0] & 0x80) == 0) {
		*dest = src[0];
		return src[0] != 0;
	} else if ((src[0] & 0xE0) == 0xC0
		   && (src[1] & 0xC0) == 0x80
		   && (src[0] & 0x1E) != 0) {
		*dest = (src[0] & 0x1F) << 6
			| (src[1] & 0x3F);
		return 2;
	} else if ((src[0] & 0xF0) == 0xE0
		   && (src[1] & 0xC0) == 0x80
		   && (src[2] & 0xC0) == 0x80
		   && ((src[0] & 0x0F) != 0
		       || (src[1] & 0x20) != 0)) {
		*dest = (src[0] & 0x0F) << 12
			| (src[1] & 0x3F) << 6
			| (src[2] & 0x3F);
		return 3;
	} else if ((src[0] & 0xF8) == 0xF0
		   && (src[1] & 0xC0) == 0x80
		   && (src[2] & 0xC0) == 0x80
		   && (src[3] & 0xC0) == 0x80) {
		uint32_t c = (src[0] & 0x07) << 18
			| (src[1] & 0x3F) << 12
			| (src[2] & 0x3F) << 6
			| (src[3] & 0x3F);
		if (c < 0x10000
		    || c > 0x10FFFF
		    || (c & 0x1FF800) == 0x00D800)
			return (size_t) -1;
		*dest = c;
		return 4;
	}
	return (size_t) -1;
}

/* helper function to convert a UTF-8 string to a wide character
 * string, the wide character string is allocated */
static uint32_t *
utf8stoucs(const char *src)
{
	uint32_t *dest;
	size_t i = 0;
	size_t j = 0;

	/* count how many uint32_t's we need, while also checking for
	 * correctness of the input */
	while (src[j]) {
		i++;
		if ((src[j+0] & 0x80) == 0) {
			j += 1;
		} else if ((src[j+0] & 0xE0) == 0xC0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+0] & 0x1E) != 0) {
			j += 2;
		} else if ((src[j+0] & 0xF0) == 0xE0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && ((src[j+0] & 0x0F) != 0
			       || (src[j+1] & 0x20) != 0)) {
			j += 3;
		} else if ((src[j+0] & 0xF8) == 0xF0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && (src[j+3] & 0xC0) == 0x80) {
			uint32_t c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
			if (c < 0x10000
			    || c > 0x10FFFF
			    || (c & 0x1FF800) == 0x00D800)
				return NULL;
			j += 4;
		} else {
			return NULL;
		}
	}
	dest = GDKmalloc((i + 1) * sizeof(uint32_t));
	if (dest == NULL)
		return NULL;
	/* go through the source string again, this time we can skip
	 * the correctness tests */
	i = j = 0;
	while (src[j]) {
		if ((src[j+0] & 0x80) == 0) {
			dest[i++] = src[j+0];
			j += 1;
		} else if ((src[j+0] & 0xE0) == 0xC0) {
			dest[i++] = (src[j+0] & 0x1F) << 6
				| (src[j+1] & 0x3F);
			j += 2;
		} else if ((src[j+0] & 0xF0) == 0xE0) {
			dest[i++] = (src[j+0] & 0x0F) << 12
				| (src[j+1] & 0x3F) << 6
				| (src[j+2] & 0x3F);
			j += 3;
		} else if ((src[j+0] & 0xF8) == 0xF0) {
			dest[i++] = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
			j += 4;
		}
	}
	dest[i] = 0;
	return dest;
}

static size_t
myucslen(const uint32_t *ucs)
{
	size_t i = 0;

	while (ucs[i])
		i++;
	return i;
}

static int
mywstrncasecmp(const char *restrict s1, const uint32_t *restrict s2, size_t n2)
{
	uint32_t c1;

	while (n2 > 0) {
		size_t nn1 = utfc8touc(&c1, s1);
		if (nn1 == 0 || nn1 == (size_t) -1)
			return -(*s2 != 0);
		if (*s2 == 0)
			return 1;
		if (nn1 == (size_t) -1 || nn1 == (size_t) -2)
			return 0;	 /* actually an error that shouldn't happen */
#if SIZEOF_WCHAR_T == 2
		if (c1 > 0xFFFF || *s2 > 0xFFFF) {
			if (c1 != *s2)
				return c1 - *s2;
		} else
#endif
		if (towlower((wint_t) c1) != towlower((wint_t) *s2))
			return towlower((wint_t) c1) - towlower((wint_t) *s2);
		s1 += nn1;
		n2--;
		s2++;
	}
	return 0;
}

static int
mystrcasecmp(const char *s1, const char *s2)
{
	uint32_t c1, c2;

	for (;;) {
		size_t nn1 = utfc8touc(&c1, s1);
		size_t nn2 = utfc8touc(&c2, s2);
		if (nn1 == 0 || nn1 == (size_t) -1)
			return -(nn2 != 0 && nn2 != (size_t) -1);
		if (nn2 == 0 || nn2 == (size_t) -1)
			return 1;
		if (nn1 == (size_t) -1 || nn1 == (size_t) -2 ||
			nn2 == (size_t) -1 || nn2 == (size_t) -2)
			return 0;	 /* actually an error that shouldn't happen */
#if SIZEOF_WCHAR_T == 2
		if (c1 > 0xFFFF || c2 > 0xFFFF) {
			if (c1 != c2)
				return c1 - c2;
		} else
#endif
		if (towlower((wint_t) c1) != towlower((wint_t) c2))
			return towlower((wint_t) c1) - towlower((wint_t) c2);
		s1 += nn1;
		s2 += nn2;
	}
}

static int
mywstrcasecmp(const char *restrict s1, const uint32_t *restrict s2)
{
	uint32_t c1;

	for (;;) {
		size_t nn1 = utfc8touc(&c1, s1);
		if (nn1 == 0 || nn1 == (size_t) -1)
			return -(*s2 != 0);
		if (*s2 == 0)
			return 1;
		if (nn1 == (size_t) -1 || nn1 == (size_t) -2)
			return 0;	 /* actually an error that shouldn't happen */
#if SIZEOF_WCHAR_T == 2
		if (c1 > 0xFFFF || *s2 > 0xFFFF) {
			if (c1 != *s2)
				return c1 - *s2;
		} else
#endif
		if (towlower((wint_t) c1) != towlower((wint_t) *s2))
			return towlower((wint_t) c1) - towlower((wint_t) *s2);
		s1 += nn1;
		s2++;
	}
}

static const char *
mywstrcasestr(const char *restrict haystack, const uint32_t *restrict wneedle)
{
	size_t nlen = myucslen(wneedle);

	if (nlen == 0)
		return haystack;

	size_t hlen = strlen(haystack);

	while (*haystack) {
		size_t i;
		size_t h;
		size_t step = 0;
		for (i = h = 0; i < nlen; i++) {
			uint32_t c;
			size_t j = utfc8touc(&c, haystack + h);
			if (j == 0 || j == (size_t) -1)
				return NULL;
			if (i == 0) {
				step = j;
			}
#if SIZEOF_WCHAR_T == 2
			if (c > 0xFFFF || wneedle[i] > 0xFFFF) {
				if (c != wneedle[i])
					break;
			} else
#endif
			if (towlower((wint_t) c) != towlower((wint_t) wneedle[i]))
				break;
			h += j;
		}
		if (i == nlen)
			return haystack;
		haystack += step;
		hlen -= step;
	}
	return NULL;
}

/* returns true if the pattern does not contain unescaped `_' (single
 * character match) and ends with unescaped `%' (any sequence
 * match) */
static bool
re_simple(const char *pat, unsigned char esc)
{
	bool escaped = false;
	bool percatend = false;

	if (pat == 0)
		return 0;
	if (*pat == '%') {
		percatend = true;
		pat++;
	}
	while (*pat) {
		percatend = false;
		if (escaped) {
			escaped = false;
		} else if ((unsigned char) *pat == esc) {
			escaped = true;
		} else if (*pat == '_') {
			return 0;
		} else if (*pat == '%') {
			percatend = true;
		}
		pat++;
	}
	return percatend;
}

static bool
is_strcmpable(const char *pat, const char *esc)
{
	if (pat[strcspn(pat, "%_")])
		return false;
	return strlen(esc) == 0 || strNil(esc) || strstr(pat, esc) == NULL;
}

static bool
re_match_ignore(const char *s, RE *pattern)
{
	RE *r;

	for (r = pattern; r; r = r->n) {
		if (*r->w == 0 && (r->search || *s == 0))
			return true;
		if (!*s ||
			(r->search ? (s = mywstrcasestr(s, r->w)) == NULL : mywstrncasecmp(s, r->w, r->len) != 0))
			return false;
		s += r->len;
	}
	return true;
}

static bool
re_match_no_ignore(const char *s, RE *pattern)
{
	RE *r;

	for (r = pattern; r; r = r->n) {
		if (*r->k == 0 && (r->search || *s == 0))
			return true;
		if (!*s ||
			(r->search ? (s = strstr(s, r->k)) == NULL : strncmp(s, r->k, r->len) != 0))
			return false;
		s += r->len;
	}
	return true;
}

static void
re_destroy(RE *p)
{
	if (p) {
		GDKfree(p->k);
		GDKfree(p->w);
		do {
			RE *n = p->n;

			GDKfree(p);
			p = n;
		} while (p);
	}
}

/* Create a linked list of RE structures.  Depending on the caseignore
 * flag, the w (if true) or the k (if false) field is used.  These
 * fields in the first structure are allocated, whereas in all
 * subsequent structures the fields point into the allocated buffer of
 * the first. */
static RE *
re_create(const char *pat, bool caseignore, uint32_t esc)
{
	RE *r = (RE*)GDKmalloc(sizeof(RE)), *n = r;
	bool escaped = false;

	if (r == NULL)
		return NULL;
	*r = (struct RE) {.search = false};

	while (esc != '%' && *pat == '%') {
		pat++; /* skip % */
		r->search = true;
	}
	if (caseignore) {
		uint32_t *wp;
		uint32_t *wq;
		wp = utf8stoucs(pat);
		if (wp == NULL) {
			GDKfree(r);
			return NULL;
		}
		r->w = wp;
		wq = wp;
		while (*wp) {
			if (escaped) {
				*wq++ = *wp;
				escaped = false;
			} else if (*wp == esc) {
				escaped = true;
			} else if (*wp == '%') {
				n->len = (size_t) (wq - r->w);
				while (wp[1] == '%')
					wp++;
				if (wp[1]) {
					n = n->n = GDKmalloc(sizeof(RE));
					if (n == NULL)
						goto bailout;
					*n = (struct RE) {.search = true, .w = wp + 1};
				}
				*wq++ = 0;
			} else {
				*wq++ = *wp;
			}
			wp++;
		}
	} else {
		char *p, *q;
		if ((p = GDKstrdup(pat)) == NULL) {
			GDKfree(r);
			return NULL;
		}
		r->k = p;
		q = p;
		while (*p) {
			if (escaped) {
				*q++ = *p;
				escaped = false;
			} else if ((unsigned char) *p == esc) {
				escaped = true;
			} else if (*p == '%') {
				n->len = (size_t) (q - r->k);
				while (p[1] == '%')
					p++;
				if (p[1]) {
					n = n->n = GDKmalloc(sizeof(RE));
					if (n == NULL)
						goto bailout;
					*n = (struct RE) {.search = true, .k = p + 1};
				}
				*q++ = 0;
			} else {
				*q++ = *p;
			}
			p++;
		}
	}
	return r;
  bailout:
	re_destroy(r);
	return NULL;
}

#ifdef HAVE_LIBPCRE
static str
pcre_compile_wrap(pcre **res, const char *pattern, bit insensitive)
{
	pcre *r;
	const char *err_p = NULL;
	int errpos = 0;
	int options = PCRE_UTF8 | PCRE_MULTILINE;
	if (insensitive)
		options |= PCRE_CASELESS;

	if ((r = pcre_compile(pattern, options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, "pcre.compile", OPERATION_FAILED
			  " with\n'%s'\nat %d in\n'%s'.\n",
			  err_p, errpos, pattern);
	}
	*res = r;
	return MAL_SUCCEED;
}
#endif

/* these two defines are copies from gdk_select.c */

/* scan select loop with candidates */
#define candscanloop(TEST)												\
	do {																\
		TRC_DEBUG(ALGO,													\
				  "BATselect(b=%s#"BUNFMT",s=%s,anti=%d): "				\
				  "scanselect %s\n", BATgetId(b), BATcount(b),			\
				  s ? BATgetId(s) : "NULL", anti, #TEST);				\
		for (p = 0; p < ci.ncand; p++) {								\
			o = canditer_next(&ci);										\
			r = (BUN) (o - off);										\
			v = BUNtvar(bi, r);											\
			if (TEST)													\
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)			\
					goto bunins_failed;									\
		}																\
	} while (0)

/* scan select loop without candidates */
#define scanloop(TEST)													\
	do {																\
		TRC_DEBUG(ALGO,													\
				  "BATselect(b=%s#"BUNFMT",s=%s,anti=%d): "				\
				  "scanselect %s\n", BATgetId(b), BATcount(b),			\
				  s ? BATgetId(s) : "NULL", anti, #TEST);				\
		while (p < q) {													\
			v = BUNtvar(bi, p-off);										\
			if (TEST) {													\
				o = (oid) p;											\
				if (bunfastappTYPE(oid, bn, &o) != GDK_SUCCEED)			\
					goto bunins_failed;									\
			}															\
			p++;														\
		}																\
	} while (0)

static str
pcre_likeselect(BAT **bnp, BAT *b, BAT *s, const char *pat, bool caseignore, bool anti)
{
#ifdef HAVE_LIBPCRE
	int options = PCRE_UTF8 | PCRE_MULTILINE | PCRE_DOTALL;
	pcre *re;
	pcre_extra *pe;
	const char *error;
	int errpos;
	int ovector[9];
#else
	int options = REG_NEWLINE | REG_NOSUB | REG_EXTENDED;
	regex_t re;
	int errcode;
#endif
	BATiter bi = bat_iterator(b);
	BAT *bn;
	BUN p, q;
	oid o, off;
	const char *v;
	struct canditer ci;

	canditer_init(&ci, b, s);

	assert(ATOMstorage(b->ttype) == TYPE_str);

	if (caseignore) {
#ifdef HAVE_LIBPCRE
		options |= PCRE_CASELESS;
#else
		options |= REG_ICASE;
#endif
	}
#ifdef HAVE_LIBPCRE
	if ((re = pcre_compile(pat, options, &error, &errpos, NULL)) == NULL)
		throw(MAL, "pcre.likeselect",
			  OPERATION_FAILED ": compilation of pattern \"%s\" failed\n", pat);
	pe = pcre_study(re, (s ? BATcount(s) : BATcount(b)) > JIT_COMPILE_MIN ? PCRE_STUDY_JIT_COMPILE : 0, &error);
	if (error != NULL) {
		pcre_free(re);
		throw(MAL, "pcre.likeselect",
			  OPERATION_FAILED ": studying pattern \"%s\" failed\n", pat);
	}
#else
	if ((errcode = regcomp(&re, pat, options)) != 0) {
		throw(MAL, "pcre.likeselect",
			  OPERATION_FAILED ": compilation of pattern \"%s\" failed\n", pat);
	}
#endif
	bn = COLnew(0, TYPE_oid, ci.ncand, TRANSIENT);
	if (bn == NULL) {
#ifdef HAVE_LIBPCRE
		pcre_free_study(pe);
		pcre_free(re);
#else
		regfree(&re);
#endif
		throw(MAL, "pcre.likeselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	off = b->hseqbase;

	if (s && !BATtdense(s)) {
		BUN r;

#ifdef HAVE_LIBPCRE
#define BODY     (pcre_exec(re, pe, v, (int) strlen(v), 0, 0, ovector, 9) >= 0)
#else
#define BODY     (regexec(&re, v, (size_t) 0, NULL, 0) != REG_NOMATCH)
#endif
		if (anti)
			candscanloop(v && *v != '\200' && !BODY);
		else
			candscanloop(v && *v != '\200' && BODY);
	} else {
		if (s) {
			assert(BATtdense(s));
			p = (BUN) s->tseqbase;
			q = p + BATcount(s);
			if ((oid) p < b->hseqbase)
				p = b->hseqbase;
			if ((oid) q > b->hseqbase + BATcount(b))
				q = b->hseqbase + BATcount(b);
		} else {
			p = off;
			q = BUNlast(b) + off;
		}
		if (anti)
			scanloop(v && *v != '\200' && !BODY);
		else
			scanloop(v && *v != '\200' && BODY);
	}
#ifdef HAVE_LIBPCRE
	pcre_free_study(pe);
	pcre_free(re);
#else
	regfree(&re);
#endif
	BATsetcount(bn, BATcount(bn)); /* set some properties */
	bn->theap.dirty |= BATcount(bn) > 0;
	bn->tsorted = true;
	bn->trevsorted = bn->batCount <= 1;
	bn->tkey = true;
	bn->tseqbase = bn->batCount == 0 ? 0 : bn->batCount == 1 ? * (oid *) Tloc(bn, 0) : oid_nil;
	*bnp = bn;
	return MAL_SUCCEED;

  bunins_failed:
	BBPreclaim(bn);
#ifdef HAVE_LIBPCRE
	pcre_free_study(pe);
	pcre_free(re);
#else
	regfree(&re);
#endif
	*bnp = NULL;
	throw(MAL, "pcre.likeselect", OPERATION_FAILED);
}

static str
re_likeselect(BAT **bnp, BAT *b, BAT *s, const char *pat, bool caseignore, bool anti, bool use_strcmp, uint32_t esc)
{
	BATiter bi = bat_iterator(b);
	BAT *bn;
	BUN p, q;
	oid o, off;
	const char *v;
	RE *re = NULL;
	uint32_t *wpat = NULL;

	assert(ATOMstorage(b->ttype) == TYPE_str);

	bn = COLnew(0, TYPE_oid, s ? BATcount(s) : BATcount(b), TRANSIENT);
	if (bn == NULL)
		throw(MAL, "pcre.likeselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	off = b->hseqbase;

	if (!use_strcmp) {
		re = re_create(pat, caseignore, esc);
		if (!re)
			throw(MAL, "pcre.likeselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (s && !BATtdense(s)) {
		struct canditer ci;
		BUN r;

		canditer_init(&ci, b, s);

		if (use_strcmp) {
			if (caseignore) {
				wpat = utf8stoucs(pat);
				if (wpat == NULL)
					throw(MAL, "pcre.likeselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				if (anti)
					candscanloop(v && *v != '\200' &&
								 mywstrcasecmp(v, wpat) != 0);
				else
					candscanloop(v && *v != '\200' &&
								 mywstrcasecmp(v, wpat) == 0);
				GDKfree(wpat);
				wpat = NULL;
			} else {
				if (anti)
					candscanloop(v && *v != '\200' &&
								 strcmp(v, pat) != 0);
				else
					candscanloop(v && *v != '\200' &&
								 strcmp(v, pat) == 0);
			}
		} else {
			if (caseignore) {
				if (anti)
					candscanloop(v && *v != '\200' &&
								 re_match_ignore(v, re) == 0);
				else
					candscanloop(v && *v != '\200' &&
								 re_match_ignore(v, re));
			} else {
				if (anti)
					candscanloop(v && *v != '\200' &&
								 re_match_no_ignore(v, re) == 0);
				else
					candscanloop(v && *v != '\200' &&
								 re_match_no_ignore(v, re));
			}
		}
	} else {
		if (s) {
			assert(BATtdense(s));
			p = (BUN) s->tseqbase;
			q = p + BATcount(s);
			if ((oid) p < b->hseqbase)
				p = b->hseqbase;
			if ((oid) q > b->hseqbase + BATcount(b))
				q = b->hseqbase + BATcount(b);
		} else {
			p = off;
			q = BUNlast(b) + off;
		}
		if (use_strcmp) {
			if (caseignore) {
				wpat = utf8stoucs(pat);
				if (wpat == NULL)
					throw(MAL, "pcre.likeselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				if (anti)
					scanloop(v && *v != '\200' &&
							 mywstrcasecmp(v, wpat) != 0);
				else
					scanloop(v && *v != '\200' &&
							 mywstrcasecmp(v, wpat) == 0);
				GDKfree(wpat);
				wpat = NULL;
			} else {
				if (anti)
					scanloop(v && *v != '\200' &&
							 strcmp(v, pat) != 0);
				else
					scanloop(v && *v != '\200' &&
							 strcmp(v, pat) == 0);
			}
		} else {
			if (caseignore) {
				if (anti)
					scanloop(v && *v != '\200' &&
							 re_match_ignore(v, re) == 0);
				else
					scanloop(v && *v != '\200' &&
							 re_match_ignore(v, re));
			} else {
				if (anti)
					scanloop(v && *v != '\200' &&
							 re_match_no_ignore(v, re) == 0);
				else
					scanloop(v && *v != '\200' &&
							 re_match_no_ignore(v, re));
			}
		}
	}
	BATsetcount(bn, BATcount(bn)); /* set some properties */
	bn->tsorted = true;
	bn->trevsorted = bn->batCount <= 1;
	bn->tkey = true;
	bn->tseqbase = bn->batCount == 0 ? 0 : bn->batCount == 1 ? * (oid *) Tloc(bn, 0) : oid_nil;
	*bnp = bn;
	re_destroy(re);
	return MAL_SUCCEED;

  bunins_failed:
	re_destroy(re);
	GDKfree(wpat);
	BBPreclaim(bn);
	*bnp = NULL;
	throw(MAL, "pcre.likeselect", OPERATION_FAILED);
}

/* maximum number of back references and quoted \ or $ in replacement string */
#define MAX_NR_REFS		20

struct backref {
	int idx;
	int start;
	int end;
};

#ifdef HAVE_LIBPCRE
/* fill in parameter backrefs (length maxrefs) with information about
 * back references in the replacement string; a back reference is a
 * dollar or backslash followed by a number */
static int
parse_replacement(const char *replacement, int len_replacement,
				  struct backref *backrefs, int maxrefs)
{
	int nbackrefs = 0;

	for (int i = 0; i < len_replacement && nbackrefs < maxrefs; i++) {
		if (replacement[i] == '$' || replacement[i] == '\\') {
			char *endptr;
			backrefs[nbackrefs].idx = strtol(replacement + i + 1, &endptr, 10);
			if (endptr > replacement + i + 1) {
				int k = (int) (endptr - (replacement + i + 1));
				backrefs[nbackrefs].start = i;
				backrefs[nbackrefs].end = i + k + 1;
				nbackrefs++;
			} else if (replacement[i] == replacement[i + 1]) {
				/* doubled $ or \, we must copy just one to the output */
				backrefs[nbackrefs].idx = INT_MAX; /* impossible value > 0 */
				backrefs[nbackrefs].start = i;
				backrefs[nbackrefs].end = i + 1;
				i++;			/* don't look at second $ or \ again */
				nbackrefs++;
			}
			/* else: $ or \ followed by something we don't recognize,
			 * so just leave it */
		}
	}
	return nbackrefs;
}

static char *
single_replace(pcre *pcre_code, pcre_extra *extra,
			   const char *origin_str, int len_origin_str,
			   int exec_options, int *ovector, int ovecsize,
			   const char *replacement, int len_replacement,
			   struct backref *backrefs, int nbackrefs,
			   bool global, char *result, int *max_result)
{
	int offset = 0;
	int len_result = 0;
	int addlen;
	char *tmp;

	do {
		int j = pcre_exec(pcre_code, extra, origin_str, len_origin_str, offset,
					  exec_options, ovector, ovecsize);
		if (j <= 0)
			break;
		addlen = ovector[0] - offset + (nbackrefs == 0 ? len_replacement : 0);
		if (len_result + addlen >= *max_result) {
			tmp = GDKrealloc(result, len_result + addlen + 1);
			if (tmp == NULL) {
				GDKfree(result);
				return NULL;
			}
			result = tmp;
			*max_result = len_result + addlen + 1;
		}
		if (ovector[0] > offset) {
			strncpy(result + len_result, origin_str + offset,
					ovector[0] - offset);
			len_result += ovector[0] - offset;
		}
		if (nbackrefs == 0) {
			strncpy(result + len_result, replacement, len_replacement);
			len_result += len_replacement;
		} else {
			int prevend = 0;
			for (int i = 0; i < nbackrefs; i++) {
				int off, len;
				if (backrefs[i].idx >= ovecsize / 3) {
					/* out of bounds, replace with empty string */
					off = 0;
					len = 0;
				} else {
					off = ovector[backrefs[i].idx * 2];
					len = ovector[backrefs[i].idx * 2 + 1] - off;
				}
				addlen = backrefs[i].start - prevend + len;
				if (len_result + addlen >= *max_result) {
					tmp = GDKrealloc(result, len_result + addlen + 1);
					if (tmp == NULL) {
						GDKfree(result);
						return NULL;
					}
					result = tmp;
					*max_result = len_result + addlen + 1;
				}
				if (backrefs[i].start > prevend) {
					strncpy(result + len_result, replacement + prevend,
							backrefs[i].start - prevend);
					len_result += backrefs[i].start - prevend;
				}
				if (len > 0) {
					strncpy(result + len_result, origin_str + off, len);
					len_result += len;
				}
				prevend = backrefs[i].end;
			}
			/* copy rest of replacement string (after last backref) */
			addlen = len_replacement - prevend;
			if (addlen > 0) {
				if (len_result + addlen >= *max_result) {
					tmp = GDKrealloc(result, len_result + addlen + 1);
					if (tmp == NULL) {
						GDKfree(result);
						return NULL;
					}
					result = tmp;
					*max_result = len_result + addlen + 1;
				}
				strncpy(result + len_result, replacement + prevend, addlen);
				len_result += addlen;
			}
		}
		offset = ovector[1];
	} while (offset < len_origin_str && global);
	if (offset < len_origin_str) {
		addlen = len_origin_str - offset;
		if (len_result + addlen >= *max_result) {
			tmp = GDKrealloc(result, len_result + addlen + 1);
			if (tmp == NULL) {
				GDKfree(result);
				return NULL;
			}
			result = tmp;
			*max_result = len_result + addlen + 1;
		}
		strncpy(result + len_result, origin_str + offset, addlen);
		len_result += addlen;
	}
	/* null terminate string */
	result[len_result] = '\0';
	return result;
}
#endif

static str
pcre_replace(str *res, const char *origin_str, const char *pattern,
			 const char *replacement, const char *flags, bool global)
{
#ifdef HAVE_LIBPCRE
	const char *err_p = NULL;
	pcre *pcre_code = NULL;
	pcre_extra *extra;
	char *tmpres;
	int max_result;
	int i, errpos = 0;
	int compile_options = PCRE_UTF8, exec_options = PCRE_NOTEMPTY;
	int *ovector, ovecsize;
	int len_origin_str = (int) strlen(origin_str);
	int len_replacement = (int) strlen(replacement);
	struct backref backrefs[MAX_NR_REFS];
	int nbackrefs = 0;

	while (*flags) {
		switch (*flags) {
		case 'e':
			exec_options &= ~PCRE_NOTEMPTY;
			break;
		case 'i':
			compile_options |= PCRE_CASELESS;
			break;
		case 'm':
			compile_options |= PCRE_MULTILINE;
			break;
		case 's':
			compile_options |= PCRE_DOTALL;
			break;
		case 'x':
			compile_options |= PCRE_EXTENDED;
			break;
		default:
			throw(MAL, global ? "pcre.replace" : "pcre.replace_first",
				  ILLEGAL_ARGUMENT ": unsupported flag character '%c'\n",
				  *flags);
		}
		flags++;
	}

	if ((pcre_code = pcre_compile(pattern, compile_options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, global ? "pcre.replace" : "pcre.replace_first",
			  OPERATION_FAILED ": pcre compile of pattern (%s) failed at %d with\n'%s'.\n",
			  pattern, errpos, err_p);
	}

	/* Since the compiled pattern is going to be used several times, it is
	 * worth spending more time analyzing it in order to speed up the time
	 * taken for matching.
	 */
	extra = pcre_study(pcre_code, 0, &err_p);
	if (err_p != NULL) {
		pcre_free(pcre_code);
		throw(MAL, global ? "pcre.replace" : "pcre.replace_first",
			  OPERATION_FAILED ": pcre study of pattern (%s) failed with '%s'.\n",
			  pattern, err_p);
	}
	pcre_fullinfo(pcre_code, extra, PCRE_INFO_CAPTURECOUNT, &i);
	ovecsize = (i + 1) * 3;
	if ((ovector = (int *) GDKmalloc(sizeof(int) * ovecsize)) == NULL) {
		pcre_free_study(extra);
		pcre_free(pcre_code);
		throw(MAL, global ? "pcre.replace" : "pcre.replace_first",
			  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* identify back references in the replacement string */
	nbackrefs = parse_replacement(replacement, len_replacement,
								  backrefs, MAX_NR_REFS);

	max_result = len_origin_str + 1;
	tmpres = GDKmalloc(max_result);
	if (tmpres == NULL) {
		GDKfree(ovector);
		pcre_free_study(extra);
		pcre_free(pcre_code);
		throw(MAL, global ? "pcre.replace" : "pcre.replace_first",
			  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	tmpres = single_replace(pcre_code, extra, origin_str, len_origin_str,
							exec_options, ovector, ovecsize, replacement,
							len_replacement, backrefs, nbackrefs, global,
							tmpres, &max_result);
	GDKfree(ovector);
	pcre_free_study(extra);
	pcre_free(pcre_code);
	if (tmpres == NULL)
		throw(MAL, global ? "pcre.replace" : "pcre.replace_first",
			  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	*res = tmpres;
	return MAL_SUCCEED;
#else
	(void) res;
	(void) origin_str;
	(void) pattern;
	(void) replacement;
	(void) flags;
	(void) global;
	throw(MAL, global ? "pcre.replace" : "pcre.replace_first",
		  "Database was compiled without PCRE support.");
#endif
}

static str
pcre_replace_bat(BAT **res, BAT *origin_strs, const char *pattern,
				 const char *replacement, const char *flags, bool global)
{
#ifdef HAVE_LIBPCRE
	BATiter origin_strsi = bat_iterator(origin_strs);
	const char *err_p = NULL;
	char *tmpres;
	int i, errpos = 0;
	int compile_options = PCRE_UTF8, exec_options = PCRE_NOTEMPTY;
	pcre *pcre_code = NULL;
	pcre_extra *extra;
	BAT *tmpbat;
	BUN p, q;
	int *ovector, ovecsize;
	int len_replacement = (int) strlen(replacement);
	struct backref backrefs[MAX_NR_REFS];
	int nbackrefs = 0;
	const char *origin_str;
	int max_dest_size = 0;

	while (*flags) {
		switch (*flags) {
		case 'e':
			exec_options &= ~PCRE_NOTEMPTY;
			break;
		case 'i':
			compile_options |= PCRE_CASELESS;
			break;
		case 'm':
			compile_options |= PCRE_MULTILINE;
			break;
		case 's':
			compile_options |= PCRE_DOTALL;
			break;
		case 'x':
			compile_options |= PCRE_EXTENDED;
			break;
		default:
			throw(MAL, global ? "batpcre.replace" : "batpcre.replace_first",
				  ILLEGAL_ARGUMENT ": unsupported flag character '%c'\n",
				  *flags);
		}
		flags++;
	}

	if ((pcre_code = pcre_compile(pattern, compile_options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, global ? "batpcre.replace" : "batpcre.replace_first",
			  OPERATION_FAILED
			  ": pcre compile of pattern (%s) failed at %d with\n'%s'.\n",
			  pattern, errpos, err_p);
	}

	/* Since the compiled pattern is going to be used several times,
	 * it is worth spending more time analyzing it in order to speed
	 * up the time taken for matching.
	 */
	extra = pcre_study(pcre_code, BATcount(origin_strs) > JIT_COMPILE_MIN ? PCRE_STUDY_JIT_COMPILE : 0, &err_p);
	if (err_p != NULL) {
		pcre_free(pcre_code);
		throw(MAL, global ? "batpcre.replace" : "batpcre.replace_first",
			  OPERATION_FAILED);
	}
	pcre_fullinfo(pcre_code, extra, PCRE_INFO_CAPTURECOUNT, &i);
	ovecsize = (i + 1) * 3;
	if ((ovector = (int *) GDKzalloc(sizeof(int) * ovecsize)) == NULL) {
		pcre_free_study(extra);
		pcre_free(pcre_code);
		throw(MAL, global ? "batpcre.replace" : "batpcre.replace_first",
			  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* identify back references in the replacement string */
	nbackrefs = parse_replacement(replacement, len_replacement,
								  backrefs, MAX_NR_REFS);

	tmpbat = COLnew(origin_strs->hseqbase, TYPE_str, BATcount(origin_strs), TRANSIENT);

	/* the buffer for all destination strings is allocated only once,
	 * and extended when needed */
	max_dest_size = len_replacement + 1;
	tmpres = GDKmalloc(max_dest_size);
	if (tmpbat == NULL || tmpres == NULL) {
		pcre_free_study(extra);
		pcre_free(pcre_code);
		GDKfree(ovector);
		BBPreclaim(tmpbat);
		GDKfree(tmpres);
		throw(MAL, global ? "batpcre.replace" : "batpcre.replace_first",
			  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BATloop(origin_strs, p, q) {
		origin_str = BUNtvar(origin_strsi, p);
		tmpres = single_replace(pcre_code, extra, origin_str,
								(int) strlen(origin_str), exec_options,
								ovector, ovecsize, replacement,
								len_replacement, backrefs, nbackrefs, global,
								tmpres, &max_dest_size);
		if (tmpres == NULL || BUNappend(tmpbat, tmpres, false) != GDK_SUCCEED) {
			pcre_free_study(extra);
			pcre_free(pcre_code);
			GDKfree(ovector);
			GDKfree(tmpres);
			BBPreclaim(tmpbat);
			throw(MAL, global ? "batpcre.replace" : "batpcre.replace_first",
				  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	pcre_free_study(extra);
	pcre_free(pcre_code);
	GDKfree(ovector);
	GDKfree(tmpres);
	*res = tmpbat;
	return MAL_SUCCEED;
#else
	(void) res;
	(void) origin_strs;
	(void) pattern;
	(void) replacement;
	(void) flags;
	(void) global;
	throw(MAL, global ? "batpcre.replace" : "batpcre.replace_first",
		  "Database was compiled without PCRE support.");
#endif
}

str
pcre_init(void *ret)
{
	(void) ret;
	return NULL;
}

static str
pcre_match_with_flags(bit *ret, const char *val, const char *pat, const char *flags)
{
	int pos;
#ifdef HAVE_LIBPCRE
	const char *err_p = NULL;
	int errpos = 0;
	int options = PCRE_UTF8;
	pcre *re;
#else
	int options = REG_NOSUB;
	regex_t re;
	int errcode;
	int retval;
#endif

	while (*flags) {
		switch (*flags) {
		case 'i':
#ifdef HAVE_LIBPCRE
			options |= PCRE_CASELESS;
#else
			options |= REG_ICASE;
#endif
			break;
		case 'm':
#ifdef HAVE_LIBPCRE
			options |= PCRE_MULTILINE;
#else
			options |= REG_NEWLINE;
#endif
			break;
#ifdef HAVE_LIBPCRE
		case 's':
			options |= PCRE_DOTALL;
			break;
#endif
		case 'x':
#ifdef HAVE_LIBPCRE
			options |= PCRE_EXTENDED;
#else
			options |= REG_EXTENDED;
#endif
			break;
		default:
			throw(MAL, "pcre.match", ILLEGAL_ARGUMENT
				  ": unsupported flag character '%c'\n", *flags);
		}
		flags++;
	}
	if (strNil(val)) {
		*ret = FALSE;
		return MAL_SUCCEED;
	}

#ifdef HAVE_LIBPCRE
	if ((re = pcre_compile(pat, options, &err_p, &errpos, NULL)) == NULL) 
#else
		if ((errcode = regcomp(&re, pat, options)) != 0)
#endif
			{
				throw(MAL, "pcre.match", OPERATION_FAILED
					  ": compilation of regular expression (%s) failed "
#ifdef HAVE_LIBPCRE
					  "at %d with '%s'", pat, errpos, err_p
#else
					  , pat
#endif
					);
			}
#ifdef HAVE_LIBPCRE
	pos = pcre_exec(re, NULL, val, (int) strlen(val), 0, 0, NULL, 0);
	pcre_free(re);
#else
	retval = regexec(&re, val, (size_t) 0, NULL, 0);
	pos = retval == REG_NOMATCH ? -1 : (retval == REG_ENOSYS ? -2 : 0);
	regfree(&re);
#endif
	if (pos >= 0)
		*ret = TRUE;
	else if (pos == -1)
		*ret = FALSE;
	else
		throw(MAL, "pcre.match", OPERATION_FAILED
			  ": matching of regular expression (%s) failed with %d",
			  pat, pos);
	return MAL_SUCCEED;
}

#ifdef HAVE_LIBPCRE
/* special characters in PCRE that need to be escaped */
static const char *pcre_specials = ".+?*()[]{}|^$\\";
#else
/* special characters in POSIX basic regular expressions that need to
 * be escaped */
static const char *pcre_specials = ".*[]^$\\";
#endif

/* change SQL LIKE pattern into PCRE pattern */
static str
sql2pcre(str *r, const char *pat, const char *esc_str)
{
	int escaped = 0;
	int hasWildcard = 0;
	char *ppat;
	int esc = esc_str[0] == '\200' ? 0 : esc_str[0]; /* should change to utf8_convert() */
	int specials;
	int c;

	if (strlen(esc_str) > 1)
		throw(MAL, "pcre.sql2pcre", SQLSTATE(22019) ILLEGAL_ARGUMENT ": ESCAPE string must have length 1");
	if (pat == NULL )
		throw(MAL, "pcre.sql2pcre", OPERATION_FAILED);
	ppat = GDKmalloc(strlen(pat)*3+3 /* 3 = "^'the translated regexp'$0" */);
	if (ppat == NULL)
		throw(MAL, "pcre.sql2pcre", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	*r = ppat;
	/* The escape character can be a char which is special in a PCRE
	 * expression.  If the user used the "+" char as escape and has "++"
	 * in their pattern, then replacing this with "+" is not correct and
	 * should be "\+" instead. */
	specials = (esc && strchr(pcre_specials, esc) != NULL);

	*ppat++ = '^';
	while ((c = *pat++) != 0) {
		if (c == esc) {
			if (escaped) {
				if (specials) { /* change ++ into \+ */
					*ppat++ = esc;
				} else { /* do not escape simple escape symbols */
					ppat[-1] = esc; /* overwrite backslash */
				}
				escaped = 0;
			} else {
				*ppat++ = '\\';
				escaped = 1;
			}
			hasWildcard = 1;
		} else if (strchr(pcre_specials, c) != NULL) {
			/* escape PCRE special chars, avoid double backslash if the
			 * user uses an invalid escape sequence */
			if (!escaped)
				*ppat++ = '\\';
			*ppat++ = c;
			hasWildcard = 1;
			escaped = 0;
		} else if (c == '%' && !escaped) {
			*ppat++ = '.';
			*ppat++ = '*';
			*ppat++ = '?';
			hasWildcard = 1;
			/* collapse multiple %, but only if it isn't the escape */
			if (esc != '%')
				while (*pat == '%')
					pat++;
		} else if (c == '_' && !escaped) {
			*ppat++ = '.';
			hasWildcard = 1;
		} else {
			if (escaped) {
				ppat[-1] = c; /* overwrite backslash of invalid escape */
			} else {
				*ppat++ = c;
			}
			escaped = 0;
		}
	}
	/* no wildcard or escape character at end of string */
	if (!hasWildcard || escaped) {
		GDKfree(*r);
		*r = NULL;
		if (escaped)
			throw(MAL, "pcre.sql2pcre", OPERATION_FAILED);
		*r = GDKstrdup(str_nil);
		if (*r == NULL)
			throw(MAL, "pcre.sql2pcre", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		*ppat++ = '$';
		*ppat = 0;
	}
	return MAL_SUCCEED;
}

#ifdef HAVE_LIBPCRE
/* change SQL PATINDEX pattern into PCRE pattern */
static str
pat2pcre(str *r, const char *pat)
{
	size_t len = strlen(pat);
	char *ppat = GDKmalloc(len*2+3 /* 3 = "^'the translated regexp'$0" */);
	int start = 0;

	if (ppat == NULL)
		throw(MAL, "pcre.sql2pcre", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*r = ppat;
	while (*pat) {
		int c = *pat++;

		if (strchr(pcre_specials, c) != NULL) {
			*ppat++ = '\\';
			*ppat++ = c;
		} else if (c == '%') {
			if (start && *pat) {
				*ppat++ = '.';
				*ppat++ = '*';
			}
			start++;
		} else if (c == '_') {
			*ppat++ = '.';
		} else {
			*ppat++ = c;
		}
	}
	*ppat = 0;
	return MAL_SUCCEED;
}
#endif

/*
 * @+ Wrapping
 */
#include "mal.h"
str
PCREreplace_wrap(str *res, const str *or, const str *pat, const str *repl, const str *flags)
{
	return pcre_replace(res, *or, *pat, *repl, *flags, true);
}

str
PCREreplace_bat_wrap(bat *res, const bat *bid, const str *pat, const str *repl, const str *flags)
{
	BAT *b, *bn = NULL;
	str msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batpcre.replace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	msg = pcre_replace_bat(&bn, b, *pat, *repl, *flags, true);
	if (msg == MAL_SUCCEED) {
		*res = bn->batCacheid;
		BBPkeepref(*res);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

str
PCREreplacefirst_wrap(str *res, const str *or, const str *pat, const str *repl, const str *flags)
{
	return pcre_replace(res, *or, *pat, *repl, *flags, false);
}

str
PCREreplacefirst_bat_wrap(bat *res, const bat *bid, const str *pat, const str *repl, const str *flags)
{
	BAT *b,*bn = NULL;
	str msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batpcre.replace_first", RUNTIME_OBJECT_MISSING);

	msg = pcre_replace_bat(&bn, b, *pat, *repl, *flags, false);
	if (msg == MAL_SUCCEED) {
		*res = bn->batCacheid;
		BBPkeepref(*res);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

str
PCREmatch(bit *ret, const str *val, const str *pat)
{
	return pcre_match_with_flags(ret, *val, *pat,
#ifdef HAVE_LIBPCRE
								 "s"
#else
								 "x"
#endif
		);
}

str
PCREimatch(bit *ret, const str *val, const str *pat)
{
	return pcre_match_with_flags(ret, *val, *pat, "i"
#ifndef HAVE_LIBPCRE
								 "x"
#endif
		);
}

str
PCREindex(int *res, const pcre *pattern, const str *s)
{
#ifdef HAVE_LIBPCRE
	int v[3];

	v[0] = v[1] = *res = 0;
	if (pcre_exec(pattern, NULL, *s, (int) strlen(*s), 0, 0, v, 3) >= 0) {
		*res = v[1];
	}
	return MAL_SUCCEED;
#else
	(void) res;
	(void) pattern;
	(void) s;
	throw(MAL, "pcre.index", "Database was compiled without PCRE support.");
#endif
}

str
PCREpatindex(int *ret, const str *pat, const str *val)
{
#ifdef HAVE_LIBPCRE
	pcre *re = NULL;
	char *ppat = NULL, *msg;

	if (strNil(*pat) || strNil(*val)) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}

	if ((msg = pat2pcre(&ppat, *pat)) != MAL_SUCCEED)
		return msg;
	if ((msg = pcre_compile_wrap(&re, ppat, FALSE)) != MAL_SUCCEED) {
		GDKfree(ppat);
		return msg;
	}
	GDKfree(ppat);
	msg = PCREindex(ret, re, val);
	pcre_free(re);
	return msg;
#else
	(void) ret;
	(void) pat;
	(void) val;
	throw(MAL, "pcre.patindex", "Database was compiled without PCRE support.");
#endif
}

str
PCREquote(str *ret, const str *val)
{
	char *p;
	const char *s = *val;

	*ret = p = GDKmalloc(strlen(s) * 2 + 1); /* certainly long enough */
	if (p == NULL)
		throw(MAL, "pcre.quote", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	/* quote all non-alphanumeric ASCII characters (i.e. leave
	   non-ASCII and alphanumeric alone) */
	while (*s) {
		if (!((*s & 0x80) != 0 ||
		      ('a' <= *s && *s <= 'z') ||
		      ('A' <= *s && *s <= 'Z') ||
		      isdigit((unsigned char) *s)))
			*p++ = '\\';
		*p++ = *s++;
	}
	*p = 0;
	return MAL_SUCCEED;
}

str
PCREsql2pcre(str *ret, const str *pat, const str *esc)
{
	return sql2pcre(ret, *pat, *esc);
}

static str
PCRElike4(bit *ret, const str *s, const str *pat, const str *esc, const bit *isens)
{
	char *ppat = NULL;
	str r = sql2pcre(&ppat, *pat, *esc);

	if (!r) {
		assert(ppat);
		if (strNil(*pat) || strNil(*s)) {
			*ret = bit_nil;
		} else if (strNil(ppat)) {
			*ret = FALSE;
			if (*isens) {
				if (mystrcasecmp(*s, *pat) == 0)
					*ret = TRUE;
			} else {
				if (strcmp(*s, *pat) == 0)
					*ret = TRUE;
			}
		} else {
			if (*isens) {
				r = PCREimatch(ret, s, &ppat);
			} else {
				r = PCREmatch(ret, s, &ppat);
			}
		}
	}
	if (ppat)
		GDKfree(ppat);
	return r;
}

str
PCRElike3(bit *ret, const str *s, const str *pat, const str *esc)
{
	bit no = FALSE;

	return PCRElike4(ret, s, pat, esc, &no);
}

str
PCRElike2(bit *ret, const str *s, const str *pat)
{
	char *esc = "";

	return PCRElike3(ret, s, pat, &esc);
}

str
PCREnotlike3(bit *ret, const str *s, const str *pat, const str *esc)
{
	str tmp;
	bit r;

	rethrow("str.not_like", tmp, PCRElike3(&r, s, pat, esc));
	*ret = r==bit_nil?bit_nil:!r;
	return MAL_SUCCEED;
}

str
PCREnotlike2(bit *ret, const str *s, const str *pat)
{
	str tmp;
	bit r;

	rethrow("str.not_like", tmp, PCRElike2(&r, s, pat));
	*ret = r==bit_nil?bit_nil:!r;
	return MAL_SUCCEED;
}

str
PCREilike3(bit *ret, const str *s, const str *pat, const str *esc)
{
	bit yes = TRUE;

	return PCRElike4(ret, s, pat, esc, &yes);
}

str
PCREilike2(bit *ret, const str *s, const str *pat)
{
	char *esc = "\\";

	return PCREilike3(ret, s, pat, &esc);
}

str
PCREnotilike3(bit *ret, const str *s, const str *pat, const str *esc)
{
	str tmp;
	bit r;

	rethrow("str.not_ilike", tmp, PCREilike3(&r, s, pat, esc));
	*ret = r==bit_nil?bit_nil:!r;
	return MAL_SUCCEED;
}

str
PCREnotilike2(bit *ret, const str *s, const str *pat)
{
	str tmp;
	bit r;

	rethrow("str.not_ilike", tmp, PCREilike2(&r, s, pat));
	*ret = r==bit_nil?bit_nil:!r;
	return MAL_SUCCEED;
}

static str
BATPCRElike3(bat *ret, const bat *bid, const str *pat, const str *esc, const bit *isens, const bit *not)
{
	char *ppat = NULL;
	str res = sql2pcre(&ppat, *pat, *esc);

	if (res == MAL_SUCCEED) {
		BAT *strs = BATdescriptor(*bid);
		BATiter strsi;
		BAT *r;
		bit *br;
		BUN p, q, i = 0;

		if (strs == NULL) {
			GDKfree(ppat);
			throw(MAL, "batstr.like", OPERATION_FAILED);
		}

		r = COLnew(strs->hseqbase, TYPE_bit, BATcount(strs), TRANSIENT);
		if (r==NULL) {
			GDKfree(ppat);
			BBPunfix(strs->batCacheid);
			throw(MAL, "pcre.like3", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		br = (bit*)Tloc(r, 0);
		strsi = bat_iterator(strs);

		if (strNil(*pat)) {
			BATloop(strs, p, q) {
				br[i] = bit_nil;
				i++;
			}
			r->tnonil = false;
			r->tnil = true;
		} else if (strNil(ppat)) {
			BATloop(strs, p, q) {
				const char *s = (str)BUNtvar(strsi, p);

				if (strcmp(s, *pat) == 0)
					br[i] = TRUE;
				else
					br[i] = FALSE;
				if (*not)
					br[i] = !br[i];
				i++;
			}
		} else {
			int pos;
#ifdef HAVE_LIBPCRE
			const char *err_p = NULL;
			int errpos = 0;
			int options = PCRE_UTF8 | PCRE_DOTALL;
			pcre *re;
#else
			regex_t re;
			int options = REG_NEWLINE | REG_NOSUB | REG_EXTENDED;
			int errcode;
#endif

			if (*isens) {
#ifdef HAVE_LIBPCRE
				options |= PCRE_CASELESS;
#else
				options |= REG_ICASE;
#endif
			}
			if (
#ifdef HAVE_LIBPCRE
				(re = pcre_compile(ppat, options, &err_p, &errpos, NULL)) == NULL
#else
				(errcode = regcomp(&re, ppat, options)) != 0
#endif
				) {
				BBPunfix(strs->batCacheid);
				BBPunfix(r->batCacheid);
				res = createException(MAL, "pcre.match", OPERATION_FAILED
									  ": compilation of regular expression (%s) failed"
#ifdef HAVE_LIBPCRE
									  " at %d with '%s'", ppat, errpos, err_p
#else
									  , ppat
#endif
					);
				GDKfree(ppat);
				return res;
			}

			BATloop(strs, p, q) {
				const char *s = (str)BUNtvar(strsi, p);

				if (*s == '\200') {
					br[i] = bit_nil;
					r->tnonil = false;
					r->tnil = true;
				} else {
#ifdef HAVE_LIBPCRE
					pos = pcre_exec(re, NULL, s, (int) strlen(s), 0, 0, NULL, 0);
#else
					int retval = regexec(&re, s, (size_t) 0, NULL, 0);
					pos = retval == REG_NOMATCH ? -1 : (retval == REG_ENOSYS ? -2 : 0);
#endif
					if (pos >= 0)
						br[i] = *not? FALSE:TRUE;
					else if (pos == -1)
						br[i] = *not? TRUE: FALSE;
					else {
						BBPunfix(strs->batCacheid);
						BBPunfix(r->batCacheid);
						res = createException(MAL, "pcre.match", OPERATION_FAILED
											  ": matching of regular expression (%s) failed with %d", ppat, pos);
						GDKfree(ppat);
						return res;
					}
				}
				i++;
			}
#ifdef HAVE_LIBPCRE
			pcre_free(re);
#else
			regfree(&re);
#endif
		}
		BATsetcount(r, i);
		r->tsorted = false;
		r->trevsorted = false;
		BATkey(r, false);

		BBPkeepref(*ret = r->batCacheid);
		BBPunfix(strs->batCacheid);
		GDKfree(ppat);
	}
	return res;
}

str
BATPCRElike(bat *ret, const bat *bid, const str *pat, const str *esc)
{
	bit no = FALSE;

	return BATPCRElike3(ret, bid, pat, esc, &no, &no);
}

str
BATPCRElike2(bat *ret, const bat *bid, const str *pat)
{
	char *esc = "\\";

	return BATPCRElike(ret, bid, pat, &esc);
}

str
BATPCREnotlike(bat *ret, const bat *bid, const str *pat, const str *esc)
{
	bit no = FALSE;
	bit yes = TRUE;

	return BATPCRElike3(ret, bid, pat, esc, &no, &yes);
}

str
BATPCREnotlike2(bat *ret, const bat *bid, const str *pat)
{
	char *esc = "\\";

	return BATPCREnotlike(ret, bid, pat, &esc);
}

str
BATPCREilike(bat *ret, const bat *bid, const str *pat, const str *esc)
{
	bit yes = TRUE;
	bit no = FALSE;

	return BATPCRElike3(ret, bid, pat, esc, &yes, &no);
}

str
BATPCREilike2(bat *ret, const bat *bid, const str *pat)
{
	char *esc = "\\";

	return BATPCREilike(ret, bid, pat, &esc);
}

str
BATPCREnotilike(bat *ret, const bat *bid, const str *pat, const str *esc)
{
	bit yes = TRUE;

	return BATPCRElike3(ret, bid, pat, esc, &yes, &yes);
}

str
BATPCREnotilike2(bat *ret, const bat *bid, const str *pat)
{
	char *esc = "\\";

	return BATPCREnotilike(ret, bid, pat, &esc);
}

str
PCRElikeselect2(bat *ret, const bat *bid, const bat *sid, const str *pat, const str *esc, const bit *caseignore, const bit *anti)
{
	BAT *b, *s = NULL, *bn = NULL;
	str res;
	char *ppat = NULL;
	bool use_re = false;
	bool use_strcmp = false;
	bool empty = false;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.likeselect", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && *sid && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.likeselect", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	/* no escape, try if a simple list of keywords works */
	if (strNil(*pat)) {
		empty = true;
	} else if (is_strcmpable(*pat, *esc)) {
		use_re = true;
		use_strcmp = true;
	} else if (re_simple(*pat, **esc == '\200' ? 0 : (unsigned char) **esc)) {
		use_re = true;
	} else {
		res = sql2pcre(&ppat, *pat, *esc);
		if (res != MAL_SUCCEED) {
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			return res;
		}
		if (strNil(ppat)) {
			GDKfree(ppat);
			ppat = NULL;
			if (*caseignore) {
				ppat = GDKmalloc(strlen(*pat) + 3);
				if (ppat == NULL) {
					BBPunfix(b->batCacheid);
					if (s)
						BBPunfix(s->batCacheid);
					throw(MAL, "algebra.likeselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				ppat[0] = '^';
				strcpy(ppat + 1, *pat);
				strcat(ppat, "$");
			}
		}
	}

	if (use_re) {
		res = re_likeselect(&bn, b, s, *pat, (bool) *caseignore, (bool) *anti, use_strcmp, **esc == '\200' ? 0 : (unsigned char) **esc);
	} else if (ppat == NULL) {
		/* no pattern and no special characters: can use normal select */
		if (empty) 
			bn = BATdense(0, 0, 0);
		else
			bn = BATselect(b, s, *pat, NULL, true, true, *anti);
		if (bn == NULL)
			res = createException(MAL, "algebra.likeselect", GDK_EXCEPTION);
		else
			res = MAL_SUCCEED;
	} else {
		res = pcre_likeselect(&bn, b, s, ppat, (bool) *caseignore, (bool) *anti);
	}
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	GDKfree(ppat);
	if (res != MAL_SUCCEED)
		return res;
	assert(bn);
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

str
PCRElikeselect1(bat *ret, const bat *bid, const bat *cid, const str *pat, const str *esc, const bit *anti)
{
	const bit f = TRUE;
	return PCRElikeselect2(ret, bid, cid, pat, esc, &f, anti);
}

str
PCRElikeselect3(bat *ret, const bat *bid, const bat *sid, const str *pat, const str *esc, const bit *anti)
{
	const bit f = FALSE;
	return PCRElikeselect2(ret, bid, sid, pat, esc, &f, anti);
}

str
PCRElikeselect4(bat *ret, const bat *bid, const bat *cid, const str *pat, const bit *anti)
{
	const bit f = TRUE;
	const str esc ="";
	return PCRElikeselect2(ret, bid, cid, pat, &esc, &f, anti);
}

str
PCRElikeselect5(bat *ret, const bat *bid, const bat *sid, const str *pat, const bit *anti)
{
	const bit f = FALSE;
	const str esc ="";
	return PCRElikeselect2(ret, bid, sid, pat, &esc, &f, anti);
}

#define APPEND(b, o)	(((oid *) b->theap.base)[b->batCount++] = (o))
#define VALUE(s, x)		(s##vars + VarHeapVal(s##vals, (x), s##width))

static char *
pcrejoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr,
		 const char *esc, bool caseignore)
{
	struct canditer lci, rci;
	const char *lvals, *rvals;
	const char *lvars, *rvars;
	int lwidth, rwidth;
	const char *vl, *vr;
	oid lastl = 0;		/* last value inserted into r1 */
	BUN nl;
	BUN newcap;
	oid lo, ro;
	int rskipped = 0;	/* whether we skipped values in r */
	char *msg = MAL_SUCCEED;
	RE *re = NULL;
	char *pcrepat = NULL;
#ifdef HAVE_LIBPCRE
	pcre *pcrere = NULL;
	pcre_extra *pcreex = NULL;
	const char *err_p = NULL;
	int errpos;
	int pcreopt = PCRE_UTF8 | PCRE_MULTILINE;
	int pcrestopt = (sl ? BATcount(sl) : BATcount(l)) > JIT_COMPILE_MIN ? PCRE_STUDY_JIT_COMPILE : 0;
#else
	int pcrere = 0;
	regex_t regex;
	int options =  REG_NEWLINE | REG_NOSUB | REG_EXTENDED;
	int errcode = -1;
#endif


	if (caseignore)
#ifdef HAVE_LIBPCRE
		pcreopt |= PCRE_CASELESS;
#else
	options |= REG_ICASE;
#endif

	TRC_DEBUG(ALGO, 
			  "pcrejoin(l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s)\n",
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  sl ? BATgetId(sl) : "NULL", sl ? BATcount(sl) : 0,
			  sl && sl->tsorted ? "-sorted" : "",
			  sl && sl->trevsorted ? "-revsorted" : "",
			  sr ? BATgetId(sr) : "NULL", sr ? BATcount(sr) : 0,
			  sr && sr->tsorted ? "-sorted" : "",
			  sr && sr->trevsorted ? "-revsorted" : "");

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);
	assert(sl == NULL || sl->tsorted);
	assert(sr == NULL || sr->tsorted);

	canditer_init(&lci, l, sl);
	canditer_init(&rci, r, sr);

	lvals = (const char *) Tloc(l, 0);
	rvals = (const char *) Tloc(r, 0);
	assert(r->tvarsized && r->ttype);
	lvars = l->tvheap->base;
	rvars = r->tvheap->base;
	lwidth = l->twidth;
	rwidth = r->twidth;

	r1->tkey = true;
	r1->tsorted = true;
	r1->trevsorted = true;
	r2->tkey = true;
	r2->tsorted = true;
	r2->trevsorted = true;

	/* nested loop implementation for PCRE join */
	for (BUN ri = 0; ri < rci.ncand; ri++) {
		ro = canditer_next(&rci);
		vr = VALUE(r, ro - r->hseqbase);
		if (strNil(vr))
			continue;
		if (re_simple(vr, esc && *esc != '\200' ? (unsigned char) *esc : 0)) {
			re = re_create(vr, caseignore, esc && *esc != '\200' ? (unsigned char) *esc : 0);
			if (re == NULL) {
				msg = createException(MAL, "pcre.join", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
		} else {
			assert(pcrepat == NULL);
			msg = sql2pcre(&pcrepat, vr, esc);
			if (msg != MAL_SUCCEED)
				goto bailout;
			if (strNil(pcrepat)) {
				GDKfree(pcrepat);
				if (caseignore) {
					pcrepat = GDKmalloc(strlen(vr) + 3);
					if (pcrepat == NULL) {
						msg = createException(MAL, "pcre.join", SQLSTATE(HY013) MAL_MALLOC_FAIL);
						goto bailout;
					}
					sprintf(pcrepat, "^%s$", vr);
				} else {
					/* a simple strcmp suffices */
					pcrepat = NULL;
				}
			}
			if (pcrepat) {
#ifdef HAVE_LIBPCRE
				pcrere = pcre_compile(pcrepat, pcreopt, &err_p, &errpos, NULL);
				if (pcrere == NULL) {
					msg = createException(MAL, "pcre.join", OPERATION_FAILED
										  ": pcre compile of pattern (%s) "
										  "failed at %d with '%s'",
										  pcrepat, errpos, err_p);
					goto bailout;
				}
				pcreex = pcre_study(pcrere, pcrestopt, &err_p);
				if (err_p != NULL) {
					msg = createException(MAL, "pcre.join", OPERATION_FAILED
										  ": pcre study of pattern (%s) "
										  "failed with '%s'", pcrepat, err_p);
					goto bailout;
				}
#else
				if ((errcode = regcomp(&regex, pcrepat, options)) != 0) {
					msg = createException(MAL, "pcre.join", OPERATION_FAILED
										  ": pcre compile of pattern (%s)",
										  pcrepat);
					goto bailout;
				}
				pcrere = 1;
#endif
				GDKfree(pcrepat);
				pcrepat = NULL;
			}
		}
		nl = 0;
		canditer_reset(&lci);
		for (BUN li = 0; li < lci.ncand; li++) {
			lo = canditer_next(&lci);
			vl = VALUE(l, lo - l->hseqbase);
			if (strNil(vl))
				continue;
			if (re) {
				if (caseignore) {
					if (!re_match_ignore(vl, re))
						continue;
				} else {
					if (!re_match_no_ignore(vl, re))
						continue;
				}
			} else if (pcrere) {
#ifdef HAVE_LIBPCRE
				if (pcre_exec(pcrere, pcreex, vl, (int) strlen(vl), 0, 0, NULL, 0) < 0)
					continue;
#else
				int retval = regexec(&regex, vl, (size_t) 0, NULL, 0);
				if (retval == REG_NOMATCH || retval == REG_ENOSYS)
					continue;
#endif
			} else {
				if (strcmp(vl, vr) != 0)
					continue;
			}
			if (BUNlast(r1) == BATcapacity(r1)) {
				newcap = BATgrows(r1);
				BATsetcount(r1, BATcount(r1));
				BATsetcount(r2, BATcount(r2));
				if (BATextend(r1, newcap) != GDK_SUCCEED ||
					BATextend(r2, newcap) != GDK_SUCCEED) {
					msg = createException(MAL, "pcre.join", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				assert(BATcapacity(r1) == BATcapacity(r2));
			}
			if (BATcount(r1) > 0) {
				if (lastl + 1 != lo)
					r1->tseqbase = oid_nil;
				if (nl == 0) {
					r2->trevsorted = false;
					if (lastl > lo) {
						r1->tsorted = false;
						r1->tkey = false;
					} else if (lastl < lo) {
						r1->trevsorted = false;
					} else {
						r1->tkey = false;
					}
				}
			}
			APPEND(r1, lo);
			APPEND(r2, ro);
			lastl = lo;
			nl++;
		}
		if (re) {
			re_destroy(re);
			re = NULL;
		}
		if (pcrere) {
#ifdef HAVE_LIBPCRE
			pcre_free_study(pcreex);
			pcre_free(pcrere);
			pcrere = NULL;
			pcreex = NULL;
#else
			regfree(&regex);
			pcrere = 0;
#endif
		}
		if (nl > 1) {
			r2->tkey = false;
			r2->tseqbase = oid_nil;
			r1->trevsorted = false;
		} else if (nl == 0) {
			rskipped = BATcount(r2) > 0;
		} else if (rskipped) {
			r2->tseqbase = oid_nil;
		}
	}
	assert(BATcount(r1) == BATcount(r2));
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	BATsetcount(r2, BATcount(r2));
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap.base)[0];
		if (BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap.base)[0];
	} else {
		r1->tseqbase = r2->tseqbase = 0;
	}
	TRC_DEBUG(ALGO, 
			  "pcrejoin(l=%s,r=%s)=(%s#"BUNFMT"%s%s,%s#"BUNFMT"%s%s\n",
			  BATgetId(l), BATgetId(r),
			  BATgetId(r1), BATcount(r1),
			  r1->tsorted ? "-sorted" : "",
			  r1->trevsorted ? "-revsorted" : "",
			  BATgetId(r2), BATcount(r2),
			  r2->tsorted ? "-sorted" : "",
			  r2->trevsorted ? "-revsorted" : "");
	return MAL_SUCCEED;

  bailout:
	if (re)
		re_destroy(re);
	if (pcrepat)
		GDKfree(pcrepat);
#ifdef HAVE_LIBPCRE
	if (pcreex)
		pcre_free_study(pcreex);
	if (pcrere)
		pcre_free(pcrere);
#else
	if (pcrere)
		regfree(&regex);
#endif

	assert(msg != MAL_SUCCEED);
	return msg;
}

static str
PCREjoin(bat *r1, bat *r2, bat lid, bat rid, bat slid, bat srid,
		 const char *esc, bool caseignore)
{
	BAT *left = NULL, *right = NULL, *candleft = NULL, *candright = NULL;
	BAT *result1 = NULL, *result2 = NULL;
	char *msg = MAL_SUCCEED;

	if ((left = BATdescriptor(lid)) == NULL)
		goto fail;
	if ((right = BATdescriptor(rid)) == NULL)
		goto fail;
	if (!is_bat_nil(slid) && (candleft = BATdescriptor(slid)) == NULL)
		goto fail;
	if (!is_bat_nil(srid) && (candright = BATdescriptor(srid)) == NULL)
		goto fail;
	result1 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	result2 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	if (result1 == NULL || result2 == NULL) {
		msg = createException(MAL, "pcre.join", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto fail;
	}
	result1->tnil = false;
	result1->tnonil = true;
	result1->tkey = true;
	result1->tsorted = true;
	result1->trevsorted = true;
	result1->tseqbase = 0;
	result2->tnil = false;
	result2->tnonil = true;
	result2->tkey = true;
	result2->tsorted = true;
	result2->trevsorted = true;
	result2->tseqbase = 0;
	msg = pcrejoin(result1, result2, left, right, candleft, candright,
				   esc, caseignore);
	if (msg)
		goto fail;
	*r1 = result1->batCacheid;
	*r2 = result2->batCacheid;
	BBPkeepref(*r1);
	BBPkeepref(*r2);
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	if (candleft)
		BBPunfix(candleft->batCacheid);
	if (candright)
		BBPunfix(candright->batCacheid);
	return MAL_SUCCEED;

  fail:
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	if (candleft)
		BBPunfix(candleft->batCacheid);
	if (candright)
		BBPunfix(candright->batCacheid);
	if (result1)
		BBPunfix(result1->batCacheid);
	if (result2)
		BBPunfix(result2->batCacheid);
	if (msg)
		return msg;
	throw(MAL, "pcre.join", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

str
LIKEjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const str *esc, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate)
{
	(void) nil_matches;
	(void) estimate;
	return PCREjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *esc, 0);
}

str
LIKEjoin1(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate)
{
	(void) nil_matches;
	(void) estimate;
	return PCREjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, "", 0);
}

str
ILIKEjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const str *esc, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate)
{
	(void) nil_matches;
	(void) estimate;
	return PCREjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *esc, 1);
}

str
ILIKEjoin1(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate)
{
	(void) nil_matches;
	(void) estimate;
	return PCREjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, "", 1);
}
