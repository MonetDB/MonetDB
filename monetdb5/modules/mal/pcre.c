/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal_client.h"
#include "mal_interpreter.h"
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

/* current implementation assumes simple %keyword% [keyw%]* */
struct RE {
	char *k;
	uint32_t *w;
	bool search:1,
		atend:1;
	size_t len;
	struct RE *n;
};

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

static inline bool
mywstrncaseeq(const char *restrict s1, const uint32_t *restrict s2, size_t n2, bool atend)
{
	uint32_t c1;

	while (n2 > 0) {
		size_t nn1 = utfc8touc(&c1, s1);
		if (nn1 == 0 || nn1 == (size_t) -1)
			return (*s2 == 0);
		if (*s2 == 0)
			return false;
		if (nn1 == (size_t) -1 || nn1 == (size_t) -2)
			return true;	 /* actually an error that shouldn't happen */
#if SIZEOF_WCHAR_T == 2
		if (c1 > 0xFFFF || *s2 > 0xFFFF) {
			if (c1 != *s2)
				return false;
		} else
#endif
		if (towlower((wint_t) c1) != towlower((wint_t) *s2))
			return false;
		s1 += nn1;
		n2--;
		s2++;
	}
	return !atend || *s1 == 0;
}

static inline int
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

static inline int
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

static inline const char *
mywstrcasestr(const char *restrict haystack, const uint32_t *restrict wneedle, bool atend)
{
	size_t nlen = myucslen(wneedle);

	if (nlen == 0)
		return atend ? haystack + strlen(haystack) : haystack;

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
		if (i == nlen && (!atend || haystack[h] == 0))
			return haystack;
		haystack += step;
		hlen -= step;
	}
	return NULL;
}

/* returns true if the pattern does not contain unescaped `_' (single
 * character match) and ends with unescaped `%' (any sequence
 * match) */
static inline bool
re_simple(const char *pat, unsigned char esc)
{
	bool escaped = false;

	if (pat == 0)
		return false;
	if (*pat == '%') {
		pat++;
	}
	while (*pat) {
		if (escaped) {
			escaped = false;
		} else if ((unsigned char) *pat == esc) {
			escaped = true;
		} else if (*pat == '_') {
			return false;
		}
		pat++;
	}
	return true;
}

static inline bool
re_is_pattern_properly_escaped(const char *pat, unsigned char esc)
{
	bool escaped = false;

	if (pat == 0)
		return true;
	while (*pat) {
		if (escaped) {
			escaped = false;
		} else if ((unsigned char) *pat == esc) {
			escaped = true;
		}
		pat++;
	}
	return escaped ? false : true;
}

static inline bool
is_strcmpable(const char *pat, const char *esc)
{
	if (pat[strcspn(pat, "%_")])
		return false;
	return strlen(esc) == 0 || strNil(esc) || strstr(pat, esc) == NULL;
}

static inline bool
re_match_ignore(const char *restrict s, const struct RE *restrict pattern)
{
	const struct RE *r;

	for (r = pattern; r; r = r->n) {
		if (*r->w == 0 && (r->search || *s == 0))
			return true;
		if (!*s ||
			(r->search
			 ? (s = mywstrcasestr(s, r->w, r->atend)) == NULL
			 : !mywstrncaseeq(s, r->w, r->len, r->atend)))
			return false;
		s += r->len;
	}
	return true;
}

static inline bool
re_match_no_ignore(const char *restrict s, const struct RE *restrict pattern)
{
	const struct RE *r;
	size_t l;

	for (r = pattern; r; r = r->n) {
		if (*r->k == 0 && (r->search || *s == 0))
			return true;
		if (!*s ||
			(r->search
			 ? (r->atend
				? (l = strlen(s)) < r->len || strcmp(s + l - r->len, r->k) != 0
				: (s = strstr(s, r->k)) == NULL)
			 : (r->atend
				? strcmp(s, r->k) != 0
				: strncmp(s, r->k, r->len) != 0)))
			return false;
		s += r->len;
	}
	return true;
}

static void
re_destroy(struct RE *p)
{
	if (p) {
		GDKfree(p->k);
		GDKfree(p->w);
		do {
			struct RE *n = p->n;

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
static struct RE *
re_create(const char *pat, bool caseignore, uint32_t esc)
{
	struct RE *r = GDKmalloc(sizeof(struct RE)), *n = r;
	bool escaped = false;

	if (r == NULL)
		return NULL;
	*r = (struct RE) {.atend = true};

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
				n->len++;
				escaped = false;
			} else if (*wp == esc) {
				escaped = true;
			} else if (*wp == '%') {
				n->atend = false;
				while (wp[1] == '%')
					wp++;
				if (wp[1]) {
					n = n->n = GDKmalloc(sizeof(struct RE));
					if (n == NULL)
						goto bailout;
					*n = (struct RE) {.search = true, .atend = true, .w = wp + 1};
				}
				*wq++ = 0;
			} else {
				*wq++ = *wp;
				n->len++;
			}
			wp++;
		}
		*wq = 0;
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
				n->len++;
				escaped = false;
			} else if ((unsigned char) *p == esc) {
				escaped = true;
			} else if (*p == '%') {
				n->atend = false;
				while (p[1] == '%')
					p++;
				if (p[1]) {
					n = n->n = GDKmalloc(sizeof(struct RE));
					if (n == NULL)
						goto bailout;
					*n = (struct RE) {.search = true, .atend = true, .k = p + 1};
				}
				*q++ = 0;
			} else {
				*q++ = *p;
				n->len++;
			}
			p++;
		}
		*q = 0;
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

static str
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
	if (pat == NULL)
		throw(MAL, "pcre.sql2pcre", SQLSTATE(22019) ILLEGAL_ARGUMENT ": (I)LIKE pattern must not be NULL");
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
			throw(MAL, "pcre.sql2pcre", SQLSTATE(22019) ILLEGAL_ARGUMENT ": (I)LIKE pattern must not end with escape character");
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

static str
PCREreplace_wrap(str *res, const str *or, const str *pat, const str *repl, const str *flags)
{
	return pcre_replace(res, *or, *pat, *repl, *flags, true);
}

static str
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

static str
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

static str
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

static str
PCREimatch(bit *ret, const str *val, const str *pat)
{
	return pcre_match_with_flags(ret, *val, *pat, "i"
#ifndef HAVE_LIBPCRE
								 "x"
#endif
		);
}

static str
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

static str
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

static str
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

static str
PCREsql2pcre(str *ret, const str *pat, const str *esc)
{
	return sql2pcre(ret, *pat, *esc);
}

static inline str
choose_like_path(char **ppat, bool *use_re, bool *use_strcmp, bool *empty, const str *pat, const str *esc)
{
	str res = MAL_SUCCEED;
	*use_re = false;
	*use_strcmp = false;
	*empty = false;

	if (strNil(*pat) || strNil(*esc)) {
		*empty = true;
	} else {
		if (!re_is_pattern_properly_escaped(*pat, (unsigned char) **esc))
			throw(MAL, "pcre.sql2pcre", SQLSTATE(22019) ILLEGAL_ARGUMENT ": (I)LIKE pattern must not end with escape character");
		if (is_strcmpable(*pat, *esc)) {
			*use_re = true;
			*use_strcmp = true;
		} else if (re_simple(*pat, (unsigned char) **esc)) {
			*use_re = true;
		} else {
			if ((res = sql2pcre(ppat, *pat, *esc)) != MAL_SUCCEED)
				return res;
			if (strNil(*ppat)) {
				GDKfree(*ppat);
				*ppat = NULL;
				*use_re = true;
				*use_strcmp = true;
			}
		}
	}
	return res;
}

static str
PCRElike_imp(bit *ret, const str *s, const str *pat, const str *esc, const bit *isens)
{
	str res = MAL_SUCCEED;
	char *ppat = NULL;
	bool use_re = false, use_strcmp = false, empty = false;
	struct RE *re = NULL;

	if ((res = choose_like_path(&ppat, &use_re, &use_strcmp, &empty, pat, esc)) != MAL_SUCCEED)
		return res;

	MT_thread_setalgorithm(empty ? "pcrelike: trivially empty" : use_strcmp ? "pcrelike: pattern matching using strcmp" :
						   use_re ? "pcrelike: pattern matching using RE" : "pcrelike: pattern matching using pcre");

	if (strNil(*s) || empty) {
		*ret = bit_nil;
	} else if (use_re) {
		if (use_strcmp) {
			*ret = *isens ? mystrcasecmp(*s, *pat) == 0 : strcmp(*s, *pat) == 0;
		} else {
			if (!(re = re_create(*pat, *isens, (unsigned char) **esc)))
				res = createException(MAL, "pcre.like4", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			else
				*ret = *isens ? re_match_ignore(*s, re) : re_match_no_ignore(*s, re);
		}
	} else {
		res = *isens ? PCREimatch(ret, s, &ppat) : PCREmatch(ret, s, &ppat);
	}

	if (re)
		re_destroy(re);
	GDKfree(ppat);
	return res;
}

static str
PCRElike(bit *ret, const str *s, const str *pat, const str *esc, const bit *isens)
{
	return PCRElike_imp(ret, s, pat, esc, isens);
}

static str
PCREnotlike(bit *ret, const str *s, const str *pat, const str *esc, const bit *isens)
{
	str tmp;
	bit r;

	rethrow("str.not_like", tmp, PCRElike(&r, s, pat, esc, isens));
	*ret = r==bit_nil?bit_nil:!r;
	return MAL_SUCCEED;
}

static inline str
re_like_build(struct RE **re, uint32_t **wpat, const char *pat, bool caseignore, bool use_strcmp, uint32_t esc)
{
	if (!use_strcmp) {
		if (!(*re = re_create(pat, caseignore, esc)))
			return createException(MAL, "pcre.re_like_build", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else if (caseignore) {
		if (!(*wpat = utf8stoucs(pat)))
			return createException(MAL, "pcre.re_like_build", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

#define proj_scanloop(TEST)	\
	do {	\
		if (*s == '\200') \
			return bit_nil; \
		else \
			return TEST; \
	} while (0)

static inline bit
re_like_proj_apply(str s, struct RE *re, uint32_t *wpat, const char *pat, bool caseignore, bool anti, bool use_strcmp)
{
	if (use_strcmp) {
		if (caseignore) {
			if (anti)
				proj_scanloop(mywstrcasecmp(s, wpat) != 0);
			else
				proj_scanloop(mywstrcasecmp(s, wpat) == 0);
		} else {
			if (anti)
				proj_scanloop(strcmp(s, pat) != 0);
			else
				proj_scanloop(strcmp(s, pat) == 0);
		}
	} else {
		if (caseignore) {
			if (anti)
				proj_scanloop(!re_match_ignore(s, re));
			else
				proj_scanloop(re_match_ignore(s, re));
		} else {
			if (anti)
				proj_scanloop(!re_match_no_ignore(s, re));
			else
				proj_scanloop(re_match_no_ignore(s, re));
		}
	}
}

static inline void
re_like_clean(struct RE **re, uint32_t **wpat)
{
	if (*re) {
		re_destroy(*re);
		*re = NULL;
	}
	if (*wpat) {
		GDKfree(*wpat);
		*wpat = NULL;
	}
}

static inline str
pcre_like_build(
#ifdef HAVE_LIBPCRE
	pcre **res,
	pcre_extra **ex
#else
	regex_t *res,
	void *ex
#endif
, const char *ppat, bool caseignore, BUN count)
{
#ifdef HAVE_LIBPCRE
	const char *err_p = NULL;
	int errpos = 0;
	int options = PCRE_UTF8 | PCRE_MULTILINE | PCRE_DOTALL;
	int pcrestopt = count > JIT_COMPILE_MIN ? PCRE_STUDY_JIT_COMPILE : 0;

	*res = NULL;
	*ex = NULL;
#else
	int options = REG_NEWLINE | REG_NOSUB | REG_EXTENDED;
	int errcode;

	*res = (regex_t) {0};
	(void) count;
#endif

	if (caseignore) {
#ifdef HAVE_LIBPCRE
		options |= PCRE_CASELESS;
#else
		options |= REG_ICASE;
#endif
	}
	if (
#ifdef HAVE_LIBPCRE
		(*res = pcre_compile(ppat, options, &err_p, &errpos, NULL)) == NULL
#else
		(errcode = regcomp(res, ppat, options)) != 0
#endif
		)
		return createException(MAL, "pcre.pcre_like_build", OPERATION_FAILED
								": compilation of regular expression (%s) failed"
#ifdef HAVE_LIBPCRE
								" at %d with '%s'", ppat, errpos, err_p
#else
								, ppat
#endif
			);
#ifdef HAVE_LIBPCRE
	*ex = pcre_study(*res, pcrestopt, &err_p);
	if (err_p != NULL)
		return createException(MAL, "pcre.pcre_like_build", OPERATION_FAILED
								": pcre study of pattern (%s) "
								"failed with '%s'", ppat, err_p);
#else
	(void) ex;
#endif
	return MAL_SUCCEED;
}

#define PCRE_LIKE_BODY(LOOP_BODY, RES1, RES2) \
	do { \
		LOOP_BODY  \
		if (*s == '\200') \
			*ret = bit_nil; \
		else if (pos >= 0) \
			*ret = RES1; \
		else if (pos == -1) \
			*ret = RES2; \
		else \
			return createException(MAL, "pcre.match", OPERATION_FAILED ": matching of regular expression (%s) failed with %d", ppat, pos); \
	} while(0)

static inline str
pcre_like_apply(bit *ret, str s,
#ifdef HAVE_LIBPCRE
	pcre *re, pcre_extra *ex
#else
	regex_t re, void *ex
#endif
, const char *ppat, bool anti)
{
	int pos;

#ifdef HAVE_LIBPCRE
#define LOOP_BODY	\
	pos = pcre_exec(re, ex, s, (int) strlen(s), 0, 0, NULL, 0);
#else
#define LOOP_BODY	\
	int retval = regexec(&re, s, (size_t) 0, NULL, 0); \
	(void) ex; \
	pos = retval == REG_NOMATCH ? -1 : (retval == REG_ENOSYS ? -2 : 0);
#endif

	if (anti)
		PCRE_LIKE_BODY(LOOP_BODY, FALSE, TRUE);
	else
		PCRE_LIKE_BODY(LOOP_BODY, TRUE, FALSE);

	return MAL_SUCCEED;
}

static inline void
pcre_clean(
#ifdef HAVE_LIBPCRE
	pcre **re, pcre_extra **ex) {
	if (*re)
		pcre_free(*re);
	if (*ex)
		pcre_free_study(*ex);
	*re = NULL;
	*ex = NULL;
#else
	regex_t *re, void *ex) {
	regfree(re);
	*re = (regex_t) {0};
	(void) ex;
#endif
}

static str
BATPCRElike_imp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const str *esc, const bit *isens, const bit *not)
{
	str msg = MAL_SUCCEED, input = NULL, pat = NULL;
	BAT *b = NULL, *pbn = NULL, *bn = NULL;
	char *ppat = NULL;
	bool use_re = false, use_strcmp = false, empty = false, isensitive = (bool) *isens, anti = (bool) *not, has_nil = false,
		 input_is_a_bat = isaBatType(getArgType(mb, pci, 1)), pattern_is_a_bat = isaBatType(getArgType(mb, pci, 2));
	bat *r = getArgReference_bat(stk, pci, 0);
	BUN q = 0;
	bit *ret = NULL;
#ifdef HAVE_LIBPCRE
	pcre *re = NULL;
	pcre_extra *ex = NULL;
#else
	regex_t re = (regex_t) {0};
	void *ex = NULL;
#endif
	struct RE *re_simple = NULL;
	uint32_t *wpat = NULL;
	BATiter bi = (BATiter) {0}, pi;

	(void) cntxt;
	if (input_is_a_bat) {
		bat *bid = getArgReference_bat(stk, pci, 1);
		if (!(b = BATdescriptor(*bid))) {
			msg = createException(MAL, "batalgebra.batpcrelike3", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	}
	if (pattern_is_a_bat) {
		bat *pb = getArgReference_bat(stk, pci, 2);
		if (!(pbn = BATdescriptor(*pb))) {
			msg = createException(MAL, "batalgebra.batpcrelike3", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	}
	assert((!b || ATOMstorage(b->ttype) == TYPE_str) && (!pbn || ATOMstorage(pbn->ttype) == TYPE_str));

	q = BATcount(b ? b : pbn);
	if (!(bn = COLnew(b ? b->hseqbase : pbn->hseqbase, TYPE_bit, q, TRANSIENT))) {
		msg = createException(MAL, "batalgebra.batpcrelike3", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	ret = (bit*) Tloc(bn, 0);

	if (pattern_is_a_bat) {
		pi = bat_iterator(pbn);
		if (b)
			bi = bat_iterator(b);
		else
			input = *getArgReference_str(stk, pci, 1);

		for (BUN p = 0; p < q; p++) {
			const str next_input = b ? BUNtail(bi, p) : input, np = BUNtail(pi, p);

			if ((msg = choose_like_path(&ppat, &use_re, &use_strcmp, &empty, &np, esc)) != MAL_SUCCEED)
				goto bailout;

			if (use_re) {
				if ((msg = re_like_build(&re_simple, &wpat, np, isensitive, use_strcmp, (unsigned char) **esc)) != MAL_SUCCEED)
					goto bailout;
				ret[p] = re_like_proj_apply(next_input, re_simple, wpat, np, isensitive, anti, use_strcmp);
				re_like_clean(&re_simple, &wpat);
			} else if (empty) {
				ret[p] = bit_nil;
			} else {
				if ((msg = pcre_like_build(&re, &ex, ppat, isensitive, 1)) != MAL_SUCCEED)
					goto bailout;
				if ((msg = pcre_like_apply(&(ret[p]), next_input, re, ex, ppat, anti)) != MAL_SUCCEED)
					goto bailout;
				pcre_clean(&re, &ex);
			}
			has_nil |= is_bit_nil(ret[p]);
			GDKfree(ppat);
			ppat = NULL;
		}
	} else {
		bi = bat_iterator(b);
		pat = *getArgReference_str(stk, pci, 2);
		if ((msg = choose_like_path(&ppat, &use_re, &use_strcmp, &empty, &pat, esc)) != MAL_SUCCEED)
			goto bailout;

		MT_thread_setalgorithm(empty ? "pcrelike: trivially empty" : use_strcmp ? "pcrelike: pattern matching using strcmp" :
							   use_re ? "pcrelike: pattern matching using RE" : "pcrelike: pattern matching using pcre");

		if (use_re) {
			if ((msg = re_like_build(&re_simple, &wpat, pat, isensitive, use_strcmp, (unsigned char) **esc)) != MAL_SUCCEED)
				goto bailout;
			for (BUN p = 0; p < q; p++) {
				const str s = BUNtail(bi, p);
				ret[p] = re_like_proj_apply(s, re_simple, wpat, pat, isensitive, anti, use_strcmp);
				has_nil |= is_bit_nil(ret[p]);
			}
		} else if (empty) {
			for (BUN p = 0; p < q; p++)
				ret[p] = bit_nil;
			has_nil = true;
		} else {
			if ((msg = pcre_like_build(&re, &ex, ppat, isensitive, q)) != MAL_SUCCEED)
				goto bailout;
			for (BUN p = 0; p < q; p++) {
				const str s = BUNtail(bi, p);
				if ((msg = pcre_like_apply(&(ret[p]), s, re, ex, ppat, anti)) != MAL_SUCCEED)
					goto bailout;
				has_nil |= is_bit_nil(ret[p]);
			}
		}
	}

bailout:
	GDKfree(ppat);
	re_like_clean(&re_simple, &wpat);
	pcre_clean(&re, &ex);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = has_nil;
		bn->tnonil = !has_nil;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		BBPkeepref(*r = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	if (b)
		BBPunfix(b->batCacheid);
	if (pbn)
		BBPunfix(pbn->batCacheid);
	return msg;
}

static str
BATPCRElike(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const str *esc = getArgReference_str(stk, pci, 3);
	const bit *ci = getArgReference_bit(stk, pci, 4);
	bit no = FALSE;

	return BATPCRElike_imp(cntxt, mb, stk, pci, esc, ci, &no);
}

static str
BATPCREnotlike(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const str *esc = getArgReference_str(stk, pci, 3);
	const bit *ci = getArgReference_bit(stk, pci, 4);
	bit yes = TRUE;

	return BATPCRElike_imp(cntxt, mb, stk, pci, esc, ci, &yes);
}

/* scan select loop with or without candidates */
#define pcrescanloop(TEST)		\
	do {	\
		TRC_DEBUG(ALGO,			\
				  "PCREselect(b=%s#"BUNFMT",anti=%d): "		\
				  "scanselect %s\n", BATgetId(b), BATcount(b),	\
				  anti, #TEST);		\
		if (!s || BATtdense(s)) {	\
			for (; p < q; p++) {	\
				const char *restrict v = BUNtvar(bi, p - off);	\
				if (TEST)	\
					vals[cnt++] = p;	\
			}		\
		} else {		\
			for (; p < ncands; p++) {		\
				oid o = canditer_next(ci);		\
				const char *restrict v = BUNtvar(bi, o - off);	\
				if (TEST) 	\
					vals[cnt++] = o;	\
			}		\
		}		\
	} while (0)

#ifdef HAVE_LIBPCRE
#define PCRE_LIKESELECT_BODY (pcre_exec(re, ex, v, (int) strlen(v), 0, 0, NULL, 0) >= 0)
#else
#define PCRE_LIKESELECT_BODY (regexec(&re, v, (size_t) 0, NULL, 0) != REG_NOMATCH)
#endif

static str
pcre_likeselect(BAT *bn, BAT *b, BAT *s, struct canditer *ci, BUN p, BUN q, BUN *rcnt, const char *pat, bool caseignore, bool anti)
{
#ifdef HAVE_LIBPCRE
	pcre *re = NULL;
	pcre_extra *ex = NULL;
#else
	regex_t re = (regex_t) {0};
	void *ex = NULL;
#endif
	BATiter bi = bat_iterator(b);
	BUN cnt = 0, ncands = ci->ncand;
	oid off = b->hseqbase, *restrict vals = Tloc(bn, 0);
	str msg = MAL_SUCCEED;

	if ((msg = pcre_like_build(&re, &ex, pat, caseignore, ci->ncand)) != MAL_SUCCEED)
		goto bailout;

	if (anti)
		pcrescanloop(v && *v != '\200' && !PCRE_LIKESELECT_BODY);
	else
		pcrescanloop(v && *v != '\200' && PCRE_LIKESELECT_BODY);

bailout:
	pcre_clean(&re, &ex);
	*rcnt = cnt;
	return msg;
}

static str
re_likeselect(BAT *bn, BAT *b, BAT *s, struct canditer *ci, BUN p, BUN q, BUN *rcnt, const char *pat, bool caseignore, bool anti, bool use_strcmp, uint32_t esc)
{
	BATiter bi = bat_iterator(b);
	BUN cnt = 0, ncands = ci->ncand;
	oid off = b->hseqbase, *restrict vals = Tloc(bn, 0);
	struct RE *re = NULL;
	uint32_t *wpat = NULL;
	str msg = MAL_SUCCEED;

	if ((msg = re_like_build(&re, &wpat, pat, caseignore, use_strcmp, esc)) != MAL_SUCCEED)
		goto bailout;

	if (use_strcmp) {
		if (caseignore) {
			if (anti)
				pcrescanloop(v && *v != '\200' && mywstrcasecmp(v, wpat) != 0);
			else
				pcrescanloop(v && *v != '\200' && mywstrcasecmp(v, wpat) == 0);
		} else {
			if (anti)
				pcrescanloop(v && *v != '\200' && strcmp(v, pat) != 0);
			else
				pcrescanloop(v && *v != '\200' && strcmp(v, pat) == 0);
		}
	} else {
		if (caseignore) {
			if (anti)
				pcrescanloop(v && *v != '\200' && !re_match_ignore(v, re));
			else
				pcrescanloop(v && *v != '\200' && re_match_ignore(v, re));
		} else {
			if (anti)
				pcrescanloop(v && *v != '\200' && !re_match_no_ignore(v, re));
			else
				pcrescanloop(v && *v != '\200' && re_match_no_ignore(v, re));
		}
	}

bailout:
	re_like_clean(&re, &wpat);
	*rcnt = cnt;
	return msg;
}

static str
PCRElikeselect(bat *ret, const bat *bid, const bat *sid, const str *pat, const str *esc, const bit *caseignore, const bit *anti)
{
	BAT *b, *s = NULL, *bn = NULL;
	str msg = MAL_SUCCEED;
	char *ppat = NULL;
	bool use_re = false, use_strcmp = false, empty = false;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(MAL, "algebra.likeselect", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(MAL, "algebra.likeselect", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}

	assert(ATOMstorage(b->ttype) == TYPE_str);
	if ((msg = choose_like_path(&ppat, &use_re, &use_strcmp, &empty, pat, esc)) != MAL_SUCCEED)
		goto bailout;

	MT_thread_setalgorithm(empty ? "pcrelike: trivially empty" : use_strcmp ? "pcrelike: pattern matching using strcmp" :
						   use_re ? "pcrelike: pattern matching using RE" : "pcrelike: pattern matching using pcre");

	if (empty) {
		if (!(bn = BATdense(0, 0, 0)))
			msg = createException(MAL, "algebra.likeselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		BUN p = 0, q = 0, rcnt = 0;
		struct canditer ci;

		canditer_init(&ci, b, s);
		if (!(bn = COLnew(0, TYPE_oid, ci.ncand, TRANSIENT))) {
			msg = createException(MAL, "algebra.likeselect", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}

		if (!s || BATtdense(s)) {
			if (s) {
				assert(BATtdense(s));
				p = (BUN) s->tseqbase;
				q = p + BATcount(s);
				if ((oid) p < b->hseqbase)
					p = b->hseqbase;
				if ((oid) q > b->hseqbase + BATcount(b))
					q = b->hseqbase + BATcount(b);
			} else {
				p = b->hseqbase;
				q = BUNlast(b) + b->hseqbase;
			}
		}

		if (use_re) {
			msg = re_likeselect(bn, b, s, &ci, p, q, &rcnt, *pat, (bool) *caseignore, (bool) *anti, use_strcmp, (unsigned char) **esc);
		} else {
			msg = pcre_likeselect(bn, b, s, &ci, p, q, &rcnt, ppat, (bool) *caseignore, (bool) *anti);
		}
		if (!msg) { /* set some properties */
			BATsetcount(bn, rcnt);
			bn->tsorted = true;
			bn->trevsorted = bn->batCount <= 1;
			bn->tkey = true;
			bn->tseqbase = rcnt == 0 ? 0 : rcnt == 1 || rcnt == b->batCount ? b->hseqbase : oid_nil;
		}
	}

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	GDKfree(ppat);
	if (bn && !msg)
		BBPkeepref(*ret = bn->batCacheid);
	else if (bn)
		BBPreclaim(bn);
	return msg;
}

#define APPEND(b, o)	(((oid *) b->theap->base)[b->batCount++] = (o))
#define VALUE(s, x)		(s##vars + VarHeapVal(s##vals, (x), s##width))

#ifdef HAVE_LIBPCRE
#define PCRE_EXEC \
	do { \
		retval = pcre_exec(pcrere, pcreex, vl, (int) strlen(vl), 0, 0, NULL, 0); \
	} while (0)
#define PCRE_EXEC_COND (retval < 0)
#else
#define PCRE_EXEC \
	do { \
		retval = regexec(&pcrere, vl, (size_t) 0, NULL, 0); \
	} while (0)
#define PCRE_EXEC_COND (retval == REG_NOMATCH || retval == REG_ENOSYS)
#endif

/* nested loop implementation for PCRE join */
#define pcre_join_loop(STRCMP, RE_MATCH, PCRE_COND) \
	do { \
		for (BUN ri = 0; ri < rci.ncand; ri++) { \
			ro = canditer_next(&rci); \
			vr = VALUE(r, ro - r->hseqbase); \
			nl = 0; \
			use_re = use_strcmp = empty = false; \
			if ((msg = choose_like_path(&pcrepat, &use_re, &use_strcmp, &empty, (const str*)&vr, (const str*)&esc))) \
				goto bailout; \
			if (!empty) { \
				if (use_re) { \
					if ((msg = re_like_build(&re, &wpat, vr, caseignore, use_strcmp, (unsigned char) *esc)) != MAL_SUCCEED) \
						goto bailout; \
				} else if (pcrepat) { \
					if ((msg = pcre_like_build(&pcrere, &pcreex, pcrepat, caseignore, lci.ncand)) != MAL_SUCCEED) \
						goto bailout; \
					GDKfree(pcrepat); \
					pcrepat = NULL; \
				} \
				canditer_reset(&lci); \
				for (BUN li = 0; li < lci.ncand; li++) { \
					lo = canditer_next(&lci); \
					vl = VALUE(l, lo - l->hseqbase); \
					if (strNil(vl)) { \
						continue; \
					} else if (use_re) { \
						if (use_strcmp) { \
							if (STRCMP) \
								continue; \
						} else { \
							assert(re); \
							if (RE_MATCH) \
								continue; \
						} \
					} else { \
						int retval; \
						PCRE_EXEC;  \
						if (PCRE_COND) \
							continue; \
					} \
					if (BUNlast(r1) == BATcapacity(r1)) { \
						newcap = BATgrows(r1); \
						BATsetcount(r1, BATcount(r1)); \
						if (r2) \
							BATsetcount(r2, BATcount(r2)); \
						if (BATextend(r1, newcap) != GDK_SUCCEED || (r2 && BATextend(r2, newcap) != GDK_SUCCEED)) { \
							msg = createException(MAL, "pcre.join", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
							goto bailout; \
						} \
						assert(!r2 || BATcapacity(r1) == BATcapacity(r2)); \
					} \
					if (BATcount(r1) > 0) { \
						if (lastl + 1 != lo) \
							r1->tseqbase = oid_nil; \
						if (nl == 0) { \
							if (r2) \
								r2->trevsorted = false; \
							if (lastl > lo) { \
								r1->tsorted = false; \
								r1->tkey = false; \
							} else if (lastl < lo) { \
								r1->trevsorted = false; \
							} else { \
								r1->tkey = false; \
							} \
						} \
					} \
					APPEND(r1, lo); \
					if (r2) \
						APPEND(r2, ro); \
					lastl = lo; \
					nl++; \
				} \
				re_like_clean(&re, &wpat); \
				pcre_clean(&pcrere, &pcreex); \
			} \
			if (r2) { \
				if (nl > 1) { \
					r2->tkey = false; \
					r2->tseqbase = oid_nil; \
					r1->trevsorted = false; \
				} else if (nl == 0) { \
					rskipped = BATcount(r2) > 0; \
				} else if (rskipped) { \
					r2->tseqbase = oid_nil; \
				} \
			} else if (nl > 1) { \
				r1->trevsorted = false; \
			} \
		} \
	} while (0)

static char *
pcrejoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr, const char *esc, bit caseignore, bit anti)
{
	struct canditer lci, rci;
	const char *lvals, *rvals, *lvars, *rvars, *vl, *vr;
	int lwidth, rwidth, rskipped = 0;	/* whether we skipped values in r */
	oid lo, ro, lastl = 0;		/* last value inserted into r1 */
	BUN nl, newcap;
	char *pcrepat = NULL, *msg = MAL_SUCCEED;
	struct RE *re = NULL;
	bool use_re = false, use_strcmp = false, empty = false;
	uint32_t *wpat = NULL;
#ifdef HAVE_LIBPCRE
	pcre *pcrere = NULL;
	pcre_extra *pcreex = NULL;
#else
	regex_t pcrere = (regex_t) {0};
	void *pcreex = NULL;
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
	if (r2) {
		r2->tkey = true;
		r2->tsorted = true;
		r2->trevsorted = true;
	}

	if (anti) {
		if (caseignore) {
			pcre_join_loop(mywstrcasecmp(vl, wpat) == 0, re_match_ignore(vl, re), !PCRE_EXEC_COND);
		} else {
			pcre_join_loop(strcmp(vl, vr) == 0, re_match_no_ignore(vl, re), !PCRE_EXEC_COND);
		}
	} else {
		if (caseignore) {
			pcre_join_loop(mywstrcasecmp(vl, wpat) != 0, !re_match_ignore(vl, re), PCRE_EXEC_COND);
		} else {
			pcre_join_loop(strcmp(vl, vr) != 0, !re_match_no_ignore(vl, re), PCRE_EXEC_COND);
		}
	}

	assert(!r2 || BATcount(r1) == BATcount(r2));
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	if (r2)
		BATsetcount(r2, BATcount(r2));
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2)
			r2->tseqbase = 0;
	}
	if (r2)
		TRC_DEBUG(ALGO,
				"pcrejoin(l=%s,r=%s)=(%s#"BUNFMT"%s%s,%s#"BUNFMT"%s%s\n",
				BATgetId(l), BATgetId(r),
				BATgetId(r1), BATcount(r1),
				r1->tsorted ? "-sorted" : "",
				r1->trevsorted ? "-revsorted" : "",
				BATgetId(r2), BATcount(r2),
				r2->tsorted ? "-sorted" : "",
				r2->trevsorted ? "-revsorted" : "");
	else
		TRC_DEBUG(ALGO,
			"pcrejoin(l=%s,r=%s)=(%s#"BUNFMT"%s%s\n",
			BATgetId(l), BATgetId(r),
			BATgetId(r1), BATcount(r1),
			r1->tsorted ? "-sorted" : "",
			r1->trevsorted ? "-revsorted" : "");
	return MAL_SUCCEED;

bailout:
	GDKfree(pcrepat);
	re_like_clean(&re, &wpat);
	pcre_clean(&pcrere, &pcreex);
	assert(msg != MAL_SUCCEED);
	return msg;
}

static str
PCREjoin(bat *r1, bat *r2, bat lid, bat rid, bat slid, bat srid, bat elid, bat ciid, bit anti)
{
	BAT *left = NULL, *right = NULL, *escape = NULL, *caseignore = NULL, *candleft = NULL, *candright = NULL;
	BAT *result1 = NULL, *result2 = NULL;
	char *msg = MAL_SUCCEED, *esc = "";
	bit ci;

	if ((left = BATdescriptor(lid)) == NULL)
		goto fail;
	if ((right = BATdescriptor(rid)) == NULL)
		goto fail;
	if ((escape = BATdescriptor(elid)) == NULL)
		goto fail;
	if ((caseignore = BATdescriptor(ciid)) == NULL)
		goto fail;
	if (!is_bat_nil(slid) && (candleft = BATdescriptor(slid)) == NULL)
		goto fail;
	if (!is_bat_nil(srid) && (candright = BATdescriptor(srid)) == NULL)
		goto fail;
	result1 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	if (r2)
		result2 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	if (!result1 || (r2 && !result2)) {
		msg = createException(MAL, "pcre.join", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto fail;
	}
	result1->tnil = false;
	result1->tnonil = true;
	result1->tkey = true;
	result1->tsorted = true;
	result1->trevsorted = true;
	result1->tseqbase = 0;
	if (r2) {
		result2->tnil = false;
		result2->tnonil = true;
		result2->tkey = true;
		result2->tsorted = true;
		result2->trevsorted = true;
		result2->tseqbase = 0;
	}
	if (BATcount(escape) != 1) {
		msg = createException(MAL, "pcre.join", SQLSTATE(42000) "At the moment, only one value is allowed for the escape input at pcre join");
		goto fail;
	}
	esc = BUNtvar(bat_iterator(escape), 0);
	if (BATcount(caseignore) != 1) {
		msg = createException(MAL, "pcre.join", SQLSTATE(42000) "At the moment, only one value is allowed for the case ignore input at pcre join");
		goto fail;
	}
	ci = *(bit*)BUNtail(bat_iterator(caseignore), 0);
	msg = pcrejoin(result1, result2, left, right, candleft, candright, esc, ci, anti);
	if (msg)
		goto fail;
	*r1 = result1->batCacheid;
	BBPkeepref(*r1);
	if (r2) {
		*r2 = result2->batCacheid;
		BBPkeepref(*r2);
	}
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	if (escape)
		BBPunfix(escape->batCacheid);
	if (caseignore)
		BBPunfix(caseignore->batCacheid);
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
	if (escape)
		BBPunfix(escape->batCacheid);
	if (caseignore)
		BBPunfix(caseignore->batCacheid);
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

static str
LIKEjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *elid, const bat *cid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate, const bit *anti)
{
	(void) nil_matches;
	(void) estimate;
	return PCREjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *elid, *cid, *anti);
}

static str
LIKEjoin1(bat *r1, const bat *lid, const bat *rid, const bat *elid, const bat *cid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate, const bit *anti)
{
	(void) nil_matches;
	(void) estimate;
	return PCREjoin(r1, NULL, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *elid, *cid, *anti);
}

#include "mel.h"
mel_atom pcre_init_atoms[] = {
 { .name="pcre", },  { .cmp=NULL }
};
mel_func pcre_init_funcs[] = {
 command("pcre", "index", PCREindex, false, "match a pattern, return matched position (or 0 when not found)", args(1,3, arg("",int),arg("pat",pcre),arg("s",str))),
 command("pcre", "match", PCREmatch, false, "Perl Compatible Regular Expression pattern matching against a string", args(1,3, arg("",bit),arg("s",str),arg("pat",str))),
 command("pcre", "imatch", PCREimatch, false, "Caseless Perl Compatible Regular Expression pattern matching against a string", args(1,3, arg("",bit),arg("s",str),arg("pat",str))),
 command("pcre", "patindex", PCREpatindex, false, "Location of the first POSIX pattern matching against a string", args(1,3, arg("",int),arg("pat",str),arg("s",str))),
 command("pcre", "replace", PCREreplace_wrap, false, "Replace _all_ matches of \"pattern\" in \"origin_str\" with \"replacement\".\nParameter \"flags\" accept these flags: 'i', 'm', 's', and 'x'.\n'e': if present, an empty string is considered to be a valid match\n'i': if present, the match operates in case-insensitive mode.\nOtherwise, in case-sensitive mode.\n'm': if present, the match operates in multi-line mode.\n's': if present, the match operates in \"dot-all\"\nThe specifications of the flags can be found in \"man pcreapi\"\nThe flag letters may be repeated.\nNo other letters than 'e', 'i', 'm', 's' and 'x' are allowed in \"flags\".\nReturns the replaced string, or if no matches found, the original string.", args(1,5, arg("",str),arg("origin",str),arg("pat",str),arg("repl",str),arg("flags",str))),
 command("pcre", "replace_first", PCREreplace_wrap, false, "Replace _the first_ match of \"pattern\" in \"origin_str\" with \"replacement\".\nParameter \"flags\" accept these flags: 'i', 'm', 's', and 'x'.\n'e': if present, an empty string is considered to be a valid match\n'i': if present, the match operates in case-insensitive mode.\nOtherwise, in case-sensitive mode.\n'm': if present, the match operates in multi-line mode.\n's': if present, the match operates in \"dot-all\"\nThe specifications of the flags can be found in \"man pcreapi\"\nThe flag letters may be repeated.\nNo other letters than 'e', 'i', 'm', 's' and 'x' are allowed in \"flags\".\nReturns the replaced string, or if no matches found, the original string.", args(1,5, arg("",str),arg("origin",str),arg("pat",str),arg("repl",str),arg("flags",str))),
 command("pcre", "pcre_quote", PCREquote, false, "Return a PCRE pattern string that matches the argument exactly.", args(1,2, arg("",str),arg("s",str))),
 command("pcre", "sql2pcre", PCREsql2pcre, false, "Convert a SQL like pattern with the given escape character into a PCRE pattern.", args(1,3, arg("",str),arg("pat",str),arg("esc",str))),
 command("pcre", "prelude", pcre_init, false, "Initialize pcre", args(1,1, arg("",void))),
 command("str", "replace", PCREreplace_wrap, false, "", args(1,5, arg("",str),arg("origin",str),arg("pat",str),arg("repl",str),arg("flags",str))),
 command("batpcre", "replace", PCREreplace_bat_wrap, false, "", args(1,5, batarg("",str),batarg("orig",str),arg("pat",str),arg("repl",str),arg("flag",str))),
 command("batpcre", "replace_first", PCREreplacefirst_bat_wrap, false, "", args(1,5, batarg("",str),batarg("orig",str),arg("pat",str),arg("repl",str),arg("flag",str))),
 command("algebra", "like", PCRElike, false, "", args(1,5, arg("",bit),arg("s",str),arg("pat",str),arg("esc",str),arg("caseignore",bit))),
 command("algebra", "not_like", PCREnotlike, false, "", args(1,5, arg("",bit),arg("s",str),arg("pat",str),arg("esc",str),arg("caseignore",bit))),
 pattern("batalgebra", "like", BATPCRElike, false, "", args(1,5, batarg("",bit),batarg("s",str),arg("pat",str),arg("esc",str),arg("caseignore",bit))),
 pattern("batalgebra", "like", BATPCRElike, false, "", args(1,5, batarg("",bit),arg("s",str),batarg("pat",str),arg("esc",str),arg("caseignore",bit))),
 pattern("batalgebra", "like", BATPCRElike, false, "", args(1,5, batarg("",bit),batarg("s",str),batarg("pat",str),arg("esc",str),arg("caseignore",bit))),
 pattern("batalgebra", "not_like", BATPCREnotlike, false, "", args(1,5, batarg("",bit),batarg("s",str),arg("pat",str),arg("esc",str),arg("caseignore",bit))),
 pattern("batalgebra", "not_like", BATPCREnotlike, false, "", args(1,5, batarg("",bit),arg("s",str),batarg("pat",str),arg("esc",str),arg("caseignore",bit))),
 pattern("batalgebra", "not_like", BATPCREnotlike, false, "", args(1,5, batarg("",bit),batarg("s",str),batarg("pat",str),arg("esc",str),arg("caseignore",bit))),
 command("algebra", "likeselect", PCRElikeselect, false, "Select all head values of the first input BAT for which the\ntail value is \"like\" the given (SQL-style) pattern and for\nwhich the head value occurs in the tail of the second input\nBAT.\nInput is a dense-headed BAT, output is a dense-headed BAT with in\nthe tail the head value of the input BAT for which the\nrelationship holds.  The output BAT is sorted on the tail value.", args(1,7, batarg("",oid),batarg("b",str),batarg("s",oid),arg("pat",str),arg("esc",str),arg("caseignore",bit),arg("anti",bit))),
 command("algebra", "likejoin", LIKEjoin, false, "Join the string bat L with the pattern bat R\nwith optional candidate lists SL and SR using pattern escape string ESC\nand doing a case sensitive match.\nThe result is two aligned bats with oids of matching rows.", args(2,11, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("esc",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
 command("algebra", "likejoin", LIKEjoin1, false, "The same as LIKEjoin_esc, but only produce one output", args(1,10,batarg("",oid),batarg("l",str),batarg("r",str),batarg("esc",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pcre_mal)
{ mal_module("pcre", pcre_init_atoms, pcre_init_funcs); }
