/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
	bool search:1, atend:1, case_ignore:1;
	size_t skip;			/* number of codepoints to skip before matching */
	size_t len;				/* number of bytes in string */
	size_t ulen;			/* number of codepoints in string */
	struct RE *n;
};

/* We cannot use strcasecmp and strncasecmp since they work byte for
 * byte and don't deal with multibyte encodings (such as UTF-8). */

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

/* returns true if the pattern does not contain wildcard
 * characters ('%' or '_') and no character is escaped
 */
static inline bool
is_strcmpable(const char *pat, const char *esc)
{
	if (pat[strcspn(pat, "%_")])
		return false;
	return strlen(esc) == 0 || strNil(esc) || strstr(pat, esc) == NULL;
}

/* Match regular expression by comparing bytes.
 */
static inline bool
re_match(const char *restrict s, const struct RE *restrict pattern)
{
	const struct RE *r;

	for (r = pattern; r; r = r->n) {
		for (size_t i = 0; i < r->skip; s++) {
			if (*s == 0)
				return false;
			i += (*s & 0xC0) != 0x80;
		}
		if (r->search) {
			if (r->atend) {
				/* we're searching for a string at the end, so just skip
				 * over everything and just compare with the tail of the
				 * haystack */
				size_t slen = strlen(s);
				if (slen < r->ulen) {
					/* remaining string too short: each codepoint
					 * requires at least one byte */
					return false;
				}
				const char *e = s + slen;
				if (!r->case_ignore) {
					if (slen < r->len) {
						/* remaining string is too short to match */
						return false;
					}
					e -= r->len;
					if ((*e & 0xC0) == 0x80) {
						/* not at start of a Unicode character, so
						 * cannot match (this test not strictly
						 * required: the strcmp should also return
						 * unequal) */
						return false;
					}
					return strcmp(e, r->k) == 0;
				}
				size_t ulen = r->ulen;
				while (e > s && ulen != 0) {
					ulen -= (*--e & 0xC0) != 0x80;
				}
				/* ulen != 0 means remaining string is too short */
				return ulen == 0 && GDKstrcasecmp(e, r->k) == 0;
			}
			/* in case we have a pattern consisting of % followed by _,
			 * we need to backtrack, so use recursion; here we know we
			 * have the %, look for an _ in the rest of the pattern
			 * (note %_ and _% are equivalent and is taken care of by
			 * the pattern construction in re_create) */
			for (const struct RE *p = r->n; p; p = p->n) {
				if (p->skip != 0) {
					struct RE pat = *r;
					pat.search = false;
					pat.skip = 0;
					do {
						if (re_match(s, &pat))
							return true;
						do
							s++;
						while (*s && (*s & 0xC0) == 0x80);
					} while (*s != 0);
					return false;
				}
			}
		}
		if (r->k[0] == 0 && (r->search || *s == 0))
			return true;
		if (r->case_ignore) {
			for (;;) {
				if (r->search && (s = GDKstrcasestr(s, r->k)) == NULL)
					return false;
				if (*s == '\0')
					return false;
				/* in "atend" comparison, compare whole string, else
				 * only part */
				if ((!r->search || r->atend) &&
					(r->atend ? GDKstrcasecmp(s, r->k) : GDKstrncasecmp(s, r->k, SIZE_MAX, r->len)) != 0) {
					/* no match */
					if (!r->search)
						return false;
					/* try again with next character */
					do
						s++;
					while (*s != '\0' && (*s & 0xC0) == 0x80);
					continue;
				}
				/* match; find end of match by counting codepoints */
				for (size_t i = 0; *s && i < r->ulen; s++)
					i += (*s & 0xC0) != 0x80;
				break;
			}
		} else {
			for (;;) {
				if (r->search && (s = strstr(s, r->k)) == NULL)
					return false;
				if (*s == '\0')
					return false;
				/* in "atend" comparison, include NUL byte in the compare */
				if ((!r->search || r->atend) &&
					strncmp(s, r->k, r->len + r->atend) != 0) {
					/* no match */
					if (!r->search)
						return false;
					/* try again with next character: have search start
					 * after current first byte */
					if ((s = strchr(s + 1, r->k[0])) == NULL)
						return false;
					continue;
				}
				/* match */
				s += r->len;
				break;
			}
		}
	}
	return true;
}

static void
re_destroy(struct RE *p)
{
	if (p) {
		GDKfree(p->k);
		do {
			struct RE *n = p->n;

			GDKfree(p);
			p = n;
		} while (p);
	}
}

/* Create a linked list of RE structures.  Depending on the
 * caseignore and the ascii_pattern flags, the w
 * (if caseignore == true && ascii_pattern == false) or the k
 * (in every other case) field is used.  These in the first
 * structure are allocated, whereas in all subsequent
 * structures the fields point into the allocated buffer of
 * the first.
 */
static struct RE *
re_create(const char *pat, bool caseignore, uint32_t esc)
{
	struct RE *r = GDKmalloc(sizeof(struct RE)), *n = r;
	bool escaped = false;
	char *p, *q;

	if (r == NULL)
		return NULL;
	*r = (struct RE) {
		.atend = true,
		.case_ignore = caseignore,
	};

	for (;;) {
		if (esc != '%' && *pat == '%') {
			pat++;					/* skip % */
			r->search = true;
		} else if (esc != '_' && *pat == '_') {
			pat++;
			r->skip++;
		} else {
			break;
		}
	}
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
			n->ulen += (*p & 0xC0) != 0x80;
			escaped = false;
		} else if ((unsigned char) *p == esc) {
			escaped = true;
		} else if (*p == '%' || *p == '_') {
			n->atend = false;
			bool search = false;
			size_t skip = 0;
			for (;;) {
				if (*p == '_')
					skip++;
				else if (*p == '%')
					search = true;
				else
					break;
				p++;
			}
			if (*p || skip != 0) {
				n = n->n = GDKmalloc(sizeof(struct RE));
				if (n == NULL)
					goto bailout;
				*n = (struct RE) {
					.search = search,
					.atend = true,
					.skip = skip,
					.k = p,
					.case_ignore = caseignore,
				};
			}
			*q = 0;
			q = p;
			continue;			/* skip increment, we already did it */
		} else {
			*q++ = *p;
			n->len++;
			n->ulen += (*p & 0xC0) != 0x80;
		}
		p++;
	}
	*q = 0;
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
	int options = PCRE_UTF8 | PCRE_NO_UTF8_CHECK | PCRE_MULTILINE;
	if (insensitive)
		options |= PCRE_CASELESS;

	if ((r = pcre_compile(pattern, options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, "pcre.compile", OPERATION_FAILED
			  " with\n'%s'\nat %d in\n'%s'.\n", err_p, errpos, pattern);
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
				backrefs[nbackrefs].idx = INT_MAX;	/* impossible value > 0 */
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
	int compile_options = PCRE_UTF8 | PCRE_NO_UTF8_CHECK;
	int exec_options = PCRE_NOTEMPTY | PCRE_NO_UTF8_CHECK;
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
			  OPERATION_FAILED
			  ": pcre compile of pattern (%s) failed at %d with\n'%s'.\n",
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
			  OPERATION_FAILED
			  ": pcre study of pattern (%s) failed with '%s'.\n", pattern,
			  err_p);
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
	const char *err_p = NULL;
	char *tmpres;
	int i, errpos = 0;
	int compile_options = PCRE_UTF8 | PCRE_NO_UTF8_CHECK;
	int exec_options = PCRE_NOTEMPTY | PCRE_NO_UTF8_CHECK;
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
	extra = pcre_study(pcre_code,
					   BATcount(origin_strs) >
					   JIT_COMPILE_MIN ? PCRE_STUDY_JIT_COMPILE : 0, &err_p);
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

	tmpbat = COLnew(origin_strs->hseqbase, TYPE_str, BATcount(origin_strs),
					TRANSIENT);

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
	BATiter origin_strsi = bat_iterator(origin_strs);
	BATloop(origin_strs, p, q) {
		origin_str = BUNtvar(origin_strsi, p);
		tmpres = single_replace(pcre_code, extra, origin_str,
								(int) strlen(origin_str), exec_options,
								ovector, ovecsize, replacement,
								len_replacement, backrefs, nbackrefs, global,
								tmpres, &max_dest_size);
		if (tmpres == NULL || BUNappend(tmpbat, tmpres, false) != GDK_SUCCEED) {
			bat_iterator_end(&origin_strsi);
			pcre_free_study(extra);
			pcre_free(pcre_code);
			GDKfree(ovector);
			GDKfree(tmpres);
			BBPreclaim(tmpbat);
			throw(MAL, global ? "batpcre.replace" : "batpcre.replace_first",
				  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	bat_iterator_end(&origin_strsi);
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
pcre_match_with_flags(bit *ret, const char *val, const char *pat,
					  const char *flags)
{
	int pos;
#ifdef HAVE_LIBPCRE
	const char *err_p = NULL;
	int errpos = 0;
	int options = PCRE_UTF8 | PCRE_NO_UTF8_CHECK;
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
	pos = pcre_exec(re, NULL, val, (int) strlen(val), 0, PCRE_NO_UTF8_CHECK,
					NULL, 0);
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
			  ": matching of regular expression (%s) failed with %d", pat, pos);
	return MAL_SUCCEED;
}

#ifdef HAVE_LIBPCRE
/* special characters in PCRE that need to be escaped */
static const char pcre_specials[] = "$()*+.?[\\]^{|}";
#else
/* special characters in POSIX basic regular expressions that need to
 * be escaped */
static const char pcre_specials[] = "$()*+.?[\\^{|";
#endif

/* change SQL LIKE pattern into PCRE pattern */
static str
sql2pcre(str *r, const char *pat, const char *esc_str)
{
	int escaped = 0;
	int hasWildcard = 0;
	char *ppat;
	int esc = strNil(esc_str) ? 0 : esc_str[0];	/* should change to utf8_convert() */
	int specials;
	int c;

	if (strlen(esc_str) > 1)
		throw(MAL, "pcre.sql2pcre",
			  SQLSTATE(22019) ILLEGAL_ARGUMENT
			  ": ESCAPE string must have length 1");
	if (pat == NULL)
		throw(MAL, "pcre.sql2pcre",
			  SQLSTATE(22019) ILLEGAL_ARGUMENT
			  ": (I)LIKE pattern must not be NULL");
	ppat = GDKmalloc(strlen(pat) * 3 +
					 3 /* 3 = "^'the translated regexp'$0" */ );
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
				if (specials) {	/* change ++ into \+ */
					*ppat++ = esc;
				} else {		/* do not escape simple escape symbols */
					ppat[-1] = esc;	/* overwrite backslash */
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
				ppat[-1] = c;	/* overwrite backslash of invalid escape */
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
			throw(MAL, "pcre.sql2pcre",
				  SQLSTATE(22019) ILLEGAL_ARGUMENT
				  ": (I)LIKE pattern must not end with escape character");
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
	char *ppat = GDKmalloc(len * 2 + 3 /* 3 = "^'the translated regexp'$0" */ );
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
PCREreplace_wrap(str *res, const str *or, const str *pat, const str *repl,
				 const str *flags)
{
	return pcre_replace(res, *or, *pat, *repl, *flags, true);
}

static str
PCREreplacefirst_wrap(str *res, const str *or, const str *pat, const str *repl,
					  const str *flags)
{
	return pcre_replace(res, *or, *pat, *repl, *flags, false);
}

static str
PCREreplace_bat_wrap(bat *res, const bat *bid, const str *pat, const str *repl,
					 const str *flags)
{
	BAT *b, *bn = NULL;
	str msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batpcre.replace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	msg = pcre_replace_bat(&bn, b, *pat, *repl, *flags, true);
	if (msg == MAL_SUCCEED) {
		*res = bn->batCacheid;
		BBPkeepref(bn);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

static str
PCREreplacefirst_bat_wrap(bat *res, const bat *bid, const str *pat,
						  const str *repl, const str *flags)
{
	BAT *b, *bn = NULL;
	str msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batpcre.replace_first", RUNTIME_OBJECT_MISSING);

	msg = pcre_replace_bat(&bn, b, *pat, *repl, *flags, false);
	if (msg == MAL_SUCCEED) {
		*res = bn->batCacheid;
		BBPkeepref(bn);
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
	if (pcre_exec(pattern, NULL, *s, (int) strlen(*s), 0,
				  PCRE_NO_UTF8_CHECK, v, 3) >= 0) {
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

	*ret = p = GDKmalloc(strlen(s) * 2 + 1);	/* certainly long enough */
	if (p == NULL)
		throw(MAL, "pcre.quote", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	/* quote all non-alphanumeric ASCII characters (i.e. leave
	   non-ASCII and alphanumeric alone) */
	while (*s) {
		if (!((*s & 0x80) != 0 ||
			  ('a' <= *s && *s <= 'z') ||
			  ('A' <= *s && *s <= 'Z') || isdigit((unsigned char) *s)))
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
choose_like_path(bool *use_re, bool *use_strcmp, bool *empty,
				 const char *pat, const char *esc)
{
	str res = MAL_SUCCEED;
	*use_re = false;
	*use_strcmp = false;
	*empty = false;


	if (strNil(pat) || strNil(esc)) {
		*empty = true;
	} else {
		if (!re_is_pattern_properly_escaped(pat, (unsigned char) *esc))
			throw(MAL, "pcre.sql2pcre",
				  SQLSTATE(22019) ILLEGAL_ARGUMENT
				  ": (I)LIKE pattern must not end with escape character");
		if (is_strcmpable(pat, esc)) {
			*use_re = true;
			*use_strcmp = true;
		} else {
			*use_re = true;
		}
	}
	return res;
}

static str
PCRElike_imp(bit *ret, const str *s, const str *pat, const str *esc,
			 const bit *isens)
{
	str res = MAL_SUCCEED;
	bool use_re = false, use_strcmp = false, empty = false;
	struct RE *re = NULL;

	if ((res = choose_like_path(&use_re, &use_strcmp, &empty,
								*pat, *esc)) != MAL_SUCCEED)
		return res;

	MT_thread_setalgorithm(empty ? "pcrelike: trivially empty" : use_strcmp ?
						   "pcrelike: pattern matching using strcmp" : use_re ?
						   "pcrelike: pattern matching using RE" :
						   "pcrelike: pattern matching using pcre");

	if (strNil(*s) || empty) {
		*ret = bit_nil;
	} else {
		if (use_strcmp) {
			*ret = *isens ? GDKstrcasecmp(*s, *pat) == 0
				: strcmp(*s, *pat) == 0;
		} else {
			if (!(re = re_create(*pat, *isens, (unsigned char) **esc)))
				res = createException(MAL, "pcre.like4",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			else
				*ret = re_match(*s, re);
		}
	}

	if (re)
		re_destroy(re);
	return res;
}

static str
PCRElike(bit *ret, const str *s, const str *pat, const str *esc,
		 const bit *isens)
{
	return PCRElike_imp(ret, s, pat, esc, isens);
}

static str
PCREnotlike(bit *ret, const str *s, const str *pat, const str *esc,
			const bit *isens)
{
	str tmp;
	bit r;

	rethrow("str.not_like", tmp, PCRElike(&r, s, pat, esc, isens));
	*ret = r == bit_nil ? bit_nil : !r;
	return MAL_SUCCEED;
}

static inline str
re_like_build(struct RE **re, const char *pat, bool caseignore,
			  bool use_strcmp, uint32_t esc)
{
	if (!use_strcmp) {
		if (!(*re = re_create(pat, caseignore, esc)))
			return createException(MAL, "pcre.re_like_build",
								   SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

static inline bit
re_like_proj_apply(const char *s, const struct RE *restrict re,
				   const char *pat,
				   bool caseignore, bool anti, bool use_strcmp)
{
	if (strNil(s))
		return bit_nil;
	if (use_strcmp) {
		if (caseignore) {
			if (anti)
				return GDKstrcasecmp(s, pat) != 0;
			else
				return GDKstrcasecmp(s, pat) == 0;
		} else {
			if (anti)
				return strcmp(s, pat) != 0;
			else
				return strcmp(s, pat) == 0;
		}
	} else {
		if (anti)
			return !re_match(s, re);
		else
			return re_match(s, re);
	}
}

static inline void
re_like_clean(struct RE **re)
{
	if (*re) {
		re_destroy(*re);
		*re = NULL;
	}
}

static str
BATPCRElike_imp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
				const str *esc, const bit *isens, const bit *not)
{
	str msg = MAL_SUCCEED;
	BAT *b = NULL, *pbn = NULL, *bn = NULL;
	const char *input = NULL;
	bool use_re = false,
		use_strcmp = false,
		empty = false,
		isensitive = (bool) *isens,
		anti = (bool) *not,
		has_nil = false,
		input_is_a_bat = isaBatType(getArgType(mb, pci, 1)),
		pattern_is_a_bat = isaBatType(getArgType(mb, pci, 2));
	bat *r = getArgReference_bat(stk, pci, 0);
	BUN q = 0;
	bit *restrict ret = NULL;
	struct RE *re_simple = NULL;
	BATiter bi = (BATiter) { 0 }, pi;

	(void) cntxt;
	if (input_is_a_bat) {
		bat *bid = getArgReference_bat(stk, pci, 1);
		if (!(b = BATdescriptor(*bid))) {
			msg = createException(MAL, "batalgebra.batpcrelike3",
								  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	}
	if (pattern_is_a_bat) {
		bat *pb = getArgReference_bat(stk, pci, 2);
		if (!(pbn = BATdescriptor(*pb))) {
			msg = createException(MAL, "batalgebra.batpcrelike3",
								  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	}
	assert((!b || ATOMstorage(b->ttype) == TYPE_str)
		   && (!pbn || ATOMstorage(pbn->ttype) == TYPE_str));

	q = BATcount(b ? b : pbn);
	if (!(bn = COLnew(b ? b->hseqbase : pbn->hseqbase, TYPE_bit, q, TRANSIENT))) {
		msg = createException(MAL, "batalgebra.batpcrelike3",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	ret = (bit *) Tloc(bn, 0);

	if (pattern_is_a_bat) {
		pi = bat_iterator(pbn);
		if (b)
			bi = bat_iterator(b);
		else
			input = *getArgReference_str(stk, pci, 1);

		for (BUN p = 0; p < q; p++) {
			const char *next_input = b ? BUNtvar(bi, p) : input,
				*np = BUNtvar(pi, p);

			if ((msg = choose_like_path(&use_re, &use_strcmp, &empty,
										np, *esc)) != MAL_SUCCEED) {
				bat_iterator_end(&pi);
				if (b)
					bat_iterator_end(&bi);
				goto bailout;
			}

			if (empty) {
				ret[p] = bit_nil;
			} else {
				if ((msg = re_like_build(&re_simple, np, isensitive,
										 use_strcmp,
										 (unsigned char) **esc)) != MAL_SUCCEED) {
					bat_iterator_end(&pi);
					if (b)
						bat_iterator_end(&bi);
					goto bailout;
				}
				ret[p] = re_like_proj_apply(next_input, re_simple, np,
											isensitive, anti, use_strcmp);
				re_like_clean(&re_simple);
			}
			has_nil |= is_bit_nil(ret[p]);
		}
		bat_iterator_end(&pi);
		if (b)
			bat_iterator_end(&bi);
	} else {
		const char *pat = *getArgReference_str(stk, pci, 2);
		if ((msg = choose_like_path(&use_re, &use_strcmp, &empty,
									pat, *esc)) != MAL_SUCCEED)
			goto bailout;

		bi = bat_iterator(b);
		MT_thread_setalgorithm(empty ? "pcrelike: trivially empty" : use_strcmp
							   ? "pcrelike: pattern matching using strcmp" :
							   use_re ? "pcrelike: pattern matching using RE" :
							   "pcrelike: pattern matching using pcre");

		if (empty) {
			for (BUN p = 0; p < q; p++)
				ret[p] = bit_nil;
			has_nil = true;
		} else {
			if ((msg = re_like_build(&re_simple, pat, isensitive, use_strcmp,
									 (unsigned char) **esc)) != MAL_SUCCEED) {
				bat_iterator_end(&bi);
				goto bailout;
			}
			for (BUN p = 0; p < q; p++) {
				const char *s = BUNtvar(bi, p);
				ret[p] = re_like_proj_apply(s, re_simple, pat, isensitive,
											anti, use_strcmp);
				has_nil |= is_bit_nil(ret[p]);
			}
		}
		bat_iterator_end(&bi);
	}

  bailout:
	re_like_clean(&re_simple);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = has_nil;
		bn->tnonil = !has_nil;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		*r = bn->batCacheid;
		BBPkeepref(bn);
	} else if (bn)
		BBPreclaim(bn);
	BBPreclaim(b);
	BBPreclaim(pbn);
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
#define pcrescanloop(TEST, KEEP_NULLS)									\
	do {																\
		TRC_DEBUG(ALGO,													\
				  "PCREselect(b=%s#"BUNFMT",anti=%d): "					\
				  "scanselect %s\n", BATgetId(b), BATcount(b),			\
				  anti, #TEST);											\
		if (!s || BATtdense(s)) {										\
			for (; p < q; p++) {										\
				GDK_CHECK_TIMEOUT(qry_ctx, counter,						\
								  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx)); \
				const char *restrict v = BUNtvar(bi, p - off);			\
				if ((TEST) || ((KEEP_NULLS) && strNil(v)))				\
					vals[cnt++] = p;									\
			}															\
		} else {														\
			for (; p < ncands; p++) {									\
				GDK_CHECK_TIMEOUT(qry_ctx, counter,						\
								  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx)); \
				oid o = canditer_next(ci);								\
				const char *restrict v = BUNtvar(bi, o - off);			\
				if ((TEST) || ((KEEP_NULLS) && strNil(v)))				\
					vals[cnt++] = o;									\
			}															\
		}																\
	} while (0)

static str
re_likeselect(BAT *bn, BAT *b, BAT *s, struct canditer *ci, BUN p, BUN q,
			  BUN *rcnt, const char *pat, bool caseignore, bool anti,
			  bool use_strcmp, uint32_t esc, bool keep_nulls)
{
	BATiter bi = bat_iterator(b);
	BUN cnt = 0, ncands = ci->ncand;
	oid off = b->hseqbase, *restrict vals = Tloc(bn, 0);
	struct RE *re = NULL;
	str msg = MAL_SUCCEED;

	size_t counter = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	if ((msg = re_like_build(&re, pat, caseignore, use_strcmp,
							 esc)) != MAL_SUCCEED)
		goto bailout;

	if (use_strcmp) {
		if (caseignore) {
			if (anti)
				pcrescanloop(!strNil(v)
							 && GDKstrcasecmp(v, pat) != 0, keep_nulls);
			else
				pcrescanloop(!strNil(v)
							 && GDKstrcasecmp(v, pat) == 0, keep_nulls);
		} else {
			if (anti)
				pcrescanloop(!strNil(v) && strcmp(v, pat) != 0, keep_nulls);
			else
				pcrescanloop(!strNil(v) && strcmp(v, pat) == 0, keep_nulls);
		}
	} else {
		if (caseignore) {
			if (anti) {
				pcrescanloop(!strNil(v)
							 && !re_match(v, re), keep_nulls);
			} else {
				pcrescanloop(!strNil(v)
							 && re_match(v, re), keep_nulls);
			}
		} else {
			if (anti)
				pcrescanloop(!strNil(v)
							 && !re_match(v, re), keep_nulls);
			else
				pcrescanloop(!strNil(v)
							 && re_match(v, re), keep_nulls);
		}
	}

  bailout:
	bat_iterator_end(&bi);
	re_like_clean(&re);
	*rcnt = cnt;
	return msg;
}

static str
PCRElikeselect(bat *ret, const bat *bid, const bat *sid, const str *pat,
			   const str *esc, const bit *caseignore, const bit *anti)
{
	BAT *b, *s = NULL, *bn = NULL, *old_s = NULL;
	str msg = MAL_SUCCEED;
	bool use_re = false,
		use_strcmp = false,
		empty = false;
	bool with_strimps = false;
	bool with_strimps_anti = false;
	BUN p = 0, q = 0, rcnt = 0;
	struct canditer ci;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(MAL, "algebra.likeselect",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(MAL, "algebra.likeselect",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}

	assert(ATOMstorage(b->ttype) == TYPE_str);

	if ((msg = choose_like_path(&use_re, &use_strcmp, &empty,
								*pat, *esc)) != MAL_SUCCEED)
		goto bailout;

	if (empty) {
		if (!(bn = BATdense(0, 0, 0)))
			msg = createException(MAL, "algebra.likeselect",
								  SQLSTATE(HY013) MAL_MALLOC_FAIL);

		goto bailout;
	}
	/* Since the strimp pre-filtering of a LIKE query produces a superset of the actual result the complement of that
	 * set will necessarily reject some of the matching entries in the NOT LIKE query.
	 *
	 * In this case we run the PCRElikeselect as a LIKE query with strimps and return the complement of the result,
	 * taking extra care to not return NULLs. This currently means that we do not run strimps for NOT LIKE queries if
	 * the BAT contains NULLs.
	 */
	if (BAThasstrimps(b)) {
		if (STRMPcreate(b, NULL) == GDK_SUCCEED) {
			BAT *tmp_s = STRMPfilter(b, s, *pat, *anti);
			if (tmp_s) {
				old_s = s;
				s = tmp_s;
				if (!*anti)
					with_strimps = true;
				else
					with_strimps_anti = true;
			}
		} else {				/* If we cannot filter with the strimp just continue normally */
			GDKclrerr();
		}
	}


	MT_thread_setalgorithm(use_strcmp
						   ? (with_strimps ?
							  "pcrelike: pattern matching using strcmp with strimps"
							  : (with_strimps_anti ?
								 "pcrelike: pattern matching using strcmp with strimps anti"
								 : "pcrelike: pattern matching using strcmp")) :
						   use_re ? (with_strimps ?
									 "pcrelike: pattern matching using RE with strimps"
									 : (with_strimps_anti ?
										"pcrelike: patterm matching using RE with strimps anti"
										:
										"pcrelike: pattern matching using RE"))
						   : (with_strimps ?
							  "pcrelike: pattern matching using pcre with strimps"
							  : (with_strimps_anti ?
								 "pcrelike: pattermatching using pcre with strimps anti"
								 : "pcrelike: pattern matching using pcre")));

	canditer_init(&ci, b, s);
	if (!(bn = COLnew(0, TYPE_oid, ci.ncand, TRANSIENT))) {
		msg = createException(MAL, "algebra.likeselect",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
			q = BATcount(b) + b->hseqbase;
		}
	}

	msg = re_likeselect(bn, b, s, &ci, p, q, &rcnt, *pat, *caseignore, *anti
						&& !with_strimps_anti, use_strcmp,
						(unsigned char) **esc, with_strimps_anti);

	if (!msg) {					/* set some properties */
		BATsetcount(bn, rcnt);
		bn->tsorted = true;
		bn->trevsorted = bn->batCount <= 1;
		bn->tkey = true;
		bn->tnil = false;
		bn->tnonil = true;
		bn->tseqbase = rcnt == 0 ? 0 : rcnt == 1 ? *(const oid *) Tloc(bn, 0) : rcnt == b->batCount ? b->hseqbase : oid_nil;
		if (with_strimps_anti) {
			/* Reverse the result taking into account the original candidate list. */
			// BAT *rev = BATdiffcand(BATdense(b->hseqbase, 0, b->batCount), bn);
			BAT *rev;
			if (old_s) {
				rev = BATdiffcand(old_s, bn);
#ifndef NDEBUG
				BAT *is = BATintersectcand(old_s, bn);
				if (is) {
					assert(is->batCount == bn->batCount);
					BBPreclaim(is);
				}
				assert(rev->batCount == old_s->batCount - bn->batCount);
#endif
			}

			else
				rev = BATnegcands(b->batCount, bn);
			/* BAT *rev = BATnegcands(b->batCount, bn); */
			BBPunfix(bn->batCacheid);
			bn = rev;
		}
	}


  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	BBPreclaim(old_s);
	if (bn && !msg) {
		*ret = bn->batCacheid;
		BBPkeepref(bn);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

#define APPEND(b, o)	(((oid *) b->theap->base)[b->batCount++] = (o))
#define VALUE(s, x)		(s##vars + VarHeapVal(s##vals, (x), s##i.width))

/* nested loop implementation for PCRE join */
#define pcre_join_loop(STRCMP, RE_MATCH)								\
	do {																\
		for (BUN ridx = 0; ridx < rci.ncand; ridx++) {					\
			ro = canditer_next(&rci);									\
			vr = VALUE(r, ro - rbase);									\
			nl = 0;														\
			use_re = use_strcmp = empty = false;						\
			if ((msg = choose_like_path(&use_re, &use_strcmp, &empty, vr, esc))) \
				goto bailout;											\
			if (!empty) {												\
				if ((msg = re_like_build(&re, vr, false, use_strcmp, (unsigned char) *esc)) != MAL_SUCCEED) \
					goto bailout;										\
				canditer_reset(&lci);									\
				TIMEOUT_LOOP_IDX_DECL(lidx, lci.ncand, qry_ctx) {		\
					lo = canditer_next(&lci);							\
					vl = VALUE(l, lo - lbase);							\
					if (strNil(vl)) {									\
						continue;										\
					} else {											\
						if (use_strcmp) {								\
							if (STRCMP)									\
								continue;								\
						} else {										\
							assert(re);									\
							if (RE_MATCH)								\
								continue;								\
						}												\
					}													\
					if (BATcount(r1) == BATcapacity(r1)) {				\
						newcap = BATgrows(r1);							\
						BATsetcount(r1, BATcount(r1));					\
						if (r2)											\
							BATsetcount(r2, BATcount(r2));				\
						if (BATextend(r1, newcap) != GDK_SUCCEED || (r2 && BATextend(r2, newcap) != GDK_SUCCEED)) { \
							msg = createException(MAL, "pcre.join", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
							goto bailout;								\
						}												\
						assert(!r2 || BATcapacity(r1) == BATcapacity(r2)); \
					}													\
					if (BATcount(r1) > 0) {								\
						if (lastl + 1 != lo)							\
							r1->tseqbase = oid_nil;						\
						if (nl == 0) {									\
							if (r2)										\
								r2->trevsorted = false;					\
							if (lastl > lo) {							\
								r1->tsorted = false;					\
								r1->tkey = false;						\
							} else if (lastl < lo) {					\
								r1->trevsorted = false;					\
							} else {									\
								r1->tkey = false;						\
							}											\
						}												\
					}													\
					APPEND(r1, lo);										\
					if (r2)												\
						APPEND(r2, ro);									\
					lastl = lo;											\
					nl++;												\
				}														\
				re_like_clean(&re);										\
				TIMEOUT_CHECK(qry_ctx,									\
							  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx)); \
			}															\
			if (r2) {													\
				if (nl > 1) {											\
					r2->tkey = false;									\
					r2->tseqbase = oid_nil;								\
					r1->trevsorted = false;								\
				} else if (nl == 0) {									\
					rskipped = BATcount(r2) > 0;						\
				} else if (rskipped) {									\
					r2->tseqbase = oid_nil;								\
				}														\
			} else if (nl > 1) {										\
				r1->trevsorted = false;									\
			}															\
		}																\
	} while (0)

static char *
pcrejoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr, const char *esc,
		 bit caseignore, bit anti)
{
	struct canditer lci, rci;
	const char *lvals, *rvals, *lvars, *rvars, *vl, *vr;
	int rskipped = 0;			/* whether we skipped values in r */
	oid lbase, rbase, lo, ro, lastl = 0;	/* last value inserted into r1 */
	BUN nl, newcap;
	char *msg = MAL_SUCCEED;
	struct RE *re = NULL;
	bool use_re = false,
		use_strcmp = false,
		empty = false;
	lng t0 = 0;

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);

	BAT *ol = NULL, *or = NULL;
	if (caseignore) {
		ol = l;
		or = r;
		l = BATcasefold(l, NULL);
		r = BATcasefold(r, NULL);
		if (l == NULL || r == NULL) {
			BBPreclaim(l);
			BBPreclaim(r);
			throw(MAL, "pcre.join", GDK_EXCEPTION);
		}
	}

	canditer_init(&lci, l, sl);
	canditer_init(&rci, r, sr);

	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	lbase = l->hseqbase;
	rbase = r->hseqbase;
	lvals = (const char *) li.base;
	rvals = (const char *) ri.base;
	assert(ri.vh && r->ttype);
	lvars = li.vh->base;
	rvars = ri.vh->base;

	r1->tkey = true;
	r1->tsorted = true;
	r1->trevsorted = true;
	r1->tnil = false;
	r1->tnonil = true;
	if (r2) {
		r2->tkey = true;
		r2->tsorted = true;
		r2->trevsorted = true;
		r2->tnil = false;
		r2->tnonil = true;
	}

	if (anti) {
		pcre_join_loop(strcmp(vl, vr) == 0, re_match(vl, re));
	} else {
		pcre_join_loop(strcmp(vl, vr) != 0, !re_match(vl, re));
	}
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	if (ol) {
		BBPreclaim(l);
		BBPreclaim(r);
		l = ol;
		r = or;
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
				  "l=%s#" BUNFMT "[%s]%s%s,"
				  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
				  "sr=%s#" BUNFMT "%s%s -> "
				  "%s#" BUNFMT "%s%s,%s#" BUNFMT "%s%s (" LLFMT " usec)\n",
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
				  sr && sr->trevsorted ? "-revsorted" : "",
				  BATgetId(r1), BATcount(r1),
				  r1->tsorted ? "-sorted" : "",
				  r1->trevsorted ? "-revsorted" : "",
				  BATgetId(r2), BATcount(r2),
				  r2->tsorted ? "-sorted" : "",
				  r2->trevsorted ? "-revsorted" : "", GDKusec() - t0);
	else
		TRC_DEBUG(ALGO,
				  "l=%s#" BUNFMT "[%s]%s%s,"
				  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
				  "sr=%s#" BUNFMT "%s%s -> "
				  "%s#" BUNFMT "%s%s (" LLFMT " usec)\n",
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
				  sr && sr->trevsorted ? "-revsorted" : "",
				  BATgetId(r1), BATcount(r1),
				  r1->tsorted ? "-sorted" : "",
				  r1->trevsorted ? "-revsorted" : "", GDKusec() - t0);
	return MAL_SUCCEED;

  bailout:
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	re_like_clean(&re);
	assert(msg != MAL_SUCCEED);
	return msg;
}

static str
PCREjoin(bat *r1, bat *r2, bat lid, bat rid, bat slid, bat srid, bat elid,
		 bat ciid, bit anti)
{
	BAT *left = NULL, *right = NULL, *escape = NULL, *caseignore = NULL,
		*candleft = NULL, *candright = NULL;
	BAT *result1 = NULL, *result2 = NULL;
	char *msg = MAL_SUCCEED;
	const char *esc = "";
	bit ci;
	BATiter bi;

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
		msg = createException(MAL, "pcre.join",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		msg = createException(MAL, "pcre.join",
							  SQLSTATE(42000)
							  "At the moment, only one value is allowed for the escape input at pcre join");
		goto fail;
	}
	if (BATcount(caseignore) != 1) {
		msg = createException(MAL, "pcre.join",
							  SQLSTATE(42000)
							  "At the moment, only one value is allowed for the case ignore input at pcre join");
		goto fail;
	}
	bi = bat_iterator(caseignore);
	ci = *(bit *) BUNtloc(bi, 0);
	bat_iterator_end(&bi);
	bi = bat_iterator(escape);
	esc = BUNtvar(bi, 0);
	msg = pcrejoin(result1, result2, left, right, candleft, candright, esc, ci,
				   anti);
	bat_iterator_end(&bi);
	if (msg)
		goto fail;
	*r1 = result1->batCacheid;
	BBPkeepref(result1);
	if (r2) {
		*r2 = result2->batCacheid;
		BBPkeepref(result2);
	}
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	BBPreclaim(escape);
	BBPreclaim(caseignore);
	BBPreclaim(candleft);
	BBPreclaim(candright);
	return MAL_SUCCEED;

  fail:
	BBPreclaim(left);
	BBPreclaim(right);
	BBPreclaim(escape);
	BBPreclaim(caseignore);
	BBPreclaim(candleft);
	BBPreclaim(candright);
	BBPreclaim(result1);
	BBPreclaim(result2);
	if (msg)
		return msg;
	throw(MAL, "pcre.join", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

static str
LIKEjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *elid,
		 const bat *cid, const bat *slid, const bat *srid,
		 const bit *nil_matches, const lng *estimate, const bit *anti)
{
	(void) nil_matches;
	(void) estimate;
	return PCREjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0,
					*elid, *cid, *anti);
}

static str
LIKEjoin1(bat *r1, const bat *lid, const bat *rid, const bat *elid,
		  const bat *cid, const bat *slid, const bat *srid,
		  const bit *nil_matches, const lng *estimate, const bit *anti)
{
	(void) nil_matches;
	(void) estimate;
	return PCREjoin(r1, NULL, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0,
					*elid, *cid, *anti);
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
 command("pcre", "replace_first", PCREreplacefirst_wrap, false, "Replace _the first_ match of \"pattern\" in \"origin_str\" with \"replacement\".\nParameter \"flags\" accept these flags: 'i', 'm', 's', and 'x'.\n'e': if present, an empty string is considered to be a valid match\n'i': if present, the match operates in case-insensitive mode.\nOtherwise, in case-sensitive mode.\n'm': if present, the match operates in multi-line mode.\n's': if present, the match operates in \"dot-all\"\nThe specifications of the flags can be found in \"man pcreapi\"\nThe flag letters may be repeated.\nNo other letters than 'e', 'i', 'm', 's' and 'x' are allowed in \"flags\".\nReturns the replaced string, or if no matches found, the original string.", args(1,5, arg("",str),arg("origin",str),arg("pat",str),arg("repl",str),arg("flags",str))),
 command("pcre", "pcre_quote", PCREquote, false, "Return a PCRE pattern string that matches the argument exactly.", args(1,2, arg("",str),arg("s",str))),
 command("pcre", "sql2pcre", PCREsql2pcre, false, "Convert a SQL like pattern with the given escape character into a PCRE pattern.", args(1,3, arg("",str),arg("pat",str),arg("esc",str))),
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
