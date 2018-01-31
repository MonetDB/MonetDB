/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

#ifdef HAVE_LIBPCRE
#include <pcre.h>
#ifndef PCRE_STUDY_JIT_COMPILE
/* old library version on e.g. EPEL 6 */
#define pcre_free_study(x)		pcre_free(x)
#endif

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
	int search;
	int skip;
	int len;
	struct RE *n;
} RE;

#ifndef HAVE_STRCASESTR
static const char *
strcasestr(const char *haystack, const char *needle)
{
	const char *p, *np = 0, *startn = 0;

	for (p = haystack; *p; p++) {
		if (np) {
			if (toupper(*p) == toupper(*np)) {
				if (!*++np)
					return startn;
			} else
				np = 0;
		} else if (toupper(*p) == toupper(*needle)) {
			np = needle + 1;
			startn = p;
			if (!*np)
				return startn;
		}
	}

	return 0;
}
#endif

static int
re_simple(const char *pat)
{
	int nr = 0;

	if (pat == 0)
		return 0;
	if (*pat == '%')
		pat++;
	while (*pat) {
		if (*pat == '_')
			return 0;
		if (*pat++ == '%')
			nr++;
	}
	if (*(pat-1) != '%')
		return 0;
	return nr;
}

static int
is_strcmpable(const char *pat, const str esc)
{
	if (pat[strcspn(pat, "%_")])
		return 0;
	return strlen(esc) == 0 || strstr(pat, esc) == NULL;
}

static int
re_match_ignore(const char *s, RE *pattern)
{
	RE *r;

	for (r = pattern; r; r = r->n) {
		if (!*s ||
			(!r->search && strncasecmp(s, r->k, r->len) != 0) ||
			(r->search && (s = strcasestr(s, r->k)) == NULL))
			return 0;
		s += r->len;
	}
	return 1;
}

static int
re_match_no_ignore(const char *s, RE *pattern)
{
	RE *r;

	for (r = pattern; r; r = r->n) {
		if (!*s ||
			(!r->search && strncmp(s, r->k, r->len) != 0) ||
			(r->search && (s = strstr(s, r->k)) == NULL))
			return 0;
		s += r->len;
	}
	return 1;
}

static void
re_destroy(RE *p)
{
	while (p) {
		RE *n = p->n;

		GDKfree(p->k);
		GDKfree(p);
		p = n;
	}
}

static RE *
re_create(const char *pat, int nr)
{
	char *x = GDKstrdup(pat);
	RE *r = (RE*)GDKmalloc(sizeof(RE)), *n = r;
	char *p = x, *q = x;

	if (x == NULL || r == NULL) {
		GDKfree(x);
		GDKfree(r);
		return NULL;
	}
	r->n = NULL;
	r->search = 0;
	r->skip = 0;
	r->k = NULL;

	if (*p == '%') {
		p++; /* skip % */
		r->search = 1;
	}
	q = p;
	while ((q = strchr(p, '%')) != NULL) {
		*q = 0;
		n->k = GDKstrdup(p);
		if (n->k == NULL)
			goto bailout;
		n->len = (int) strlen(n->k);
		if (--nr > 0) {
			n = n->n = (RE*)GDKmalloc(sizeof(RE));
			if (n == NULL)
				goto bailout;
			n->search = 1;
			n->skip = 0;
			n->n = NULL;
			n->k = NULL;
		}
		p = q + 1;
	}
	GDKfree(x);
	return r;
  bailout:
	GDKfree(x);
	re_destroy(r);
	return NULL;
}

static str
pcre_compile_wrap(pcre **res, const char *pattern, bit insensitive)
{
#ifdef HAVE_LIBPCRE
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
#else
	(void) res;
	(void) pattern;
	(void) insensitive;
	throw(MAL, "pcre.compile", "Database was compiled without PCRE support.");
#endif
}

/* these two defines are copies from gdk_select.c */

/* scan select loop with candidates */
#define candscanloop(TEST)												\
	do {																\
		ALGODEBUG fprintf(stderr,										\
						  "#BATselect(b=%s#"BUNFMT",s=%s,anti=%d): "	\
						  "scanselect %s\n", BATgetId(b), BATcount(b),	\
						  s ? BATgetId(s) : "NULL", anti, #TEST);		\
		while (p < q) {													\
			o = *candlist++;											\
			r = (BUN) (o - off);										\
			v = BUNtail(bi, r);											\
			if (TEST)													\
				bunfastapp(bn, &o);										\
			p++;														\
		}																\
	} while (0)

/* scan select loop without candidates */
#define scanloop(TEST)													\
	do {																\
		ALGODEBUG fprintf(stderr,										\
						  "#BATselect(b=%s#"BUNFMT",s=%s,anti=%d): "	\
						  "scanselect %s\n", BATgetId(b), BATcount(b),	\
						  s ? BATgetId(s) : "NULL", anti, #TEST);		\
		while (p < q) {													\
			v = BUNtail(bi, p-off);										\
			if (TEST) {													\
				o = (oid) p;											\
				bunfastapp(bn, &o);										\
			}															\
			p++;														\
		}																\
	} while (0)

static str
pcre_likeselect(BAT **bnp, BAT *b, BAT *s, const char *pat, int caseignore, int anti)
{
#ifdef HAVE_LIBPCRE
	int options = PCRE_UTF8 | PCRE_MULTILINE | PCRE_DOTALL;
	pcre *re;
	pcre_extra *pe;
	const char *error;
	int errpos;
	int ovector[10];
#else
	int options = REG_NEWLINE | REG_NOSUB;
	pcre re;
	int errcode;
#endif
	BATiter bi = bat_iterator(b);
	BAT *bn;
	BUN p, q;
	oid o, off;
	const char *v;

	assert(ATOMstorage(b->ttype) == TYPE_str);
	assert(anti == 0 || anti == 1);

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
	pe = pcre_study(re, 0, &error);
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
	bn = COLnew(0, TYPE_oid, s ? BATcount(s) : BATcount(b), TRANSIENT);
	if (bn == NULL) {
#ifdef HAVE_LIBPCRE
		pcre_free_study(pe);
		pcre_free(re);
#else
		regfree(&re);
#endif
		throw(MAL, "pcre.likeselect", MAL_MALLOC_FAIL);
	}
	off = b->hseqbase;

	if (s && !BATtdense(s)) {
		const oid *candlist;
		BUN r;

		assert(s->ttype == TYPE_oid || s->ttype == TYPE_void);
		assert(s->tsorted);
		assert(s->tkey);
		/* setup candscanloop loop vars to only iterate over
		 * part of s that has values that are in range of b */
		o = b->hseqbase + BATcount(b);
		q = SORTfndfirst(s, &o);
		p = SORTfndfirst(s, &b->hseqbase);
		candlist = (const oid *) Tloc(s, p);
#ifdef HAVE_LIBPCRE
#define BODY     (pcre_exec(re, pe, v, (int) strlen(v), 0, 0, ovector, 10) >= 0)
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
	bn->tsorted = 1;
	bn->trevsorted = bn->batCount <= 1;
	bn->tkey = 1;
	bn->tdense = bn->batCount <= 1;
	if (bn->batCount == 1)
		bn->tseqbase =  * (oid *) Tloc(bn, 0);
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
re_likeselect(BAT **bnp, BAT *b, BAT *s, const char *pat, int caseignore, int anti, int use_strcmp)
{
	BATiter bi = bat_iterator(b);
	BAT *bn;
	BUN p, q;
	oid o, off;
	const char *v;
	int nr;
	RE *re = NULL;

	assert(ATOMstorage(b->ttype) == TYPE_str);
	assert(anti == 0 || anti == 1);

	bn = COLnew(0, TYPE_oid, s ? BATcount(s) : BATcount(b), TRANSIENT);
	if (bn == NULL)
		throw(MAL, "pcre.likeselect", MAL_MALLOC_FAIL);
	off = b->hseqbase;

	if (!use_strcmp) {
		nr = re_simple(pat);
		re = re_create(pat, nr);
		if (!re)
			throw(MAL, "pcre.likeselect", MAL_MALLOC_FAIL);
	}
	if (s && !BATtdense(s)) {
		const oid *candlist;
		BUN r;

		assert(s->ttype == TYPE_oid || s->ttype == TYPE_void);
		assert(s->tsorted);
		assert(s->tkey);
		/* setup candscanloop loop vars to only iterate over
		 * part of s that has values that are in range of b */
		o = b->hseqbase + BATcount(b);
		q = SORTfndfirst(s, &o);
		p = SORTfndfirst(s, &b->hseqbase);
		candlist = (const oid *) Tloc(s, p);
		if (use_strcmp) {
			if (caseignore) {
				if (anti)
					candscanloop(v && *v != '\200' &&
								 strcasecmp(v, pat) != 0);
				else
					candscanloop(v && *v != '\200' &&
								 strcasecmp(v, pat) == 0);
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
				if (anti)
					scanloop(v && *v != '\200' &&
							 strcasecmp(v, pat) != 0);
				else
					scanloop(v && *v != '\200' &&
							 strcasecmp(v, pat) == 0);
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
	bn->tsorted = 1;
	bn->trevsorted = bn->batCount <= 1;
	bn->tkey = 1;
	bn->tdense = bn->batCount <= 1;
	if (bn->batCount == 1)
		bn->tseqbase =  * (oid *) Tloc(bn, 0);
	*bnp = bn;
	re_destroy(re);
	return MAL_SUCCEED;

  bunins_failed:
	re_destroy(re);
	BBPreclaim(bn);
	*bnp = NULL;
	throw(MAL, "pcre.likeselect", OPERATION_FAILED);
}

#define MAX_NR_CAPTURES  1024 /* Maximal number of captured substrings in one original string */

static str
pcre_replace(str *res, const char *origin_str, const char *pattern, const char *replacement, const char *flags)
{
#ifdef HAVE_LIBPCRE
	const char *err_p = NULL;
	pcre *pcre_code = NULL;
	pcre_extra *extra;
	char *tmpres;
	int i, j, k, len, errpos = 0, offset = 0;
	int compile_options = PCRE_UTF8, exec_options = PCRE_NOTEMPTY;
	int *ovector, ovecsize;
	int len_origin_str = (int) strlen(origin_str);
	int len_replacement = (int) strlen(replacement);
	int capture_offsets[MAX_NR_CAPTURES * 2], ncaptures = 0, len_del = 0;

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
			throw(MAL, "pcre.replace", ILLEGAL_ARGUMENT ": unsupported flag character '%c'\n", *flags);
		}
		flags++;
	}

	if ((pcre_code = pcre_compile(pattern, compile_options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, "pcre.replace", OPERATION_FAILED ": pcre compile of pattern (%s) failed at %d with\n'%s'.\n", pattern, errpos, err_p);
	}

	/* Since the compiled pattern is going to be used several times, it is
	 * worth spending more time analyzing it in order to speed up the time
	 * taken for matching.
	 */
	extra = pcre_study(pcre_code, 0, &err_p);
	if (err_p != NULL) {
		pcre_free(pcre_code);
		throw(MAL, "pcre.replace", OPERATION_FAILED ": pcre study of pattern (%s) failed with '%s'.\n", pattern, err_p);
	}
	pcre_fullinfo(pcre_code, extra, PCRE_INFO_CAPTURECOUNT, &i);
	ovecsize = (i + 1) * 3;
	if ((ovector = (int *) GDKmalloc(sizeof(int) * ovecsize)) == NULL) {
		pcre_free_study(extra);
		pcre_free(pcre_code);
		throw(MAL, "pcre.replace", MAL_MALLOC_FAIL);
	}

	i = 0;
	do {
		j = pcre_exec(pcre_code, extra, origin_str, len_origin_str,
					  offset, exec_options, ovector, ovecsize);
		if (j > 0) {
			capture_offsets[i] = ovector[0];
			capture_offsets[i+1] = ovector[1];
			ncaptures++;
			i += 2;
			len_del += (ovector[1] - ovector[0]);
			offset = ovector[1];
		}
	} while (j > 0 && offset < len_origin_str && ncaptures < MAX_NR_CAPTURES);
	pcre_free_study(extra);

	if (ncaptures > 0) {
		tmpres = GDKmalloc(len_origin_str - len_del + (len_replacement * ncaptures) + 1);
		if (tmpres) {
			j = k = 0;

			/* possibly copy the substring before the first captured
			 * substring */
			strncpy(tmpres, origin_str, capture_offsets[j]);
			k = capture_offsets[j];
			j++;

			for (i = 0; i < ncaptures - 1; i++) {
				strncpy(tmpres+k, replacement, len_replacement);
				k += len_replacement;
				/* copy the substring between two captured substrings */
				len = capture_offsets[j+1] - capture_offsets[j];
				strncpy(tmpres+k, origin_str+capture_offsets[j], len);
				k += len;
				j += 2;
			}

			/* replace the last captured substring */
			strncpy(tmpres+k, replacement, len_replacement);
			k += len_replacement;
			/* possibly copy the substring after the last captured substring */
			len = len_origin_str - capture_offsets[j];
			strncpy(tmpres+k, origin_str+capture_offsets[j], len);
			k += len;
			tmpres[k] = '\0';
		}
	} else {
		/* no captured substrings, return the original string */
		tmpres = GDKstrdup(origin_str);
	}

	pcre_free(pcre_code);
	GDKfree(ovector);
	if (tmpres == NULL)
		throw(MAL, "pcre.replace", MAL_MALLOC_FAIL);
	*res = tmpres;
	return MAL_SUCCEED;
#else
	(void) res;
	(void) origin_str;
	(void) pattern;
	(void) replacement;
	(void) flags;
	throw(MAL, "pcre.replace", "Database was compiled without PCRE support.");
#endif
}

static str
pcre_replace_bat(BAT **res, BAT *origin_strs, const char *pattern, const char *replacement, const char *flags)
{
#ifdef HAVE_LIBPCRE
	BATiter origin_strsi = bat_iterator(origin_strs);
	const char *err_p = NULL;
	int i, j, k, len, errpos = 0, offset = 0;
	int compile_options = PCRE_UTF8, exec_options = PCRE_NOTEMPTY;
	pcre *pcre_code = NULL;
	pcre_extra *extra;
	BAT *tmpbat;
	BUN p, q;
	int *ovector, ovecsize;
	int len_origin_str, len_replacement = (int) strlen(replacement);
	int capture_offsets[MAX_NR_CAPTURES * 2], ncaptures = 0, len_del = 0;
	const char *origin_str;
	char *replaced_str;

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
			throw(MAL, "batpcre.replace", ILLEGAL_ARGUMENT ": unsupported flag character '%c'\n", *flags);
		}
		flags++;
	}

	if ((pcre_code = pcre_compile(pattern, compile_options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, "batpcre.replace", OPERATION_FAILED
			  ": pcre compile of pattern (%s) failed at %d with\n'%s'.\n",
			  pattern, errpos, err_p);
	}

	/* Since the compiled pattern is ging to be used several times, it
	 * is worth spending more time analyzing it in order to speed up
	 * the time taken for matching.
	 */
	extra = pcre_study(pcre_code, 0, &err_p);
	if (err_p != NULL) {
		pcre_free(pcre_code);
		throw(MAL, "batpcre.replace", OPERATION_FAILED);
	}
	pcre_fullinfo(pcre_code, extra, PCRE_INFO_CAPTURECOUNT, &i);
	ovecsize = (i + 1) * 3;
	if ((ovector = (int *) GDKzalloc(sizeof(int) * ovecsize)) == NULL) {
		pcre_free_study(extra);
		pcre_free(pcre_code);
		throw(MAL, "batpcre.replace", MAL_MALLOC_FAIL);
	}

	tmpbat = COLnew(origin_strs->hseqbase, TYPE_str, BATcount(origin_strs), TRANSIENT);
	if (tmpbat == NULL) {
		pcre_free_study(extra);
		pcre_free(pcre_code);
		GDKfree(ovector);
		throw(MAL, "batpcre.replace", MAL_MALLOC_FAIL);
	}
	BATloop(origin_strs, p, q) {
		origin_str = BUNtail(origin_strsi, p);
		len_origin_str = (int) strlen(origin_str);
		i = ncaptures = len_del = offset = 0;
		do {
			j = pcre_exec(pcre_code, extra, origin_str, len_origin_str, offset,
						  exec_options, ovector, ovecsize);
			if (j > 0) {
				capture_offsets[i] = ovector[0];
				capture_offsets[i+1] = ovector[1];
				ncaptures++;
				i += 2;
				len_del += (ovector[1] - ovector[0]);
				offset = ovector[1];
			}
		} while (j > 0 && offset < len_origin_str && ncaptures < MAX_NR_CAPTURES);

		if (ncaptures > 0) {
			replaced_str = GDKmalloc(len_origin_str - len_del + (len_replacement * ncaptures) + 1);
			if (replaced_str == NULL) {
				pcre_free_study(extra);
				pcre_free(pcre_code);
				GDKfree(ovector);
				BBPreclaim(tmpbat);
				throw(MAL, "batpcre.replace", MAL_MALLOC_FAIL);
			}

			j = k = 0;

			/* copy eventually the substring before the first captured
			 * substring */
			strncpy(replaced_str, origin_str, capture_offsets[j]);
			k = capture_offsets[j];
			j++;

			for (i = 0; i < ncaptures - 1; i++) {
				strncpy(replaced_str+k, replacement, len_replacement);
				k += len_replacement;
				/* copy the substring between two captured substrings */
				len = capture_offsets[j+1] - capture_offsets[j];
				strncpy(replaced_str+k, origin_str+capture_offsets[j], len);
				k += len;
				j += 2;
			}

			/* replace the last captured substring */
			strncpy(replaced_str+k, replacement, len_replacement);
			k += len_replacement;
			/* copy eventually the substring after the last captured substring */
			len = len_origin_str - capture_offsets[j];
			strncpy(replaced_str+k, origin_str+capture_offsets[j], len);
			k += len;
			replaced_str[k] = '\0';
			if (BUNappend(tmpbat, replaced_str, FALSE) != GDK_SUCCEED) {
				pcre_free_study(extra);
				pcre_free(pcre_code);
				GDKfree(ovector);
				GDKfree(replaced_str);
				BBPreclaim(tmpbat);
				throw(MAL, "batpcre.replace", MAL_MALLOC_FAIL);
			}
			GDKfree(replaced_str);
		} else {
			/* no captured substrings, copy the original string into new bat */
			if (BUNappend(tmpbat, origin_str, FALSE) != GDK_SUCCEED) {
				pcre_free_study(extra);
				pcre_free(pcre_code);
				GDKfree(ovector);
				BBPreclaim(tmpbat);
				throw(MAL, "batpcre.replace", MAL_MALLOC_FAIL);
			}
		}
	}

	pcre_free_study(extra);
	pcre_free(pcre_code);
	GDKfree(ovector);
	*res = tmpbat;
	return MAL_SUCCEED;
#else
	(void) res;
	(void) origin_strs;
	(void) pattern;
	(void) replacement;
	(void) flags;
	throw(MAL, "batpcre.replace", "Database was compiled without PCRE support.");
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
	pcre re;
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
	if (strcmp(val, str_nil) == 0) {
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
	int esc = esc_str[0]; /* should change to utf8_convert() */
	int specials;
	int c;

	if (pat == NULL )
		throw(MAL, "pcre.sql2pcre", OPERATION_FAILED);
	ppat = GDKmalloc(strlen(pat)*2+3 /* 3 = "^'the translated regexp'$0" */);
	if (ppat == NULL)
		throw(MAL, "pcre.sql2pcre", MAL_MALLOC_FAIL);

	*r = ppat;
	/* The escape character can be a char which is special in a PCRE
	 * expression.  If the user used the "+" char as escape and has "++"
	 * in its pattern, then replacing this with "+" is not correct and
	 * should be "\+" instead. */
	specials = (*esc_str && strchr(pcre_specials, esc) != NULL);

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
			hasWildcard = 1;
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
			throw(MAL, "pcre.sql2pcre", MAL_MALLOC_FAIL);
	} else {
		*ppat++ = '$';
		*ppat = 0;
	}
	return MAL_SUCCEED;
}

/* change SQL PATINDEX pattern into PCRE pattern */
static str
pat2pcre(str *r, const char *pat)
{
	int len = (int) strlen(pat);
	char *ppat = GDKmalloc(len*2+3 /* 3 = "^'the translated regexp'$0" */);
	int start = 0;

	if (ppat == NULL)
		throw(MAL, "pcre.sql2pcre", MAL_MALLOC_FAIL);
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
/*
 * @+ Wrapping
 */
#include "mal.h"
str
PCREreplace_wrap(str *res, const str *or, const str *pat, const str *repl, const str *flags) {
	return pcre_replace(res, *or, *pat, *repl, *flags);
}

str
PCREreplace_bat_wrap(bat *res, const bat *bid, const str *pat, const str *repl, const str *flags) {
	BAT *b, *bn = NULL;
	str msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "pcre.replace", RUNTIME_OBJECT_MISSING);

	msg = pcre_replace_bat(&bn, b, *pat, *repl, *flags);
	if (msg == MAL_SUCCEED) {
		*res= bn->batCacheid;
		BBPkeepref(*res);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

str
PCREmatch(bit *ret, const str *val, const str *pat)
{
	return pcre_match_with_flags(ret, *val, *pat, "s");
}

str
PCREimatch(bit *ret, const str *val, const str *pat)
{
	return pcre_match_with_flags(ret, *val, *pat, "i");
}

str
PCREindex(int *res, const pcre *pattern, const str *s)
{
#ifdef HAVE_LIBPCRE
	int v[2];

	v[0] = v[1] = *res = 0;
	if (pcre_exec(pattern, NULL, *s, (int) strlen(*s), 0, 0, v, 2) >= 0) {
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
	pcre *re = NULL;
	char *ppat = NULL, *msg;

	if ((msg = pat2pcre(&ppat, *pat)) != MAL_SUCCEED)
		return msg;
	if ((msg = pcre_compile_wrap(&re, ppat, FALSE)) != MAL_SUCCEED)
		return msg;
	GDKfree(ppat);
	msg = PCREindex(ret, re, val);
	pcre_free(re);
	return msg;
}

str
PCREquote(str *ret, const str *val)
{
	char *p;
	const char *s = *val;

	*ret = p = GDKmalloc(strlen(s) * 2 + 1); /* certainly long enough */
	if (p == NULL)
		throw(MAL, "pcre.quote", MAL_MALLOC_FAIL);
	/* quote all non-alphanumeric ASCII characters (i.e. leave
	   non-ASCII and alphanumeric alone) */
	while (*s) {
		if (!((*s & 0x80) != 0 ||
		      ('a' <= *s && *s <= 'z') ||
		      ('A' <= *s && *s <= 'Z') ||
		      ('0' <= *s && *s <= '9')))
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
		if (strcmp(ppat, str_nil) == 0) {
			*ret = FALSE;
			if (*isens) {
				if (strcasecmp(*s, *pat) == 0)
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
	*ret = !r;
	return MAL_SUCCEED;
}

str
PCREnotlike2(bit *ret, const str *s, const str *pat)
{
	str tmp;
	bit r;

	rethrow("str.not_like", tmp, PCRElike2(&r, s, pat));
	*ret = !r;
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
	*ret = !r;
	return MAL_SUCCEED;
}

str
PCREnotilike2(bit *ret, const str *s, const str *pat)
{
	str tmp;
	bit r;

	rethrow("str.not_ilike", tmp, PCREilike2(&r, s, pat));
	*ret = !r;
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
			throw(MAL, "pcre.like3", MAL_MALLOC_FAIL);
		}
		br = (bit*)Tloc(r, 0);
		strsi = bat_iterator(strs);

		if (strcmp(ppat, str_nil) == 0) {
			BATloop(strs, p, q) {
				const char *s = (str)BUNtail(strsi, p);

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
			pcre re;
			int options = REG_NEWLINE | REG_NOSUB;
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
				const char *s = (str)BUNtail(strsi, p);

				if (*s == '\200') {
					br[i] = bit_nil;
					r->tnonil = 0;
					r->tnil = 1;
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
		r->tsorted = 0;
		r->trevsorted = 0;
		BATkey(r, FALSE);

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
	int use_re = 0;
	int use_strcmp = 0;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "algebra.likeselect", RUNTIME_OBJECT_MISSING);
	}
	if (sid && (*sid) != bat_nil && *sid && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "algebra.likeselect", RUNTIME_OBJECT_MISSING);
	}

	/* no escape, try if a simple list of keywords works */
	if (is_strcmpable(*pat, *esc)) {
		use_re = 1;
		use_strcmp = 1;
	} else if ((strcmp(*esc, str_nil) == 0 || strlen(*esc) == 0) &&
			   re_simple(*pat) > 0) {
		use_re = 1;
	} else {
		res = sql2pcre(&ppat, *pat, strcmp(*esc, str_nil) != 0 ? *esc : "\\");
		if (res != MAL_SUCCEED) {
			BBPunfix(b->batCacheid);
			if (s)
				BBPunfix(s->batCacheid);
			return res;
		}
		if (strcmp(ppat, str_nil) == 0) {
			GDKfree(ppat);
			ppat = NULL;
			if (*caseignore) {
				ppat = GDKmalloc(strlen(*pat) + 3);
				if (ppat == NULL) {
					BBPunfix(b->batCacheid);
					if (s)
						BBPunfix(s->batCacheid);
					throw(MAL, "algebra.likeselect", MAL_MALLOC_FAIL);
				}
				ppat[0] = '^';
				strcpy(ppat + 1, *pat);
				strcat(ppat, "$");
			}
		}
	}

	if (use_re) {
		res = re_likeselect(&bn, b, s, *pat, *caseignore, *anti, use_strcmp);
	} else if (ppat == NULL) {
		/* no pattern and no special characters: can use normal select */
		bn = BATselect(b, s, *pat, NULL, 1, 1, *anti);
		if (bn == NULL)
			res = createException(MAL, "algebra.likeselect", GDK_EXCEPTION);
		else
			res = MAL_SUCCEED;
	} else {
		res = pcre_likeselect(&bn, b, s, ppat, *caseignore, *anti);
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

#include "gdk_cand.h"

#define APPEND(b, o)	(((oid *) b->theap.base)[b->batCount++] = (o))
#define VALUE(s, x)		(s##vars + VarHeapVal(s##vals, (x), s##width))

static char *
pcrejoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr,
		 const char *esc, int caseignore)
{
	BUN lstart, lend, lcnt;
	const oid *lcand = NULL, *lcandend = NULL;
	BUN rstart, rend, rcnt;
	const oid *rcand = NULL, *rcandend = NULL;
	const char *lvals, *rvals;
	const char *lvars, *rvars;
	int lwidth, rwidth;
	const char *vl, *vr;
	const oid *p;
	oid lastl = 0;		/* last value inserted into r1 */
	BUN n, nl;
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
#else
	int pcrere = 0;
	pcre regex;
	int options =  REG_NEWLINE | REG_NOSUB;
	int errcode = -1;
#endif


	if (caseignore)
#ifdef HAVE_LIBPCRE
		pcreopt |= PCRE_CASELESS;
#else
	options |= REG_ICASE;
#endif

	ALGODEBUG fprintf(stderr, "#pcrejoin(l=%s#" BUNFMT "[%s]%s%s,"
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

	CANDINIT(l, sl, lstart, lend, lcnt, lcand, lcandend);
	CANDINIT(r, sr, rstart, rend, rcnt, rcand, rcandend);

	lvals = (const char *) Tloc(l, 0);
	rvals = (const char *) Tloc(r, 0);
	assert(r->tvarsized && r->ttype);
	lvars = l->tvheap->base;
	rvars = r->tvheap->base;
	lwidth = l->twidth;
	rwidth = r->twidth;

	r1->tkey = 1;
	r1->tsorted = 1;
	r1->trevsorted = 1;
	r2->tkey = 1;
	r2->tsorted = 1;
	r2->trevsorted = 1;

	/* nested loop implementation for PCRE join */
	for (;;) {
		int nr;

		if (rcand) {
			if (rcand == rcandend)
				break;
			ro = *rcand++;
			vr = VALUE(r, ro - r->hseqbase);
		} else {
			if (rstart == rend)
				break;
			vr = VALUE(r, rstart);
			ro = rstart++ + r->hseqbase;
		}
		if (strcmp(vr, str_nil) == 0)
			continue;
		if (*esc == 0 && (nr = re_simple(vr)) > 0) {
			re = re_create(vr, nr);
			if (re == NULL) {
				msg = createException(MAL, "pcre.join", MAL_MALLOC_FAIL);
				goto bailout;
			}
		} else {
			assert(pcrepat == NULL);
			msg = sql2pcre(&pcrepat, vr, esc);
			if (msg != MAL_SUCCEED)
				goto bailout;
			if (strcmp(pcrepat, str_nil) == 0) {
				GDKfree(pcrepat);
				if (caseignore) {
					pcrepat = GDKmalloc(strlen(vr) + 3);
					if (pcrepat == NULL) {
						msg = createException(MAL, "pcre.join", MAL_MALLOC_FAIL);
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
				pcreex = pcre_study(pcrere, 0, &err_p);
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
		p = lcand;
		n = lstart;
		for (;;) {
			if (lcand) {
				if (p == lcandend)
					break;
				lo = *p++;
				vl = VALUE(l, lo - l->hseqbase);
			} else {
				if (n == lend)
					break;
				vl = VALUE(l, n);
				lo = n++ + l->hseqbase;
			}
			if (strcmp(vl, str_nil) == 0)
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
					msg = createException(MAL, "pcre.join", MAL_MALLOC_FAIL);
					goto bailout;
				}
				assert(BATcapacity(r1) == BATcapacity(r2));
			}
			if (BATcount(r1) > 0) {
				if (lastl + 1 != lo)
					r1->tdense = 0;
				if (nl == 0) {
					r2->trevsorted = 0;
					if (lastl > lo) {
						r1->tsorted = 0;
						r1->tkey = 0;
					} else if (lastl < lo) {
						r1->trevsorted = 0;
					} else {
						r1->tkey = 0;
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
			r2->tkey = 0;
			r2->tdense = 0;
			r1->trevsorted = 0;
		} else if (nl == 0) {
			rskipped = BATcount(r2) > 0;
		} else if (rskipped) {
			r2->tdense = 0;
		}
	}
	assert(BATcount(r1) == BATcount(r2));
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	BATsetcount(r2, BATcount(r2));
	if (BATcount(r1) > 0) {
		if (r1->tdense)
			r1->tseqbase = ((oid *) r1->theap.base)[0];
		if (r2->tdense)
			r2->tseqbase = ((oid *) r2->theap.base)[0];
	}
	ALGODEBUG fprintf(stderr, "#pcrejoin(l=%s,r=%s)=(%s#"BUNFMT"%s%s,%s#"BUNFMT"%s%s\n",
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
		 const char *esc, int caseignore)
{
	BAT *left = NULL, *right = NULL, *candleft = NULL, *candright = NULL;
	BAT *result1 = NULL, *result2 = NULL;
	char *msg = MAL_SUCCEED;

	if ((left = BATdescriptor(lid)) == NULL)
		goto fail;
	if ((right = BATdescriptor(rid)) == NULL)
		goto fail;
	if (slid != bat_nil && (candleft = BATdescriptor(slid)) == NULL)
		goto fail;
	if (srid != bat_nil && (candright = BATdescriptor(srid)) == NULL)
		goto fail;
	result1 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	result2 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	if (result1 == NULL || result2 == NULL) {
		msg = createException(MAL, "pcre.join", MAL_MALLOC_FAIL);
		goto fail;
	}
	result1->tnil = 0;
	result1->tnonil = 1;
	result1->tkey = 1;
	result1->tsorted = 1;
	result1->trevsorted = 1;
	result1->tdense = 1;
	result2->tnil = 0;
	result2->tnonil = 1;
	result2->tkey = 1;
	result2->tsorted = 1;
	result2->trevsorted = 1;
	result2->tdense = 1;
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
	throw(MAL, "pcre.join", RUNTIME_OBJECT_MISSING);
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
