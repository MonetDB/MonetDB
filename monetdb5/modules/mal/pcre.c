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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f pcre
 * @a N. Nes
 * @+ PCRE library interface
 * The  PCRE library is a set of functions that implement regular
 * expression pattern matching using the same syntax  and  semantics  as  Perl,
 * with  just  a  few  differences.  The  current  implementation of PCRE
 * (release 4.x) corresponds approximately with Perl 5.8, including  support
 * for  UTF-8  encoded  strings.   However,  this support has to be
 * explicitly enabled; it is not the default.
 */
/*
 * @-
 * @verbatim
 * ftp://ftp.csx.cam.ac.uk/pub/software/programming/pcre
 * @end verbatim
 *
 * @+ Implementation
 * @include prelude.mx
 */
#include "monetdb_config.h"
#include <string.h>

#include "mal.h"
#include "mal_exception.h"


#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define pcre_export extern __declspec(dllimport)
#else
#define pcre_export extern __declspec(dllexport)
#endif
#else
#define pcre_export extern
#endif

#include <pcre.h>

pcre_export str PCREquote(str *r, str *v);
pcre_export str PCREselect(int *res, str *pattern, int *bid, bit *ignore);
pcre_export str PCREuselect(int *res, str *pattern, int *bid, bit *ignore);
pcre_export str PCREmatch(bit *ret, str *val, str *pat);
pcre_export str PCREimatch(bit *ret, str *val, str *pat);
pcre_export str PCREindex(int *ret, pcre *pat, str *val);
pcre_export str PCREpatindex(int *ret, str *pat, str *val);
pcre_export str PCREfromstr(str instr, int *l, pcre ** val);

pcre_export str PCREreplace_wrap(str *res, str *or, str *pat, str *repl, str *flags);
pcre_export str PCREreplace_bat_wrap(int *res, int *or, str *pat, str *repl, str *flags);

pcre_export str PCREcompile_wrap(pcre ** res, str *pattern);
pcre_export str PCREexec_wrap(bit *res, pcre * pattern, str *s);
pcre_export int pcre_tostr(str *tostr, int *l, pcre * p);
pcre_export int pcre_fromstr(str instr, int *l, pcre ** val);
pcre_export int pcre_nequal(pcre * l, pcre * r);
pcre_export BUN pcre_hash(pcre * b);
pcre_export pcre * pcre_null(void);
pcre_export void pcre_del(Heap *h, var_t *index);
pcre_export int pcre_length(pcre * p);
pcre_export void pcre_heap(Heap *heap, size_t capacity);
pcre_export var_t pcre_put(Heap *h, var_t *bun, pcre * val);
pcre_export str PCREsql2pcre(str *ret, str *pat, str *esc);
pcre_export str PCRElike3(bit *ret, str *s, str *pat, str *esc);
pcre_export str PCRElike2(bit *ret, str *s, str *pat);
pcre_export str PCREnotlike3(bit *ret, str *s, str *pat, str *esc);
pcre_export str PCREnotlike2(bit *ret, str *s, str *pat);
pcre_export str BATPCRElike(int *ret, int *b, str *pat, str *esc);
pcre_export str BATPCRElike2(int *ret, int *b, str *pat);
pcre_export str BATPCREnotlike(int *ret, int *b, str *pat, str *esc);
pcre_export str BATPCREnotlike2(int *ret, int *b, str *pat);
pcre_export str PCREilike3(bit *ret, str *s, str *pat, str *esc);
pcre_export str PCREilike2(bit *ret, str *s, str *pat);
pcre_export str PCREnotilike3(bit *ret, str *s, str *pat, str *esc);
pcre_export str PCREnotilike2(bit *ret, str *s, str *pat);
pcre_export str BATPCREilike(int *ret, int *b, str *pat, str *esc);
pcre_export str BATPCREilike2(int *ret, int *b, str *pat);
pcre_export str BATPCREnotilike(int *ret, int *b, str *pat, str *esc);
pcre_export str BATPCREnotilike2(int *ret, int *b, str *pat);
pcre_export str PCREselectDef(int *res, str *pattern, int *bid);
pcre_export str PCREuselectDef(int *res, str *pattern, int *bid);
pcre_export str PCRElike_uselect_pcre(int *ret, int *b, str *pat, str *esc);
pcre_export str PCREilike_uselect_pcre(int *ret, int *b, str *pat, str *esc);
pcre_export str PCRElike_select_pcre(int *ret, int *b, str *pat, str *esc);
pcre_export str PCREilike_select_pcre(int *ret, int *b, str *pat, str *esc);
pcre_export str pcre_init(void);

/* current implementation assumes simple %keyword% [keyw%]* */
typedef struct RE {
	char *k;
	int search;
	int skip;
	int len;
	struct RE *n;
} RE;

#ifndef HAVE_STRCASESTR
static char *
strcasestr (char *haystack, char *needle)
{
	char *p, *startn = 0, *np = 0;

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
re_match_ignore(str s, RE *pattern)
{
	RE *r;

	for(r = pattern; r; r = r->n) {
		if (!*s ||
			(!r->search && strncasecmp(s, r->k, r->len) != 0) ||
			(r->search && (s = strcasestr(s, r->k)) == NULL))
			return 0;
		s += r->len;
	}
	return 1;
}

static int
re_match_no_ignore(str s, RE *pattern)
{
	RE *r;

	for(r = pattern; r; r = r->n) {
		if (!*s ||
			(!r->search && strncmp(s, r->k, r->len) != 0) ||
			(r->search && (s = strstr(s, r->k)) == NULL))
			return 0;
		s += r->len;
	}
	return 1;
}

static RE *
re_create( char *pat, int nr)
{
	char *x = GDKstrdup(pat);
	RE *r = (RE*)GDKmalloc(sizeof(RE)), *n = r;
	char *p = x, *q = x;

	if (x == NULL || r == NULL) {
		if (x != NULL)
			GDKfree(x);
		if (r != NULL)
			GDKfree(r);
		return NULL;
	}
	r->n = NULL;
	r->search = 0;
	r->skip = 0;

	if (*p == '%') {
		p++; /* skip % */
		r->search = 1;
	}
	q = p;
	while((q = strchr(p, '%')) != NULL) {
		*q = 0;
		n->k = GDKstrdup(p);
		n->len = (int) strlen(n->k);
		n->n = NULL;
		if ((--nr) > 0) {
			n = n->n = (RE*)GDKmalloc(sizeof(RE));
			if ( n == NULL){
				GDKfree(x);
				GDKfree(r);
				return NULL;
			}
			n->search = 1;
			n->skip = 0;
		}
		p = q+1;
	}
	GDKfree(x);
	return r;
}

static void
re_destroy( RE *p)
{
	while(p) {
		RE *n = p->n;

		GDKfree(p->k);
		GDKfree(p);
		p = n;
	}
}

static BAT *
re_uselect(RE *pattern, BAT *strs, int ignore)
{
	BATiter strsi = bat_iterator(strs);
	BAT *r;
	BUN p, q;

	if (strs->htype == TYPE_void)
		r = BATnew(TYPE_oid, TYPE_void, BATcount(strs));
	else
		r = BATnew(strs->htype, TYPE_void, BATcount(strs));

	BATaccessBegin(strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	if (ignore) {
		BATloop(strs, p, q) {
			str s = BUNtail(strsi, p);

			if (re_match_ignore(s, pattern))
				BUNfastins(r, BUNhead(strsi, p), NULL);
		}
	} else {
		BATloop(strs, p, q) {
			str s = BUNtail(strsi, p);

			if (re_match_no_ignore(s, pattern))
				BUNfastins(r, BUNhead(strsi, p), NULL);
		}
	}
	BATaccessEnd(strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	r->H->nonil = strs->H->nonil;
	r->hsorted = strs->hsorted;
	BATkey(r, BAThkey(strs));
	r->T->nonil = FALSE;
	r->tsorted = FALSE;

	if (!(r->batDirty&2)) r = BATsetaccess(r, BAT_READ);
	return r;
}

static BAT *
re_select(RE *pattern, BAT *strs, int ignore)
{
	BATiter strsi = bat_iterator(strs);
	BAT *r;
	BUN p, q;

	if (strs->htype == TYPE_void)
		r = BATnew(TYPE_oid, TYPE_str, BATcount(strs));
	else
		r = BATnew(strs->htype, TYPE_str, BATcount(strs));

	BATaccessBegin(strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	if (ignore) {
		BATloop(strs, p, q) {
			str s = BUNtail(strsi, p);

			if (re_match_ignore(s, pattern))
				BUNins(r, BUNhead(strsi, p), s, FALSE);
		}
	} else {
		BATloop(strs, p, q) {
			str s = BUNtail(strsi, p);

			if (re_match_no_ignore(s, pattern))
				BUNins(r, BUNhead(strsi, p), s, FALSE);
		}
	}
	BATaccessEnd(strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	r->H->nonil = strs->H->nonil;
	r->hsorted = strs->hsorted;
/*	BATkey(r, BAThkey(strs)); ?*/
	r->T->nonil = strs->T->nonil;
	r->tsorted = strs->tsorted;

	if (!(r->batDirty&2)) r = BATsetaccess(r, BAT_READ);
	return r;
}

#define m2p(p) (pcre*)(((size_t*)p)+1)
#define p2m(p) (pcre*)(((size_t*)p)-1)

static void *
my_pcre_malloc(size_t s)
{
	size_t *sz = (size_t *) GDKmalloc(s + sizeof(size_t));

	if ( sz == NULL)
		return NULL;
	*sz = s + sizeof(size_t);
	return (void *) (sz + 1);
}

static void
my_pcre_free(void *blk)
{
	size_t *sz;

	if (blk == NULL)
		return;
	sz = (size_t *) blk;
	sz -= 1;
	GDKfree(sz);
}

static str
pcre_compile_wrap(pcre ** res, str pattern, bit insensitive)
{
	pcre *r;
	const char err[BUFSIZ], *err_p = err;
	int errpos = 0;
	int options = PCRE_UTF8 | PCRE_MULTILINE;
	if (insensitive)
		options |= PCRE_CASELESS;

	if ((r = pcre_compile(pattern, options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL,"pcre.compile", OPERATION_FAILED
			" with\n'%s'\nat %d in\n'%s'.\n",
				err_p, errpos, pattern);
	}
	*(pcre **) res = p2m(r);
	return MAL_SUCCEED;
}

static str
pcre_exec_wrap(bit *res, pcre * pattern, str s)
{
	if (pcre_exec(m2p(pattern), NULL, s, (int) strlen(s), 0, 0, NULL, 0) >= 0) {
		*res = TRUE;
		return MAL_SUCCEED;
	}
	*res = FALSE;
	throw(MAL, "pcre.exec", OPERATION_FAILED);
}

static str
pcre_index(int *res, pcre * pattern, str s)
{
	int v[2];

	v[0] = v[1] = *res = 0;
	if (pcre_exec(m2p(pattern), NULL, s, (int) strlen(s), 0, 0, v, 2) >= 0) {
		*res = v[1];
	}
	return MAL_SUCCEED;
}

static str
pcre_select(BAT **res, str pattern, BAT *strs, bit insensitive)
{
	BATiter strsi = bat_iterator(strs);
	const char err[BUFSIZ], *err_p = err;
	int errpos = 0;
	BAT *r;
	BUN p, q;
	pcre *re = NULL;
	int options = PCRE_UTF8 | PCRE_MULTILINE;
	if (insensitive)
		options |= PCRE_CASELESS;

	if (strs->htype == TYPE_void)
		r = BATnew(TYPE_oid, TYPE_str, BATcount(strs));
	else
		r = BATnew(strs->htype, TYPE_str, BATcount(strs));
	if ((re = pcre_compile(pattern, options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, "pcre_select", OPERATION_FAILED "pcre compile of pattern (%s) failed at %d with\n'%s'.",
			pattern, errpos, err_p);
	}
	BATaccessBegin(strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	BATloop(strs, p, q) {
		str s = BUNtail(strsi, p);

		if (pcre_exec(re, NULL, s, (int) strlen(s), 0, 0, NULL, 0) >= 0) {
			BUNins(r, BUNhead(strsi, p), s, FALSE);
		}
	}
	BATaccessEnd(strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	if (!(r->batDirty&2)) r = BATsetaccess(r, BAT_READ);
	my_pcre_free(re);
	*res = r;
	return MAL_SUCCEED;
}

static str
pcre_uselect(BAT **res, str pattern, BAT *strs, bit insensitive)
{
	BATiter strsi = bat_iterator(strs);
	const char err[BUFSIZ], *err_p = err;
	int errpos = 0;
	BAT *r;
	BUN p, q;
	pcre *re = NULL;
	pcre_extra *pe = NULL;
	int options = PCRE_UTF8 | PCRE_MULTILINE;
	if (insensitive)
		options |= PCRE_CASELESS;

	if (strs->htype == TYPE_void)
		r = BATnew(TYPE_oid, TYPE_void, BATcount(strs));
	else
		r = BATnew(strs->htype, TYPE_void, BATcount(strs));
	if ((re = pcre_compile(pattern, options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, "pcre_uselect", OPERATION_FAILED "pcre compile of pattern (%s) failed at %d with\n'%s'.",
			pattern, errpos, err_p);
	}
	err_p = NULL;
	pe = pcre_study( re, 0, &err_p);
	if (err_p)
		throw(MAL, "pcre_uselect", OPERATION_FAILED "pcre compile of pattern (%s) failed with\n'%s'.", pattern, err_p);

	BATaccessBegin(strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	BATloop(strs, p, q) {
		str s = BUNtail(strsi, p);
		int l = (int) strlen(s);

		if (pcre_exec(re, pe, s, l, 0, 0, NULL, 0) >= 0) {
			BUNfastins(r, BUNhead(strsi, p), NULL);
		}
	}
	BATaccessEnd(strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	r->H->nonil = strs->H->nonil;
	r->hsorted = strs->hsorted;
	BATkey(r, BAThkey(strs));
	r->T->nonil = FALSE;
	r->tsorted = FALSE;

	my_pcre_free(re);
	my_pcre_free(pe);
	if (!(r->batDirty&2)) r = BATsetaccess(r, BAT_READ);
	*res = r;
	return MAL_SUCCEED;
}

#define MAX_NR_CAPTURES  1024 /* Maximal number of captured substrings in one original string */

static str
pcre_replace(str *res, str origin_str, str pattern, str replacement, str flags)
{
	const char err[BUFSIZ], *err_p = err, *err_p2 = err;
	pcre *pcre_code = NULL;
	pcre_extra *extra;
	str tmpres;
	int i, j, k, len, errpos = 0, offset = 0;
	int compile_options = PCRE_UTF8, exec_options = PCRE_NOTEMPTY;
	int *ovector, ovecsize;
	int len_origin_str = (int) strlen(origin_str);
	int len_replacement = (int) strlen(replacement);
	int capture_offsets[MAX_NR_CAPTURES * 2], ncaptures = 0, len_del = 0;

	for (i = 0; i < (int)strlen(flags); i++) {
		if (flags[i] == 'e') {
			exec_options -= PCRE_NOTEMPTY;
/*
			mnstr_printf(GDKout, "exec_options %d, PCRE_NOTEMPTY %d\n",
					exec_options, PCRE_NOTEMPTY);
*/
		} else if (flags[i] == 'i') {
			compile_options |= PCRE_CASELESS;
		} else if (flags[i] == 'm') {
			compile_options |= PCRE_MULTILINE;
		} else if (flags[i] == 's') {
			compile_options |= PCRE_DOTALL;
		} else if (flags[i] == 'x') {
			compile_options |= PCRE_EXTENDED;
		} else {
			throw(MAL,"pcre_replace",OPERATION_FAILED "unsupported flag character '%c'\n", flags[i]);
		}
	}

	if ((pcre_code = pcre_compile(pattern, compile_options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL,"pcre_replace",OPERATION_FAILED "pcre compile of pattern (%s) failed at %d with\n'%s'.\n", pattern, errpos, err_p);
	}

	/* Since the compiled pattern is going to be used several times, it is
	 * worth spending more time analyzing it in order to speed up the time
	 * taken for matching.
	 */
	extra = pcre_study(pcre_code, 0, &err_p2);
	pcre_fullinfo(pcre_code, extra, PCRE_INFO_CAPTURECOUNT, &i);
	ovecsize = (i + 1) * 3;
	if ((ovector = (int *) GDKmalloc(sizeof(int) * ovecsize)) == NULL) {
		my_pcre_free(pcre_code);
		throw(MAL, "pcre_replace",MAL_MALLOC_FAIL);
	}

	i = 0;
	do {
		j = pcre_exec(pcre_code, extra, origin_str, len_origin_str,
						offset, exec_options, ovector, ovecsize);
		if (j > 0){
			capture_offsets[i] = ovector[0];
			capture_offsets[i+1] = ovector[1];
			ncaptures++;
			i += 2;
			len_del += (ovector[1] - ovector[0]);
			offset = ovector[1];
		}
	} while((j > 0) && (offset < len_origin_str) && (ncaptures < MAX_NR_CAPTURES));

	if (ncaptures > 0){
		tmpres = GDKmalloc(len_origin_str - len_del + (len_replacement * ncaptures) + 1);
		if (!tmpres) {
			my_pcre_free(pcre_code);
			GDKfree(ovector);
			throw(MAL, "pcre_replace", MAL_MALLOC_FAIL);
		}

		j = k = 0;

		/* possibly copy the substring before the first captured substring */
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
	} else { /* no captured substrings, return the original string*/
		tmpres = GDKstrdup(origin_str);
	}

	my_pcre_free(pcre_code);
	GDKfree(ovector);
	*res = tmpres;
	return MAL_SUCCEED;
}

static str
pcre_replace_bat(BAT **res, BAT *origin_strs, str pattern, str replacement, str flags)
{
	BATiter origin_strsi = bat_iterator(origin_strs);
	const char err[BUFSIZ], *err_p = err, *err_p2 = err;
	int i, j, k, len, errpos = 0, offset = 0;
	int compile_options = PCRE_UTF8, exec_options = PCRE_NOTEMPTY;
	pcre *pcre_code = NULL;
	pcre_extra *extra;
	BAT *tmpbat;
	BUN p, q;
	int *ovector, ovecsize;
	int len_origin_str, len_replacement = (int) strlen(replacement);
	int capture_offsets[MAX_NR_CAPTURES * 2], ncaptures = 0, len_del = 0;
	str origin_str, replaced_str;

	for (i = 0; i < (int)strlen(flags); i++) {
		if (flags[i] == 'e') {
			exec_options |= (~PCRE_NOTEMPTY);
		} else if (flags[i] == 'i') {
			compile_options |= PCRE_CASELESS;
		} else if (flags[i] == 'm') {
			compile_options |= PCRE_MULTILINE;
		} else if (flags[i] == 's') {
			compile_options |= PCRE_DOTALL;
		} else if (flags[i] == 'x') {
			compile_options |= PCRE_EXTENDED;
		} else {
			throw(MAL,"pcre_replace_bat", ILLEGAL_ARGUMENT
				" \"flags\" contains invalid character '%c'\n", flags[i]);
		}
	}

	if ((pcre_code = pcre_compile(pattern, compile_options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL,"pcre_replace_bat", OPERATION_FAILED
			"pcre compile of pattern (%s) failed at %d with\n'%s'.\n", pattern, errpos, err_p);
	}

	/* Since the compiled pattern is ging to be used several times, it is worth spending
	 * more time analyzing it in order to speed up the time taken for matching.
	 */
	extra = pcre_study(pcre_code, 0, &err_p2);
	pcre_fullinfo(pcre_code, extra, PCRE_INFO_CAPTURECOUNT, &i);
	ovecsize = (i + 1) * 3;
	if ((ovector = (int *) GDKzalloc(sizeof(int) * ovecsize)) == NULL) {
		my_pcre_free(pcre_code);
		throw(MAL, "pcre_replace_bat", MAL_MALLOC_FAIL);
	}

	tmpbat = BATnew(origin_strs->htype, TYPE_str, BATcount(origin_strs));
	BATaccessBegin(origin_strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);
	BATloop(origin_strs, p, q) {
		origin_str = BUNtail(origin_strsi, p);
		len_origin_str = (int) strlen(origin_str);
		i = ncaptures = len_del = offset = 0;
		do {
			j = pcre_exec(pcre_code, extra, origin_str, len_origin_str, offset,
					exec_options, ovector, ovecsize);
			if (j > 0){
				capture_offsets[i] = ovector[0];
				capture_offsets[i+1] = ovector[1];
				ncaptures++;
				i += 2;
				len_del += (ovector[1] - ovector[0]);
				offset = ovector[1];
			}
		} while((j > 0) && (offset < len_origin_str) && (ncaptures < MAX_NR_CAPTURES));

		if (ncaptures > 0){
			replaced_str = GDKmalloc(len_origin_str - len_del + (len_replacement * ncaptures) + 1);
			if (!replaced_str) {
				my_pcre_free(pcre_code);
				GDKfree(ovector);
				throw(MAL, "pcre_replace_bat", MAL_MALLOC_FAIL);
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
			BUNins(tmpbat, BUNhead(origin_strsi, p), replaced_str, FALSE);
			GDKfree(replaced_str);
		} else { /* no captured substrings, copy the original string into new bat */
			BUNins(tmpbat, BUNhead(origin_strsi, p), origin_str, FALSE);
		}
	}
	BATaccessEnd(origin_strs,USE_HEAD|USE_TAIL,MMAP_SEQUENTIAL);

	my_pcre_free(pcre_code);
	GDKfree(ovector);
	if (origin_strs->htype == TYPE_void) {
		*res = BATseqbase(tmpbat, origin_strs->hseqbase);
	} else {
		*res = tmpbat;
	}
	return MAL_SUCCEED;
}

str
pcre_init(void)
{
	pcre_malloc = my_pcre_malloc;
	pcre_free = my_pcre_free;
	return NULL;
}

static str
pcre_match_with_flags(bit *ret, str val, str pat, str flags)
{
	const char err[BUFSIZ], *err_p = err;
	int errpos = 0;
	int options = PCRE_UTF8, i;
	int pos;
	pcre *re;

	for (i = 0; i < (int)strlen(flags); i++) {
		if (flags[i] == 'i') {
			options |= PCRE_CASELESS;
		} else if (flags[i] == 'm') {
			options |= PCRE_MULTILINE;
		} else if (flags[i] == 's') {
			options |= PCRE_DOTALL;
		} else if (flags[i] == 'x') {
			options |= PCRE_EXTENDED;
		} else {
			throw(MAL, "pcre.match", ILLEGAL_ARGUMENT
					": unsupported flag character '%c'\n", flags[i]);
		}
	}
	if (strcmp(val, (char*)str_nil) == 0) {
		*ret = FALSE;
		return MAL_SUCCEED;
	}

	if ((re = pcre_compile(pat, options, &err_p, &errpos, NULL)) == NULL) {
		throw(MAL, "pcre.match", OPERATION_FAILED
				": compilation of regular expression (%s) failed "
				"at %d with '%s'", pat, errpos, err_p);
	}
	pos = pcre_exec(re, NULL, val, (int) strlen(val), 0, 0, NULL, 0);
	my_pcre_free(re);
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

static int
pcre_quote(str *res, str s)
{
	str p;

	*res = p = GDKmalloc(strlen(s) * 2 + 1); /* certainly long enough */
	if (p == NULL)
		return GDK_FAIL;
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
	return GDK_SUCCEED;
}

int
pcre_tostr(str *tostr, int *l, pcre * p)
{
	(void) tostr;
	(void) l;
	(void) p;
	return GDK_FAIL;
}

int
pcre_fromstr(str instr, int *l, pcre ** val)
{
	(void) instr;
	(void) l;
	(void) val;
	return GDK_FAIL;
}

int
pcre_nequal(pcre * l, pcre * r)
{
	if (l != r)
		return 0;
	else
		return 1;
}

BUN
pcre_hash(pcre * b)
{
	return *(sht *) b;
}

pcre *
pcre_null(void)
{
	static sht nullval, *r;

	nullval = ~(sht) 0;
	r = &nullval;
	return ((pcre *) (r));
}

void
pcre_del(Heap *h, var_t *idx)
{
	HEAP_free(h, *idx);
}

#define pcresize(val) ((size_t*)val)[0]

var_t
pcre_put(Heap *h, var_t *bun, pcre * val)
{
	char *base;

	assert(pcresize(val) <= VAR_MAX);
	*bun = HEAP_malloc(h, (var_t) pcresize(val));
	base = h->base;
	if (*bun)
		memcpy(&base[*bun << GDK_VARSHIFT], (char *) val, pcresize(val));
	return *bun;
}

int
pcre_length(pcre * p)
{
	assert(pcresize(p) <= GDK_int_max);
	return (int) (pcresize(p));
}

void
pcre_heap(Heap *heap, size_t capacity)
{
	HEAP_initialize(heap, capacity, 0, (int) sizeof(var_t));
}

/* change SQL LIKE pattern into PCRE pattern */
static int
sql2pcre(str *r, str pat, str esc_str)
{
	int escaped = 0;
	int hasWildcard = 0;
	char *ppat;
	int esc = esc_str[0]; /* should change to utf8_convert() */
	int specials = 0;

	if (pat == NULL )
		return GDK_FAIL;
	ppat = GDKmalloc(strlen(pat)*2+3 /* 3 = "^'the translated regexp'$0" */);
	if (ppat == NULL)
		return GDK_FAIL;

	*r = ppat;
	/* The escape character can be a char which is special in a PCRE
	 * expression.  If the user used the "+" char as escape and has "++"
	 * in its pattern, then replacing this with "+" is not correct and
	 * should be "\+" instead. */
	if (*esc_str && strchr( ".+*()[]", esc) != NULL)
		specials = 1;

	*ppat++ = '^';
	while (*pat) {
		int c = *pat++;

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
		} else if (strchr(".?+*()[]\\", c) != NULL) {
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
		*r = GDKstrdup(str_nil);
		if (escaped)
			return GDK_FAIL;
	} else {
		*ppat++ = '$';
		*ppat = 0;
	}
	return GDK_SUCCEED;
}

/* change SQL PATINDEX pattern into PCRE pattern */
static int
pat2pcre(str *r, str pat)
{
	int len = (int) strlen(pat);
	char *ppat = GDKmalloc(len*2+3 /* 3 = "^'the translated regexp'$0" */);
	int start = 0;

	if ( ppat == NULL)
		return GDK_FAIL;
	*r = ppat;
	while (*pat) {
		int c = *pat++;

		if (strchr( ".+*()\\", c) != NULL) {
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
	return GDK_SUCCEED;
}
/*
 * @+ Wrapping
 */
#include "mal.h"
str
PCREfromstr(str instr, int *l, pcre ** val)
{
	(void) instr;
	(void) l;
	(void) val;
	return NULL;
}

str
PCREreplace_wrap(str *res, str *or, str *pat, str *repl, str *flags){
	return pcre_replace(res,*or,*pat,*repl,*flags);
}

str
PCREreplace_bat_wrap(int *res, int *bid, str *pat, str *repl, str *flags){
	BAT *b,*bn = NULL;
	str msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "pcre.replace", RUNTIME_OBJECT_MISSING);

	msg = pcre_replace_bat(&bn,b,*pat,*repl,*flags);
	if( msg == MAL_SUCCEED){
		*res= bn->batCacheid;
		BBPkeepref(*res);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

str
PCREcompile_wrap(pcre ** res, str *pattern)
{
	return pcre_compile_wrap(res, *pattern, FALSE);
}

str
PCREexec_wrap(bit *res, pcre * pattern, str *s)
{
	return pcre_exec_wrap(res, pattern, *s);
}

str
PCREselectDef(int *res, str *pattern, int *bid)
{
	bit ignore = FALSE;
	return(PCREselect(res, pattern, bid, &ignore));
}

str
PCREselect(int *res, str *pattern, int *bid, bit *ignore)
{
	BAT *bn = NULL, *strs;
	str msg;

	if ((strs = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "pcre.select", RUNTIME_OBJECT_MISSING);
	}

	if ((msg = pcre_select(&bn, *pattern, strs, *ignore)) != MAL_SUCCEED) {
		BBPunfix(strs->batCacheid);
		return msg;
	}

	*res = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPunfix(strs->batCacheid);
	return msg;
}

str
PCREuselectDef(int *res, str *pattern, int *bid)
{
	bit ignore = FALSE;
	return(PCREuselect(res, pattern, bid, &ignore));
}

str
PCREuselect(int *res, str *pattern, int *bid, bit *ignore)
{
	BAT *bn = NULL, *strs;
	str msg;

	if ((strs = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "pcre.select", RUNTIME_OBJECT_MISSING);
	}

	if ((msg = pcre_uselect(&bn, *pattern, strs, *ignore)) != MAL_SUCCEED) {
		BBPunfix(strs->batCacheid);
		return msg;
	}

	*res = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPunfix(strs->batCacheid);
	return msg;
}

str
PCREmatch(bit *ret, str *val, str *pat)
{
	char *flags = "";
	return pcre_match_with_flags(ret, *val, *pat, flags);
}

str
PCREimatch(bit *ret, str *val, str *pat)
{
	char *flags = "i";
	return pcre_match_with_flags(ret, *val, *pat, flags);
}

str
PCREindex(int *res, pcre * pattern, str *s)
{
	return pcre_index(res, pattern, *s);
}


str
PCREpatindex(int *ret, str *pat, str *val)
{
	pcre *re = NULL;
	char *ppat = NULL, *msg;

	if (pat2pcre(&ppat, *pat) <0)
		throw(MAL, "pcre.sql2pcre", OPERATION_FAILED);
	if ((msg = pcre_compile_wrap(&re, ppat, FALSE)) != NULL)
		return msg;
	GDKfree(ppat);
	msg = PCREindex(ret, re, val);
	GDKfree(re);
	return msg;
}

str
PCREquote(str *ret, str *val)
{
	if (pcre_quote(ret, *val) <0)
		throw(MAL, "pcre.quote", OPERATION_FAILED);
	return MAL_SUCCEED;
}


str
PCREsql2pcre(str *ret, str *pat, str *esc)
{
	if (sql2pcre(ret, *pat, *esc) < 0)
		throw(MAL, "pcre.sql2pcre", OPERATION_FAILED);
	return MAL_SUCCEED;
}

static str
PCRElike4(bit *ret, str *s, str *pat, str *esc, bit *isens)
{
	char *ppat = NULL;
	str r = PCREsql2pcre(&ppat, pat, esc);

	if (!r) {
		assert(ppat);
		if (strcmp(ppat, (char*)str_nil) == 0) {
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
PCRElike3(bit *ret, str *s, str *pat, str *esc)
{
	bit no = FALSE;

	return(PCRElike4(ret, s, pat, esc, &no));
}

str
PCRElike2(bit *ret, str *s, str *pat)
{
	char *esc = "\\";

	return PCRElike3(ret, s, pat, &esc);
}

str
PCREnotlike3(bit *ret, str *s, str *pat, str *esc)
{
	str tmp;
	bit r;

	rethrow("str.not_like", tmp, PCRElike3(&r, s, pat, esc));
	*ret = !r;
	return(MAL_SUCCEED);
}

str
PCREnotlike2(bit *ret, str *s, str *pat)
{
	str tmp;
	bit r;

	rethrow("str.not_like", tmp, PCRElike2(&r, s, pat));
	*ret = !r;
	return(MAL_SUCCEED);
}

str
PCREilike3(bit *ret, str *s, str *pat, str *esc)
{
	bit yes = TRUE;

	return(PCRElike4(ret, s, pat, esc, &yes));
}

str
PCREilike2(bit *ret, str *s, str *pat)
{
	char *esc = "\\";

	return PCREilike3(ret, s, pat, &esc);
}

str
PCREnotilike3(bit *ret, str *s, str *pat, str *esc)
{
	str tmp;
	bit r;

	rethrow("str.not_ilike", tmp, PCREilike3(&r, s, pat, esc));
	*ret = !r;
	return(MAL_SUCCEED);
}

str
PCREnotilike2(bit *ret, str *s, str *pat)
{
	str tmp;
	bit r;

	rethrow("str.not_ilike", tmp, PCREilike2(&r, s, pat));
	*ret = !r;
	return(MAL_SUCCEED);
}

static str
BATPCRElike3(bat *ret, int *bid, str *pat, str *esc, bit *isens, bit *not)
{
	char *ppat = NULL;
	str res = PCREsql2pcre(&ppat, pat, esc);

	if (!res) {
		BAT *strs = BATdescriptor(*bid);
		BATiter strsi;
		BAT *r;
		bit *br;
		BUN p, q, i = 0;

		if (strs == NULL)
			throw(MAL, "batstr.like", OPERATION_FAILED);

		r = BATnew(TYPE_void, TYPE_bit, BATcount(strs));
		br = (bit*)Tloc(r, BUNfirst(r));
		strsi = bat_iterator(strs);

		BATaccessBegin(strs,USE_TAIL,MMAP_SEQUENTIAL);
		if (strcmp(ppat, (char*)str_nil) == 0) {
			BATloop(strs, p, q) {
				str s = (str)BUNtail(strsi, p);

				if (strcmp(s, *pat) == 0)
					br[i] = TRUE;
				else
					br[i] = FALSE;
				if (*not)
					br[i] = !br[i];
				i++;
			}
		} else {
			if (*isens) {
				BATloop(strs, p, q) {
					str s = (str)BUNtail(strsi, p);

					res = PCREimatch(br + i, &s, &ppat);
					i++;
				}
			} else {
				BATloop(strs, p, q) {
					str s = (str)BUNtail(strsi, p);

					res = PCREmatch(br + i, &s, &ppat);
					i++;
				}
			}
			if (*not) {
				i = 0;
				BATloop(strs, p, q) {
					br[i] = !br[i];
					i++;
				}
			}
		}
		BATaccessEnd(strs,USE_TAIL,MMAP_SEQUENTIAL);
		BATsetcount(r, i);
		r->tsorted = 0;
		BATkey(BATmirror(r),FALSE);
		BATseqbase(r, strs->hseqbase);

		if (!(r->batDirty&2)) r = BATsetaccess(r, BAT_READ);

		if (strs->htype != r->htype) {
			BAT *v = VIEWcreate(strs, r);

			BBPreleaseref(r->batCacheid);
			r = v;
		}
		BBPkeepref(*ret = r->batCacheid);
		BBPreleaseref(strs->batCacheid);
	}
	if (ppat)
		GDKfree(ppat);
	return res;
}

str
BATPCRElike(bat *ret, int *bid, str *pat, str *esc)
{
	bit no = FALSE;

	return(BATPCRElike3(ret, bid, pat, esc, &no, &no));
}

str
BATPCRElike2(bat *ret, int *bid, str *pat)
{
	char *esc = "\\";

	return BATPCRElike(ret, bid, pat, &esc);
}

str
BATPCREnotlike(bat *ret, int *bid, str *pat, str *esc)
{
	bit no = FALSE;
	bit yes = TRUE;

	return(BATPCRElike3(ret, bid, pat, esc, &no, &yes));
}

str
BATPCREnotlike2(bat *ret, int *bid, str *pat)
{
	char *esc = "\\";

	return(BATPCREnotlike(ret, bid, pat, &esc));
}

str
BATPCREilike(bat *ret, int *bid, str *pat, str *esc)
{
	bit yes = TRUE;
	bit no = FALSE;

	return(BATPCRElike3(ret, bid, pat, esc, &yes, &no));
}

str
BATPCREilike2(bat *ret, int *bid, str *pat)
{
	char *esc = "\\";

	return BATPCREilike(ret, bid, pat, &esc);
}

str
BATPCREnotilike(bat *ret, int *bid, str *pat, str *esc)
{
	bit yes = TRUE;

	return(BATPCRElike3(ret, bid, pat, esc, &yes, &yes));
}

str
BATPCREnotilike2(bat *ret, int *bid, str *pat)
{
	char *esc = "\\";

	return(BATPCREnotilike(ret, bid, pat, &esc));
}

static int
re_simple(char *pat)
{
	int nr = 0;
	char *s = pat;

#if 0
	if (*s++ != '%')
		return 0;
#endif
	if (s == 0)
		return 0;
	if (*s == '%')
		s++;
	while(*s) {
		if (*s == '_')
			return 0;
		if (*s++ == '%')
			nr++;
	}
	if (*(s-1) != '%')
		return 0;
	return nr;
}

static str
PCRElike_pcre(int *ret, int *b, str *pat, str *esc, bit us, bit ignore)
{
	char *ppat = NULL;
	str r = MAL_SUCCEED;
	int nr;

	/* no escape, try if a simple list of keywords works */
	if (strlen(*esc) == 0 && (nr = re_simple(*pat)) > 0) {
		RE *re = re_create(*pat, nr);

		BAT *bp = BATdescriptor(*b);
		BAT *res = NULL;

		if (bp == NULL)
			throw(MAL, "pcre.like", OPERATION_FAILED);
		if (us)
			res = re_uselect(re, bp, ignore);
		else
			res = re_select(re, bp, ignore);

		re_destroy(re);
		*ret = res->batCacheid;
		BBPkeepref(res->batCacheid);
		BBPreleaseref(bp->batCacheid);
		return MAL_SUCCEED;
	}

	r = PCREsql2pcre(&ppat, pat, esc);

	if (!r && ppat) {
		if (strcmp(ppat, (char*)str_nil) == 0) {
			/* there is no pattern or escape involved, fall back to
			 * simple (no PCRE) match */
			/* FIXME: we have a slight problem here if we need a case
			 * insensitive match, so even though there is no pattern,
			 * just fall back to PCRE for the moment.  If there is a
			 * case insensitive BAT*select, we should use that instead */
			if (ignore) {
				GDKfree(ppat);
				ppat = GDKmalloc(sizeof(char) * (strlen(*pat) + 3));
				if (ppat == NULL)
					throw(MAL, "pcre.like", MAL_MALLOC_FAIL); /* likely to fail hard as well */

				sprintf(ppat, "^%s$", *pat);
				if (us)
					r = PCREuselect(ret, &ppat, b, &ignore);
				else
					r = PCREselect(ret, &ppat, b, &ignore);
			} else {
				BAT *bp = BATdescriptor(*b);
				BAT *res = NULL;

				if (bp == NULL)
					throw(MAL, "pcre.like", OPERATION_FAILED); /*operation?*/
				if (us)
					res = BATuselect(bp, *pat, *pat);
				else
					res = BATselect(bp, *pat, *pat);

				*ret = res->batCacheid;
				BBPkeepref(res->batCacheid);
				BBPreleaseref(bp->batCacheid);
				r = MAL_SUCCEED;
			}
		} else {
			if (us)
				r = PCREuselect(ret, &ppat, b, &ignore);
			else
				r = PCREselect(ret, &ppat, b, &ignore);
		}
	}
	if (ppat)
		GDKfree(ppat);
	return r;
}

str
PCRElike_uselect_pcre(int *ret, int *b, str *pat, str *esc)
{
	return PCRElike_pcre(ret,b,pat,esc,TRUE,FALSE);
}

str
PCREilike_uselect_pcre(int *ret, int *b, str *pat, str *esc)
{
	return PCRElike_pcre(ret,b,pat,esc,TRUE,TRUE);
}

str
PCRElike_select_pcre(int *ret, int *b, str *pat, str *esc)
{
	return PCRElike_pcre(ret,b,pat,esc,FALSE,FALSE);
}

str
PCREilike_select_pcre(int *ret, int *b, str *pat, str *esc)
{
	return PCRElike_pcre(ret,b,pat,esc,FALSE,TRUE);
}
