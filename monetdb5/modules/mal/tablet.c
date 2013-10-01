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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 *  Niels Nes, Martin Kersten
 *
 * Parallel bulk load for SQL
 * The COPY INTO command for SQL is heavily CPU bound, which means
 * that ideally we would like to exploit the multi-cores to do that
 * work in parallel.
 * Complicating factors are the initial record offset, the
 * possible variable length of the input, and the original sort order
 * that should preferable be maintained.
 *
 * The code below consists of a file reader, which breaks up the
 * file into chunks of distinct lines. Then multiple parallel threads
 * grab them, and break them on the field boundaries.
 * After all fields are identified this way, the columns are converted
 * and stored in the BATs.
 *
 * The threads get a reference to a private copy of the READERtask.
 * It includes a list of columns they should handle. This is a basis
 * to distributed cheap and expensive columns over threads.
 *
 * The file reader overlaps IO with updates of the BAT.
 * Also the buffer size of the block stream might be a little small for
 * this task (1MB). It has been increased to 8MB, which indeed improved.
 *
 * The work divider allocates subtasks to threads based on the
 * observed time spending so far.
 */

#include "monetdb_config.h"
#include "tablet.h"
#include "algebra.h"

#include <string.h>
#include <ctype.h>

tablet_export str CMDtablet_input(int *ret, int *nameid, int *sepid, int *typeid, stream *s, int *nr);


static BAT *
void_bat_create(int adt, BUN nr)
{
	BAT *b = BATnew(TYPE_void, adt, BATTINY);

	/* check for correct structures */
	if (b == NULL)
		return b;
	if (BATmirror(b))
		BATseqbase(b, 0);
	BATsetaccess(b, BAT_APPEND);
	if (nr > (BUN) REMAP_PAGE_MAXSIZE)
		BATmmap(b, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 0);
	if (nr > BATTINY && adt)
		b = BATextend(b, nr);
	if (b == NULL)
		return b;

	b->hsorted = TRUE;
	b->hrevsorted = FALSE;
	b->H->norevsorted = 1;
	b->hkey = TRUE;
	b->H->nil = FALSE;
	b->H->nonil = TRUE;

	/* disable all properties here */
	b->tsorted = FALSE;
	b->trevsorted = FALSE;
	b->T->nosorted = 0;
	b->T->norevsorted = 0;
	b->tdense = FALSE;
	b->T->nodense = 0;
	b->tkey = FALSE;
	b->T->nokey[0] = 0;
	b->T->nokey[1] = 1;
	return b;
}

static void *
TABLETstrFrStr(Column *c, char *s, char *e)
{
	int len = (int) (e - s + 1);	/* 64bit: should check for overflow */

	if (c->len < len) {
		c->len = len;
		c->data = GDKrealloc(c->data, len);
	}

	if (s == e) {
		*(char *) c->data = 0;
	} else if (GDKstrFromStr(c->data, (unsigned char *) s, (ssize_t) (e - s)) < 0) {
		return NULL;
	}
	return c->data;
}

void *
TABLETadt_frStr(Column *c, int type, char *s, char *e, char quote)
{
	if (s == NULL || (!quote && strcmp(s, "nil") == 0)) {
		memcpy(c->data, ATOMnilptr(type), c->nillen);
	} else if (type == TYPE_str) {
		return TABLETstrFrStr(c, s, e);
	} else {
		(void) (*BATatoms[type].atomFromStr) (s, &c->len, (ptr) &c->data);
	}
	return c->data;
}

int
TABLETadt_toStr(void *extra, char **buf, int *len, int type, ptr a)
{
	(void) extra;				/* fool compiler */
	if (type == TYPE_str) {
		char *dst, *src = a;
		int l;

		if (GDK_STRNIL(src)) {
			src = "nil";
		}
		l = (int) strlen(src);
		if (l + 3 > *len) {
			GDKfree(*buf);
			*len = 2 * l + 3;
			*buf = GDKzalloc(*len);
		}
		dst = *buf;
		dst[0] = '"';
		strncpy(dst + 1, src, l);
		dst[l + 1] = '"';
		dst[l + 2] = 0;
		return l + 2;
	} else {
		return (*BATatoms[type].atomToStr) (buf, len, a);
	}
}

void
TABLETdestroy_format(Tablet *as)
{
	BUN p;
	Column *fmt = as->format;

	for (p = 0; p < as->nr_attrs; p++) {
		if (fmt[p].c)
			BBPunfix(fmt[p].c->batCacheid);
		if (fmt[p].data)
			GDKfree(fmt[p].data);
		if (fmt[p].type)
			GDKfree(fmt[p].type);
	}
	GDKfree(fmt);
}

static oid
check_BATs(Tablet *as)
{
	Column *fmt = as->format;
	BUN i = 0;
	BUN cnt;
	oid base;

	if (fmt[i].c == NULL)
		i++;
	cnt = BATcount(fmt[i].c);
	base = fmt[i].c->hseqbase;

	if (!BAThdense(fmt[i].c) || as->nr != cnt)
		return oid_nil;

	for (i = 0; i < as->nr_attrs; i++) {
		BAT *b;
		BUN offset;

		b = fmt[i].c;
		if (b == NULL)
			continue;
		offset = BUNfirst(b) + as->offset;

		if (BATcount(b) != cnt || !BAThdense(b) || b->hseqbase != base)
			return oid_nil;

		fmt[i].p = offset;
	}
	return base;
}

int
TABLETcreate_bats(Tablet *as, BUN est)
{
	Column *fmt = as->format;
	BUN i;

	for (i = 0; i < as->nr_attrs; i++) {
		fmt[i].c = void_bat_create(fmt[i].adt, est);
		fmt[i].ci = bat_iterator(fmt[i].c);
		if (!fmt[i].c) {
			GDKerror("TABLETcreate_bats: Failed to create bat of size " BUNFMT "\n", as->nr);
			return -1;
		}
	}
	return 0;
}

BAT **
TABLETcollect(Tablet *as)
{
	BAT **bats = GDKmalloc(sizeof(BAT *) * as->nr_attrs);
	Column *fmt = as->format;
	BUN i;
	BUN cnt = BATcount(fmt[0].c);

	if (bats == NULL)
		return NULL;
	for (i = 0; i < as->nr_attrs; i++) {
		bats[i] = fmt[i].c;
		BBPincref(bats[i]->batCacheid, FALSE);
		BATsetaccess(fmt[i].c, BAT_READ);
		BATderiveProps(fmt[i].c, 1);

		if (cnt != BATcount(fmt[i].c)) {
			if (as->error == 0)	/* a new error */
				GDKerror("Error: column " BUNFMT "  count " BUNFMT " differs from " BUNFMT "\n", i, BATcount(fmt[i].c), cnt);
			return NULL;
		}
	}
	return bats;
}

BAT **
TABLETcollect_parts(Tablet *as, BUN offset)
{
	BAT **bats = GDKmalloc(sizeof(BAT *) * as->nr_attrs);
	Column *fmt = as->format;
	BUN i;
	BUN cnt = BATcount(fmt[0].c);

	if (bats == NULL)
		return NULL;
	for (i = 0; i < as->nr_attrs; i++) {
		BAT *b = fmt[i].c;
		BAT *bv = NULL;

		BATsetaccess(b, BAT_READ);
		bv = BATslice(b, offset, BATcount(b));
		bats[i] = bv;
		BATderiveProps(bv, 1);

		b->hkey &= bv->hkey;
		b->tkey &= bv->tkey;
		b->H->nonil &= bv->H->nonil;
		b->T->nonil &= bv->T->nonil;
		b->hdense &= bv->hdense;
		b->tdense &= bv->tdense;
		if (b->hsorted != bv->hsorted)
			b->hsorted = 0;
		if (b->hrevsorted != bv->hrevsorted)
			b->hrevsorted = 0;
		if (b->tsorted != bv->tsorted)
			b->tsorted = 0;
		if (b->trevsorted != bv->trevsorted)
			b->trevsorted = 0;
		b->batDirty = TRUE;

		if (cnt != BATcount(b)) {
			if (as->error == 0)	/* a new error */
				GDKerror("Error: column " BUNFMT "  count " BUNFMT " differs from " BUNFMT "\n", i, BATcount(b), cnt);
			return NULL;
		}
	}
	return bats;
}

static char *
tablet_skip_string(char *s, char quote)
{
	while (*s) {
		if (*s == '\\' && s[1] != '\0')
			s++;
		else if (*s == quote) {
			if (s[1] == quote)
				*s++ = '\\';	/* sneakily replace "" with \" */
			else
				break;
		}
		s++;
	}
	assert(*s == quote || *s == '\0');
	if (*s)
		s++;
	else
		return NULL;
	return s;
}

static int
TABLET_error(stream *s)
{
	if (!mnstr_errnr(GDKerr)) {
		char *err = mnstr_error(s);

		mnstr_printf(GDKout, "#Stream error %s\n", err);
		/* use free as stream allocates out side GDK */
		if (err)
			free(err);
	}
	return -1;
}

/* The output line is first built before being sent. It solves a problem
   with UDP, where you may loose most of the information using short writes
*/
static inline int
output_line(char **buf, int *len, char **localbuf, int *locallen, Column *fmt, stream *fd, BUN nr_attrs, ptr id)
{
	BUN i;
	int fill = 0;

	for (i = 0; i < nr_attrs; i++) {
		if (fmt[i].c == NULL)
			continue;
		fmt[i].p = BUNfnd(fmt[i].c, id);

		if (fmt[i].p == BUN_NONE)
			break;
	}
	if (i == nr_attrs) {
		for (i = 0; i < nr_attrs; i++) {
			Column *f = fmt + i;
			const char *p;
			int l;

			if (f->c) {
				p = BUNtail(f->ci, f->p);

				if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
					p = f->nullstr;
					l = (int) strlen(f->nullstr);
				} else {
					l = f->tostr(f->extra, localbuf, locallen, f->adt, p);
					p = *localbuf;
				}
				if (fill + l + f->seplen >= *len) {
					/* extend the buffer */
					*buf = GDKrealloc(*buf, fill + l + f->seplen + BUFSIZ);
					*len = fill + l + f->seplen + BUFSIZ;
					if (*buf == NULL)
						return -1;
				}
				strncpy(*buf + fill, p, *len - fill - 1);
				fill += l;
			}
			strncpy(*buf + fill, f->sep, *len - fill - 1);
			fill += f->seplen;
		}
	}
	if (mnstr_write(fd, *buf, 1, fill) != fill)
		return TABLET_error(fd);
	return 0;
}

static inline int
output_line_dense(char **buf, int *len, char **localbuf, int *locallen, Column *fmt, stream *fd, BUN nr_attrs)
{
	BUN i;
	int fill = 0;

	for (i = 0; i < nr_attrs; i++) {
		Column *f = fmt + i;
		const char *p;
		int l;

		if (f->c) {
			p = BUNtail(f->ci, f->p);

			if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
				p = f->nullstr;
				l = (int) strlen(p);
			} else {
				l = f->tostr(f->extra, localbuf, locallen, f->adt, p);
				p = *localbuf;
			}
			if (fill + l + f->seplen >= *len) {
				/* extend the buffer */
				*buf = GDKrealloc(*buf, fill + l + f->seplen + BUFSIZ);
				*len = fill + l + f->seplen + BUFSIZ;
				if (*buf == NULL)
					return -1;
			}
			strncpy(*buf + fill, p, *len - fill - 1);
			fill += l;
			f->p++;
		}
		strncpy(*buf + fill, f->sep, *len - fill - 1);
		fill += f->seplen;
	}
	if (mnstr_write(fd, *buf, 1, fill) != fill)
		return TABLET_error(fd);
	return 0;
}

static inline int
output_line_lookup(char **buf, int *len, Column *fmt, stream *fd, BUN nr_attrs, BUN id)
{
	BUN i;

	for (i = 0; i < nr_attrs; i++) {
		Column *f = fmt + i;

		if (f->c) {
			char *p = BUNtail(f->ci, id +BUNfirst(f->c));

			if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
				size_t l = strlen(f->nullstr);
				if (mnstr_write(fd, f->nullstr, 1, l) != (ssize_t) l)
					return TABLET_error(fd);
			} else {
				int l = f->tostr(f->extra, buf, len, f->adt, p);

				if (mnstr_write(fd, *buf, 1, l) != l)
					return TABLET_error(fd);
			}
		}
		if (mnstr_write(fd, f->sep, 1, f->seplen) != f->seplen)
			return TABLET_error(fd);
	}
	return 0;
}

static int
tablet_read_more(bstream *in, stream *out, size_t n)
{
	if (out) {
		do {
			/* query is not finished ask for more */
			/* we need more query text */
			if (bstream_next(in) < 0)
				return EOF;
			if (in->eof) {
				if (out && mnstr_write(out, PROMPT2, sizeof(PROMPT2) - 1, 1) == 1)
					mnstr_flush(out);
				in->eof = 0;
				/* we need more query text */
				if (bstream_next(in) <= 0)
					return EOF;
			}
		} while (in->len <= in->pos);
	} else if (bstream_read(in, n) <= 0) {
		return EOF;
	}
	return 1;
}

/*
 * Fast Load
 * To speedup the CPU intensive loading of files we have to break
 * the file into pieces and perform parallel analysis. Experimentation
 * against lineitem SF1 showed that half of the time goes into very
 * basis atom analysis (41 out of 102 B instructions).
 * Furthermore, the actual insertion into the BATs takes only
 * about 10% of the total. With multi-core processors around
 * it seems we can gain here significantly.
 *
 * The approach taken is to fork a parallel scan over the text file.
 * We assume that the blocked stream is already
 * positioned correctly at the reading position. The start and limit
 * indicates the byte range to search for tuples.
 * If start> 0 then we first skip to the next record separator.
 * If necessary we read more than 'limit' bytes to ensure parsing a complete
 * record and stop at the record boundary.
 * Beware, we should allocate Tablet descriptors for each file segment,
 * otherwise we end up with a gross concurrency control problem.
 * The resulting BATs should be glued at the final phase.
 *
 * Raw Load
 * Front-ends can bypass most of the overhead in loading the BATs
 * by preparing the corresponding files directly and replace those
 * created by e.g. the SQL frontend.
 * This strategy is only advisable for cases where we have very
 * large files >200GB and/or are created by a well debugged code.
 *
 * To experiment with this approach, the code base responds
 * on negative number of cores by dumping the data directly in BAT
 * storage format into a collections of files on disk.
 * It reports on the actions to be taken to replace BATs.
 * This technique is initially only supported for fixed-sized columns.
 * The rawmode() indicator acts as the internal switch.
 */

/*
 * To speed up loading ascii files we have to determine the number of blocks.
 * This depends on the number of cores available.
 * For the time being we hardwire this decision based on our own
 * platforms.
 * Furthermore, we only consider parallel load for file-based requests.
 *
 * To simplify our world, we assume a single loader process.
 */

static int
output_file_default(Tablet *as, BAT *order, stream *fd)
{
	int len = BUFSIZ, locallen = BUFSIZ, res = 0;
	char *buf = GDKmalloc(len);
	char *localbuf = GDKmalloc(len);
	BUN p, q;
	BUN i = 0;
	BUN offset = BUNfirst(order) + as->offset;
	BATiter orderi = bat_iterator(order);

	if (buf == NULL)
		return -1;
	for (q = offset + as->nr, p = offset; p < q; p++) {
		ptr h = BUNhead(orderi, p);

		if ((res = output_line(&buf, &len, &localbuf, &locallen, as->format, fd, as->nr_attrs, h)) < 0) {
			GDKfree(buf);
			GDKfree(localbuf);
			return res;
		}
		i++;
#ifdef _DEBUG_TABLET_
		if ((i % 1000000) == 0)
			mnstr_printf(GDKout, "#dumped " BUNFMT " lines\n", i);
#endif
	}
	GDKfree(localbuf);
	GDKfree(buf);
	return res;
}

static int
output_file_dense(Tablet *as, stream *fd)
{
	int len = BUFSIZ, locallen= BUFSIZ, res = 0;
	char *buf = GDKmalloc(len);
	char *localbuf = GDKmalloc(len);
	BUN i = 0;

	if (buf == NULL)
		return -1;
	for (i = 0; i < as->nr; i++) {
		if ((res = output_line_dense(&buf, &len, &localbuf, &locallen, as->format, fd, as->nr_attrs)) < 0) {
			GDKfree(buf);
			GDKfree(localbuf);
			return res;
		}
#ifdef _DEBUG_TABLET_
		if ((i % 1000000) == 0)
			mnstr_printf(GDKout, "#dumped " BUNFMT " lines\n", i);
#endif
	}
	GDKfree(localbuf);
	GDKfree(buf);
	return res;
}

static int
output_file_ordered(Tablet *as, BAT *order, stream *fd, oid base)
{
	int len = BUFSIZ, res = 0;
	char *buf = GDKmalloc(len);
	BUN p, q;
	BUN i = 0;
	BUN offset = BUNfirst(order) + as->offset;
	BATiter orderi = bat_iterator(order);

	if (buf == NULL)
		return -1;
	for (q = offset + as->nr, p = offset; p < q; p++, i++) {
		BUN h = (BUN) (*(oid *) BUNhead(orderi, p) - base);

		if ((res = output_line_lookup(&buf, &len, as->format, fd, as->nr_attrs, h)) < 0) {
			GDKfree(buf);
			return res;
		}
#ifdef _DEBUG_TABLET_
		if ((i % 1000000) == 0)
			mnstr_printf(GDKout, "#dumped " BUNFMT " lines\n", i);
#endif
	}
	GDKfree(buf);
	return res;
}

int
TABLEToutput_file(Tablet *as, BAT *order, stream *s)
{
	oid base = oid_nil;
	BUN maxnr = BATcount(order);
	int ret = 0;

	/* only set nr if it is zero or lower (bogus) to the maximum value
	 * possible (BATcount), if already set within BATcount range,
	 * preserve value such that for instance SQL's reply_size still
	 * works
	 */
	if (as->nr == BUN_NONE || as->nr > maxnr)
		as->nr = maxnr;

	if ((base = check_BATs(as)) != oid_nil) {
		if (BAThdense(order) && order->hseqbase == base)
			ret = output_file_dense(as, s);
		else
			ret = output_file_ordered(as, order, s, base);
	} else {
		ret = output_file_default(as, order, s);
	}
	return ret;
}
/*
 *  Niels Nes, Martin Kersten
 *
 * Parallel bulk load for SQL
 * The COPY INTO command for SQL is heavily CPU bound, which means
 * that ideally we would like to exploit the multi-cores to do that
 * work in parallel.
 * Complicating factors are the initial record offset, the
 * possible variable length of the input, and the original sort order
 * that should preferable be maintained.
 *
 * The code below consists of a file reader, which breaks up the
 * file into chunks of distinct lines. Then multiple parallel threads
 * grab them, and break them on the field boundaries.
 * After all fields are identified this way, the columns are converted
 * and stored in the BATs.
 *
 * The threads get a reference to a private copy of the READERtask.
 * It includes a list of columns they should handle. This is a basis
 * to distributed cheap and expensive columns over threads.
 *
 * The file reader overlaps IO with updates of the BAT.
 * Also the buffer size of the block stream might be a little small for
 * this task (1MB). It has been increased to 8MB, which indeed improved.
 *
 * The work divider allocates subtasks to threads based on the
 * observed time spending so far.
 */

/* #define _DEBUG_TABLET_*/
/* #define MLOCK_TST did not make a difference on sf10 */

#define BREAKLINE 1
#define UPDATEBAT 2

typedef struct {
	int id;						/* for self reference */
	int state;					/* line break=1 , 2 = update bat */
	int workers;				/* how many concurrent ones */
	int error;					/* error during line break */
	int next;
	int limit;
	lng *time, wtime;			/* time per col + time per thread */
	int rounds;					/* how often did we divide the work */
	MT_Id tid;
	MT_Sema producer;			/* reader waits for call */
	MT_Sema consumer;			/* data available */
	int ateof;					/* io control */
	bstream *b;
	stream *out;
	MT_Sema sema;				/* threads wait for work , negative next implies exit */
	MT_Sema reply;				/* let reader continue */
	Tablet *as;
	char *errbuf;
	char *csep, *rsep;
	size_t seplen, rseplen;
	char quote;
	char *base, *input;			/* area for tokenizer */
	size_t basesize;
	int *cols;					/* columns to handle */
	char ***fields;
} READERtask;

/*
 * The line is broken into pieces directly on their field separators. It assumes that we have
 * the record in the cache already, so we can do most work quickly.
 * Furthermore, it assume a uniform (SQL) pattern, without whitespace skipping, but with quote and separator.
 */

static str
SQLload_error(READERtask *task, int idx)
{
	str line;
	size_t sz = 0;
	unsigned int i;

	for (i = 0; i < task->as->nr_attrs; i++)
		if (task->fields[i][idx])
			sz += strlen(task->fields[i][idx]) + task->seplen;
		else
			sz += task->seplen;

	line = (str) GDKzalloc(sz + task->rseplen + 1);
	if (line == 0) {
		task->as->error = M5OutOfMemory;
		return 0;
	}
	for (i = 0; i < task->as->nr_attrs; i++) {
		if (task->fields[i][idx])
			strcat(line, task->fields[i][idx]);
		if (i < task->as->nr_attrs - 1)
			strcat(line, task->csep);
	}
	strcat(line, task->rsep);
	return line;
}

/*
 * The parsing of the individual values is straightforward. If the value represents
 * the null-replacement string then we grab the underlying nil.
 * If the string starts with the quote identified from SQL, we locate the tail
 * and interpret the body.
 */
static inline int
SQLinsert_val(Column *fmt, char *s, char quote, ptr key, str *err, int col)
{
	const void *adt;
	char buf[BUFSIZ];
	char *e, *t;
	int ret = 0;

	/* include testing on the terminating null byte !! */
	if (fmt->nullstr && strncasecmp(s, fmt->nullstr, fmt->null_length + 1) == 0) {
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout, "nil value '%s' (%d) found in :%s\n", fmt->nullstr, fmt->nillen, (s ? s : ""));
#endif
		adt = fmt->nildata;
		fmt->c->T->nonil = 0;
	} else if (quote && *s == quote) {
		/* strip the quotes when present */
		s++;
		for (t = e = s; *t; t++)
			if (*t == quote)
				e = t;
		*e = 0;
		adt = fmt->frstr(fmt, fmt->adt, s, e, 0);
		/* The user might have specified a null string escape
		 * e.g. NULL as '', which should be tested */
		if (adt == NULL && s == e && fmt->nullstr &&
			strncasecmp(s, fmt->nullstr, fmt->null_length + 1) == 0) {
			adt = fmt->nildata;
			fmt->c->T->nonil = 0;
		}
	} else {
		for (e = s; *e; e++) ;
		adt = fmt->frstr(fmt, fmt->adt, s, e, 0);
	}

	if (adt == NULL) {
		char *val;
		val = *s ? GDKstrdup(s) : GDKstrdup("");
		if (*err == NULL) {
			if (snprintf(buf, BUFSIZ,
						 "value '%.*s%s' from line " BUNFMT
						 " field %d not inserted, expecting type %s\n",
						 BUFSIZ - 200, val,
						 strlen(val) > (size_t) BUFSIZ - 200 ? "..." : "",
						 BATcount(fmt->c) + 1, col, fmt->type) < 0)
				snprintf(buf, BUFSIZ,
						 "value from line " BUNFMT
						 " field %d not inserted, expecting type %s\n",
						 BATcount(fmt->c) + 1, col, fmt->type);
			*err = GDKstrdup(buf);
		}
		GDKfree(val);
		/* replace it with a nil */
		adt = fmt->nildata;
		fmt->c->T->nonil = 0;
		ret = -1;
	}
	/* key may be NULL but that's not a problem, as long as we have void */
	bunfastins(fmt->c, key, adt);
	return ret;
  bunins_failed:
	if (*err == NULL) {
		snprintf(buf, BUFSIZ,
				 "value from line " BUNFMT " field %d not inserted: %s\n",
				 BATcount(fmt->c) + 1, col, GDKerrbuf);
		*err = GDKstrdup(buf);
	}
	return -1;
}

static int
SQLworker_column(READERtask *task, int col)
{
	int i;
	Column *fmt = task->as->format;
	str err = 0;

	/* watch out for concurrent threads */
	MT_lock_set(&mal_copyLock, "tablet insert value");
	if (BATcapacity(fmt[col].c) < BATcount(fmt[col].c) + task->next) {
		if ((fmt[col].c = BATextend(fmt[col].c, BATgrows(fmt[col].c) + task->limit)) == NULL) {
			if (task->as->error == NULL)
				task->as->error = GDKstrdup("Failed to extend the BAT, perhaps disk full");
			MT_lock_unset(&mal_copyLock, "tablet insert value");
			mnstr_printf(GDKout, "Failed to extend the BAT, perhaps disk full");
			return -1;
		}
	}
	MT_lock_unset(&mal_copyLock, "tablet insert value");

	for (i = 0; i < task->next; i++)
		if (task->fields[col][i]) {	/* no errors */
			if (SQLinsert_val(&fmt[col], task->fields[col][i], task->quote, NULL, &err, col + 1)) {
				assert(err != NULL);
				MT_lock_set(&mal_copyLock, "tablet insert value");
				if (!task->as->tryall) {
					/* watch out for concurrent threads */
					if (task->as->error == NULL)
						task->as->error = err;	/* restore for upper layers */
				} else
					BUNins(task->as->complaints, NULL, err, TRUE);
				MT_lock_unset(&mal_copyLock, "tablet insert value");
				break;
			}
		}

	if (err) {
		/* watch out for concurrent threads */
		MT_lock_set(&mal_copyLock, "tablet insert value");
		if (task->as->error == NULL)
			task->as->error = err;	/* restore for upper layers */
		MT_lock_unset(&mal_copyLock, "tablet insert value");
	}
	return err ? -1 : 0;
}

/*
 * The lines are broken on the column separator. Any error is shown and reflected with
 * setting the reference of the offending row fields to NULL.
 * This allows the loading to continue, skipping the minimal number of rows.
 */
static int
SQLload_file_line(READERtask *task, int idx)
{
	BUN i;
	char errmsg[BUFSIZ];
	char ch = *task->csep;
	char *line = task->fields[0][idx];
	Tablet *as = task->as;
	Column *fmt = as->format;

	errmsg[0] = 0;
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "SQL break line id %d  state %d\n%s", task->id, idx, task->fields[0][idx]);
#endif

	for (i = 0; i < as->nr_attrs; i++) {
		task->fields[i][idx] = line;
		/* recognize fields starting with a quote, keep them */
		if (task->quote && *line == task->quote) {
			line = tablet_skip_string(line + 1, task->quote);
			if (!line) {
				str errline = SQLload_error(task, task->next);
				snprintf(errmsg, BUFSIZ,
						 "End of string (%c) missing in \"%s\" at line " BUNFMT
						 " field " BUNFMT "\n",
						 task->quote, (errline ? errline : ""),
						 BATcount(as->format->c) + task->next + 1, i);
				if (errline)
					GDKerror("%s", errmsg);
				GDKfree(errline);
				goto errors;
			}
		}

		/* eat away the column separator */
		for (; *line; line++)
			if (*line == '\\') {
				if (line[1])
					line++;
			} else if (*line == ch && (task->seplen == 1 || strncmp(line, task->csep, task->seplen) == 0)) {
				*line = 0;
				line += task->seplen;
				goto endoffield;
			}
		/* not enough fields */
		if (i < as->nr_attrs - 1) {
			snprintf(errmsg, BUFSIZ,
					 "missing separator '%s' line " BUNFMT " expecting "
					 BUNFMT " got " BUNFMT "  fields\n",
					 fmt->sep, BATcount(fmt->c) + idx, as->nr_attrs - 1, i);
		  errors:
			/* we save all errors detected */
			MT_lock_set(&mal_copyLock, "tablet line break");
			if (as->tryall)
				BUNins(as->complaints, NULL, errmsg, TRUE);
			if (as->error) {
				str s = GDKstrdup(errmsg);
				snprintf(errmsg, BUFSIZ, "%s%s", as->error, s);
				GDKfree(s);
			}
			as->error = GDKstrdup(errmsg);
			MT_lock_unset(&mal_copyLock, "tablet line break");
			for (i = 0; i < as->nr_attrs; i++)
				task->fields[i][idx] = NULL;
			break;
		}
	  endoffield:;
	}
	return as->error ? -1 : 0;
}

static void
SQLworker(void *arg)
{
	READERtask *task = (READERtask *) arg;
	unsigned int i;
	int j, piece;
	lng t0;
	Thread thr;

	thr = THRnew("SQLworker");
	GDKsetbuf(GDKmalloc(GDKMAXERRLEN));	/* where to leave errors */
	GDKerrbuf[0] = 0;
	task->errbuf = GDKerrbuf;
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "SQLworker %d started\n", task->id);
#endif
	while (task->next >= 0) {
		MT_sema_down(&task->sema, "SQLworker");

		if (task->next < 0) {
			MT_sema_up(&task->reply, "SQLworker");
#ifdef _DEBUG_TABLET_
			mnstr_printf(GDKout, "SQLworker terminated\n");
#endif
			goto do_return;
		}

		/* stage one, break the lines spread the worker over the workers */
		if (task->state == BREAKLINE) {
			t0 = GDKusec();
			piece = (task->next + task->workers) / task->workers;
#ifdef _DEBUG_TABLET_
			mnstr_printf(GDKout, "SQLworker id %d %d  piece %d-%d\n",
						 task->id, task->next, piece * task->id,
						 (task->id + 1) * piece);
#endif
			for (j = piece * task->id; j < task->next && j < piece * (task->id +1); j++)
				if (task->fields[0][j])
					if (SQLload_file_line(task, j) < 0) {
						task->error++;
						break;
					}
			task->wtime = GDKusec() - t0;
		} else if (task->state == UPDATEBAT)
			/* stage two, updating the BATs */
			for (i = 0; i < task->as->nr_attrs; i++)
				if (task->cols[i]) {
					t0 = GDKusec();
					SQLworker_column(task, task->cols[i] - 1);
					t0 = GDKusec() - t0;
					task->time[i] += t0;
					task->wtime += t0;
				}
		task->state = 0;
		MT_sema_up(&task->reply, "SQLworker");
	}
	MT_sema_up(&task->reply, "SQLworker");
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "SQLworker exits\n");
#endif

  do_return:
	GDKfree(GDKerrbuf);
	GDKsetbuf(0);
	THRdel(thr);
}

static void
SQLworkdivider(READERtask *task, READERtask *ptask, int nr_attrs, int threads)
{
	int i, j, mi;
	lng *loc, t;

	/* after a few rounds we stick to the work assignment */
	if (task->rounds > 8)
		return;
	/* simple round robin the first time */
	if (threads == 1 || task->rounds++ == 0) {
		for (i = j = 0; i < nr_attrs; i++, j++)
			ptask[j % threads].cols[i] = task->cols[i];
		return;
	}
	loc = (lng *) GDKzalloc(sizeof(lng) * threads);
	if (loc == 0) {
		task->as->error = M5OutOfMemory;
		return;
	}
	/* use of load directives */
	for (i = 0; i < nr_attrs; i++)
		for (j = 0; j < threads; j++)
			ptask[j].cols[i] = 0;

	/* sort the attributes based on their total time cost */
	for (i = 0; i < nr_attrs; i++)
		for (j = i + 1; j < nr_attrs; j++)
			if (task->time[i] < task->time[j]) {
				mi = task->cols[i];
				t = task->time[i];
				task->cols[i] = task->cols[j];
				task->cols[j] = mi;
				task->time[i] = task->time[j];
				task->time[j] = t;
			}

	/* now allocate the work to the threads */
	for (i = 0; i < nr_attrs; i++, j++) {
		mi = 0;
		for (j = 1; j < threads; j++)
			if (loc[j] < loc[mi])
				mi = j;

		ptask[mi].cols[i] = task->cols[i];
		loc[mi] += task->time[i];
	}
	/* reset the timer */
	for (i = 0; i < nr_attrs; i++, j++)
		task->time[i] = 0;
	GDKfree(loc);
}

/*
 * Reading is handled by a separate task as a preparation for
 * mode parallelism
 */
static void
SQLloader(void *p)
{
	READERtask *task = (READERtask *) p;

#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "SQLloader started\n");
#endif
	while (task->ateof == 0) {
		MT_sema_down(&task->producer, "SQLloader");
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout, "SQL loader got buffer \n");
#endif
		if (task->ateof)		/* forced exit received */
			break;
		task->ateof = tablet_read_more(task->b, task->out, task->b->size - (task->b->len - task->b->pos)) == EOF;
		MT_sema_up(&task->consumer, "SQLloader");
	}
}

#define MAXWORKERS	64

BUN
SQLload_file(Client cntxt, Tablet *as, bstream *b, stream *out, char *csep, char *rsep, char quote, lng skip, lng maxrow)
{
	char *s, *e, *end;
	BUN cnt = 0;
	int res = 0;				/* < 0: error, > 0: success, == 0: continue processing */
	int j;
	BUN i;
	size_t rseplen;
	READERtask *task = (READERtask *) GDKzalloc(sizeof(READERtask));
	READERtask ptask[MAXWORKERS];
	int threads = (!maxrow || maxrow > (1 << 16)) ? (GDKnr_threads < MAXWORKERS ? GDKnr_threads : MAXWORKERS) : 1;
	lng lio = 0, tio, t1 = 0, total = 0, iototal = 0;
	int vmtrim = GDK_vm_trim;
	str msg = MAL_SUCCEED;

	for (i = 0; i < MAXWORKERS; i++)
		ptask[i].cols = 0;

	if (task == 0) {
		as->error = M5OutOfMemory;
		return BUN_NONE;
	}

	/* trimming process should not be active during this process. */
	/* on sf10 experiments it showed a slowdown of a factor 2 on */
	/* large tables. Instead rely on madvise */
	GDK_vm_trim = 0;

	assert(rsep);
	assert(csep);
	assert(maxrow < 0 || maxrow <= (lng) BUN_MAX);
	rseplen = strlen(rsep);
	task->fields = (char ***) GDKzalloc(as->nr_attrs * sizeof(char **));
	task->cols = (int *) GDKzalloc(as->nr_attrs * sizeof(int));
	task->time = (lng *) GDKzalloc(as->nr_attrs * sizeof(lng));
	task->base = GDKzalloc(b->size + 2);
	task->basesize = b->size + 2;

	if (task->fields == 0 || task->cols == 0 || task->time == 0 || task->base == 0) {
		as->error = M5OutOfMemory;
		goto bailout;
	}

	task->as = as;
	task->quote = quote;
	task->csep = csep;
	task->seplen = strlen(csep);
	task->rsep = rsep;
	task->rseplen = strlen(rsep);
	task->errbuf = cntxt->errbuf;
	task->input = task->base + 1;	/* wrap the buffer with null bytes */
	task->base[b->size + 1] = 0;

	MT_sema_init(&task->consumer, 0, "task->consumer");
	MT_sema_init(&task->producer, 0, "task->producer");
	task->ateof = 0;
	task->b = b;
	task->out = out;

#ifdef MLOCK_TST
	mlock(task->fields, as->nr_attrs * sizeof(char *));
	mlock(task->cols, as->nr_attrs * sizeof(int));
	mlock(task->time, as->nr_attrs * sizeof(lng));
	mlock(task->base, b->size + 2);
#endif
	as->error = NULL;

	/* there is no point in creating more threads than we have columns */
	if (as->nr_attrs < (BUN) threads)
		threads = (int) as->nr_attrs;

	/* allocate enough space for pointers into the buffer pool.  */
	/* the record separator is considered a column */
	task->limit = (int) (b->size / as->nr_attrs + as->nr_attrs);
	for (i = 0; i < as->nr_attrs; i++) {
		task->fields[i] = GDKzalloc(sizeof(char *) * task->limit);
		if (task->fields[i] == 0) {
			as->error = M5OutOfMemory;
			goto bailout;
		}
#ifdef MLOCK_TST
		mlock(task->fields[i], sizeof(char *) * task->limit);
#endif
		task->cols[i] = (int) (i + 1);	/* to distinguish non initialized later with zero */
	}

#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "Prepare copy work for %d threads col '%s' rec '%s' quot '%c'\n", threads, csep, rsep, quote);
#endif
	task->workers = threads;
	for (j = 0; j < threads; j++) {
		ptask[j] = *task;
		ptask[j].id = j;
		ptask[j].cols = (int *) GDKzalloc(as->nr_attrs * sizeof(int));
		if (ptask[j].cols == 0) {
			as->error = M5OutOfMemory;
			goto bailout;
		}
#ifdef MLOCK_TST
		mlock(ptask[j].cols, sizeof(char *) * task->limit);
#endif
		MT_sema_init(&ptask[j].sema, 0, "ptask[j].sema");
		MT_sema_init(&ptask[j].reply, 0, "ptask[j].reply");
		MT_create_thread(&ptask[j].tid, SQLworker, (void *) &ptask[j], MT_THR_JOINABLE);
	}

	MT_create_thread(&task->tid, SQLloader, (void *) task, MT_THR_JOINABLE);
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "parallel bulk load " LLFMT " - " LLFMT "\n", skip, maxrow);
#endif

	tio = GDKusec();
	MT_sema_up(&task->producer, "SQLload_file");
	MT_sema_down(&task->consumer, "SQLload_file");
	tio = GDKusec() - tio;
	t1 = GDKusec();
#ifdef MLOCK_TST
	mlock(task->b->buf, task->b->size);
#endif
	while ((task->b->pos < task->b->len || !task->b->eof) && cnt < (BUN) maxrow && res == 0) {

		if (task->errbuf && task->errbuf[0]) {
			msg = catchKernelException(cntxt, msg);
			if (msg) {
				showException(task->out, MAL, "copy_from", "%s", msg);
				GDKfree(msg);
				goto bailout;
			}
		}

		if (b->size + 2 > task->basesize) {
			/* b's buffer has grown */
			if (task->basesize >= 32*1024*1024) {
				/* end of record not found within 32M; most likely
				 * wrong delimiter */
				break;
			}
			GDKfree(task->base); /* no need to copy data, so no realloc */
			if ((task->base = GDKmalloc(b->size + 2)) == NULL) {
				/* alloc failure */
				break;
			}
			task->basesize = b->size + 2;
			task->input = task->base + 1;
			*task->base = 0;
		}
		memcpy(task->input, task->b->buf, task->b->size);

#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout, "read pos=" SZFMT " len=" SZFMT " size=" SZFMT " eof=%d \n", task->b->pos, task->b->len, task->b->size, task->b->eof);
#endif

		/* now we fill the copy buffer with pointers to the record */
		/* skipping tuples as needed */
		task->next = 0;

		end = task->input + task->b->len;
		s = task->input + task->b->pos;
		*end = '\0';			/* this is safe, as the stream ensures an extra byte */
		/* Note that we rescan from the start of a record (the last
		 * partial buffer from the previous iteration), even if in the
		 * previous iteration we have already established that there
		 * is no record separator in the first, perhaps significant,
		 * part of the buffer. This is because if the record separator
		 * is longer than one byte, it is too complex (i.e. would
		 * require more state) to be sure what the state of the quote
		 * status is when we back off a few bytes from where the last
		 * scan ended (we need to back off some since we could be in
		 * the middle of the record separator).  If this is too
		 * costly, we have to rethink the matter. */
		e = s;
		while (s < end && task->next < task->limit && (maxrow < 0 || cnt < (BUN) maxrow)) {
			char q = 0;
			/* tokenize the record completely the format of the input
			 * should comply to the following grammar rule [
			 * [[quote][[esc]char]*[quote]csep]*rsep]* where quote is
			 * a single user defined character within the quoted
			 * fields a character may be escaped with a backslash The
			 * user should supply the correct number of fields.
			 * In the first phase we simply break the lines at the
			 * record boundary. */
			if (quote == 0) {
				if (rseplen == 1) {
					for (; *e; e++) {
						if (*e == '\\') {
							e++;
							continue;
						}
						if (*e == *rsep)
							break;
					}
				} else if (rseplen == 2) {
					for (; *e; e++) {
						if (*e == '\\') {
							e++;
							continue;
						}
						if (*e == *rsep && e[1] == rsep[1])
							break;
					}
				} else {
					for (; *e; e++) {
						if (*e == '\\') {
							e++;
							continue;
						}
						if (*e == *rsep && strncmp(e, rsep, rseplen) == 0)
							break;
					}
				}
				if (*e == 0)
					e = 0;		/* nonterminated record, we need more */
			} else if (rseplen == 1) {
				for (; *e; e++) {
					if (*e == q)
						q = 0;
					else if (*e == quote)
						q = *e;
					else if (*e == '\\') {
						if (e[1])
							e++;
					} else if (!q && *e == *rsep)
						break;
				}
				if (*e == 0)
					e = 0;		/* nonterminated record, we need more */
			} else if (rseplen == 2) {
				for (; *e; e++) {
					if (*e == q)
						q = 0;
					else if (*e == quote)
						q = *e;
					else if (*e == '\\') {
						if (e[1])
							e++;
					} else if (!q && e[0] == rsep[0] && e[1] == rsep[1])
						break;
				}
				if (*e == 0)
					e = 0;		/* nonterminated record, we need more */
			} else {
				for (; *e; e++) {
					if (*e == q)
						q = 0;
					else if (*e == quote)
						q = *e;
					else if (*e == '\\') {
						if (e[1])
							e++;
					} else if (!q && *e == *rsep && strncmp(e, rsep, rseplen) == 0)
						break;
				}
				if (*e == 0)
					e = 0;		/* nonterminated record, we need more */
			}

			/* check for incomplete line and end of buffer condition */
			if (e) {
				/* found a complete record, do we need to skip it? */
				if (--skip < 0) {
					task->fields[0][task->next++] = s;
					*e = '\0';
					cnt++;
				}
				s = e + rseplen;
				e = s;
				task->b->pos = (size_t) (s - task->input);
			} else {
				/* no (unquoted) record separator found, read more data */
				break;
			}
		}
		/* start feeding new data */
		MT_sema_up(&task->producer, "SQLload_file");
		t1 = GDKusec() - t1;
		total += t1;
		iototal += tio;
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout, "fill the BATs %d  " BUNFMT " cap " BUNFMT "\n", task->next, cnt, BATcapacity(as->format[0].c));
#endif
		t1 = GDKusec();
		if (task->next) {
			/* activate the workers to break lines */
			for (j = 0; j < threads; j++) {
				/* stage one, break the lines in parallel */
				ptask[j].error = 0;
				ptask[j].state = BREAKLINE;
				ptask[j].next = task->next;
				ptask[j].fields = task->fields;
				ptask[j].limit = task->limit;
				MT_sema_up(&ptask[j].sema, "SQLload_file");
			}
		}
		if (task->next) {
			/* await completion of line break phase */
			for (j = 0; j < threads; j++) {
				MT_sema_down(&ptask[j].reply, "SQLload_file");
				if (ptask[j].error)
					res = -1;
			}
		}
		lio += GDKusec() - t1;	/* line break done */
		if (task->next) {
			if (res == 0) {
				SQLworkdivider(task, ptask, (int) as->nr_attrs, threads);

				/* activate the workers to update the BATs */
				for (j = 0; j < threads; j++) {
					/* stage two, update the BATs */
					ptask[j].state = UPDATEBAT;
					MT_sema_up(&ptask[j].sema, "SQLload_file");
				}
			}
		}

		/* shuffle remainder and continue reading */
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout, "shuffle %d:%s\n", (int) strlen(s), s);
#endif
		tio = GDKusec();
		tio = t1 - tio;

		if (res == 0 && task->next) {
			/* await completion of the BAT updates */
			for (j = 0; j < threads; j++)
				MT_sema_down(&ptask[j].reply, "SQLload_file");
		}
		if (task->ateof)
			break;
		MT_sema_down(&task->consumer, "SQLload_file");
	}

	if (task->b->pos < task->b->len && cnt < (BUN) maxrow && task->ateof) {
		showException(task->out, MAL, "copy_from", "Incomplete record at end of file.\n");
		/* indicate that we did read everything (even if we couldn't
		 * deal with it */
		task->b->pos = task->b->len;
		res = -1;
	}

	if (GDKdebug & GRPalgorithms) {
		if (cnt < (BUN) maxrow && maxrow > 0)
			/* providing a precise count is not always easy, instead
			 * consider maxrow as an upper bound */
			mnstr_printf(GDKout, "#SQLload_file: read error, tuples missing (after loading " BUNFMT " records)\n", BATcount(as->format[0].c));
		mnstr_printf(GDKout, "# COPY reader time " LLFMT " line break " LLFMT " io " LLFMT "\n", total, lio, iototal);
#ifdef _DEBUG_TABLET_
		for (i = 0; i < as->nr_attrs; i++)
			mnstr_printf(GDKout, LLFMT " ", task->time[i]);
		mnstr_printf(GDKout, "\n");
#endif
		for (j = 0; j < threads; j++)
			mnstr_printf(GDKout, "# COPY thread time " LLFMT "\n", ptask[j].wtime);
	}

	task->ateof = 1;
	MT_sema_up(&task->producer, "SQLload_file");
	for (j = 0; j < threads; j++) {
		ptask[j].next = -1;
		MT_sema_up(&ptask[j].sema, "SQLload_file");
	}
	/* wait for their death */
	for (j = 0; j < threads; j++)
		MT_sema_down(&ptask[j].reply, "SQLload_file");
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "Kill the workers\n");
#endif
	for (j = 0; j < threads; j++) {
		MT_join_thread(ptask[j].tid);
		GDKfree(ptask[j].cols);
		MT_sema_destroy(&ptask[j].sema);
		MT_sema_destroy(&ptask[j].reply);
	}
	MT_join_thread(task->tid);

#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout, "Found " BUNFMT " tuples\n", cnt);
#endif
	for (i = 0; i < as->nr_attrs; i++)
		GDKfree(task->fields[i]);
	GDKfree(task->fields);
	GDKfree(task->cols);
	GDKfree(task->time);
	GDKfree(task->base);
	MT_sema_destroy(&task->consumer);
	MT_sema_destroy(&task->producer);
	GDKfree(task);
#ifdef MLOCK_TST
	munlockall();
#endif

	/* restore system setting */
	GDK_vm_trim = vmtrim;
	return res < 0 ? BUN_NONE : cnt;

  bailout:
	if (task) {
		for (i = 0; i < as->nr_attrs; i++) {
			if (task->fields[i])
				GDKfree(task->fields[i]);
		}
		if (task->fields)
			GDKfree(task->fields);
		if (task->time)
			GDKfree(task->time);
		if (task->cols)
			GDKfree(task->cols);
		if (task->base)
			GDKfree(task->base);
		GDKfree(task);
	}
	for (i = 0; i < MAXWORKERS; i++)
		if (ptask[i].cols)
			GDKfree(ptask[i].cols);
#ifdef MLOCK_TST
	munlockall();
#endif
	/* restore system setting */
	GDK_vm_trim = vmtrim;
	return BUN_NONE;
}

#undef _DEBUG_TABLET_
