/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
 * file into chunks of distinct rows. Then multiple parallel threads
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
#include "mapi_prompt.h"
#include "mal_internal.h"

#include <string.h>
#include <ctype.h>

#define MAXWORKERS	64
#define MAXBUFFERS 2
/* We restrict the row length to be 32MB for the time being */
#define MAXROWSIZE(X) (X > 32*1024*1024 ? X : 32*1024*1024)

static MT_Lock errorlock = MT_LOCK_INITIALIZER(errorlock);

static BAT *
void_bat_create(int adt, BUN nr)
{
	BAT *b = COLnew(0, adt, nr, TRANSIENT);

	/* check for correct structures */
	if (b == NULL)
		return NULL;
	if ((b = BATsetaccess(b, BAT_APPEND)) == NULL) {
		return NULL;
	}

	/* disable all properties here */
	b->tsorted = false;
	b->trevsorted = false;
	b->tnosorted = 0;
	b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
	return b;
}

void
TABLETdestroy_format(Tablet *as)
{
	BUN p;
	Column *fmt = as->format;

	for (p = 0; p < as->nr_attrs; p++) {
		BBPreclaim(fmt[p].c);
		if (fmt[p].data)
			GDKfree(fmt[p].data);
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

	if (as->nr != cnt) {
		for (i = 0; i < as->nr_attrs; i++)
			if (fmt[i].c)
				fmt[i].p = as->offset;
		return oid_nil;
	}

	for (i = 0; i < as->nr_attrs; i++) {
		BAT *b = fmt[i].c;

		if (b == NULL)
			continue;

		if (BATcount(b) != cnt || b->hseqbase != base)
			return oid_nil;

		fmt[i].p = as->offset;
	}
	return base;
}

str
TABLETcreate_bats(Tablet *as, BUN est)
{
	Column *fmt = as->format;
	BUN i, nr = 0;

	for (i = 0; i < as->nr_attrs; i++) {
		if (fmt[i].skip)
			continue;
		fmt[i].c = void_bat_create(fmt[i].adt, est);
		if (!fmt[i].c) {
			while (i > 0) {
				if (!fmt[--i].skip) {
					BBPreclaim(fmt[i].c);
					fmt[i].c = NULL;
				}
			}
			throw(SQL, "copy", "Failed to create bat of size " BUNFMT "\n",
				  est);
		}
		fmt[i].ci = bat_iterator_nolock(fmt[i].c);
		nr++;
	}
	if (!nr)
		throw(SQL, "copy",
			  "At least one column should be read from the input\n");
	return MAL_SUCCEED;
}

str
TABLETcollect(BAT **bats, Tablet *as)
{
	Column *fmt = as->format;
	BUN i, j;
	BUN cnt = 0;

	if (bats == NULL)
		throw(SQL, "copy", "Missing container");
	for (i = 0; i < as->nr_attrs && !cnt; i++)
		if (!fmt[i].skip)
			cnt = BATcount(fmt[i].c);
	for (i = 0, j = 0; i < as->nr_attrs; i++) {
		if (fmt[i].skip)
			continue;
		bats[j] = fmt[i].c;
		BBPfix(bats[j]->batCacheid);
		if ((fmt[i].c = BATsetaccess(fmt[i].c, BAT_READ)) == NULL)
			throw(SQL, "copy",
				  "Failed to set access at tablet part " BUNFMT "\n", cnt);
		fmt[i].c->tsorted = fmt[i].c->trevsorted = false;
		fmt[i].c->tkey = false;
		BATsettrivprop(fmt[i].c);

		if (cnt != BATcount(fmt[i].c))
			throw(SQL, "copy", "Count " BUNFMT " differs from " BUNFMT "\n",
				  BATcount(fmt[i].c), cnt);
		j++;
	}
	return MAL_SUCCEED;
}

// the starting quote character has already been skipped

static char *
tablet_skip_string(char *s, char quote, bool escape)
{
	size_t i = 0, j = 0;
	while (s[i]) {
		if (escape && s[i] == '\\' && s[i + 1] != '\0')
			s[j++] = s[i++];
		else if (s[i] == quote) {
			if (s[i + 1] != quote)
				break;
			i++;				/* skip the first quote */
		}
		s[j++] = s[i++];
	}
	assert(s[i] == quote || s[i] == '\0');
	if (s[i] == 0)
		return NULL;
	s[j] = 0;
	return s + i;
}

static int
TABLET_error(stream *s)
{
	const char *err = mnstr_peek_error(s);
	if (err)
		TRC_ERROR(MAL_SERVER, "Stream error: %s\n", err);
	return -1;
}

/* The output line is first built before being sent. It solves a problem
   with UDP, where you may loose most of the information using short writes
*/
static inline int
output_line(char **buf, size_t *len, char **localbuf, size_t *locallen,
			Column *fmt, stream *fd, BUN nr_attrs, oid id)
{
	BUN i;
	ssize_t fill = 0;

	for (i = 0; i < nr_attrs; i++) {
		if (fmt[i].c == NULL)
			continue;
		if (id < fmt[i].c->hseqbase
			|| id >= fmt[i].c->hseqbase + BATcount(fmt[i].c))
			break;
		fmt[i].p = id - fmt[i].c->hseqbase;
	}
	if (i == nr_attrs) {
		for (i = 0; i < nr_attrs; i++) {
			Column *f = fmt + i;
			const char *p;
			ssize_t l;

			if (f->c) {
				p = BUNtail(f->ci, f->p);

				if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
					p = f->nullstr;
					l = (ssize_t) strlen(f->nullstr);
				} else {
					l = f->tostr(f->extra, localbuf, locallen, f->adt, p);
					if (l < 0)
						return -1;
					p = *localbuf;
				}
				if (fill + l + f->seplen >= (ssize_t) * len) {
					/* extend the buffer */
					char *nbuf;
					nbuf = GDKrealloc(*buf, fill + l + f->seplen + BUFSIZ);
					if (nbuf == NULL)
						return -1;	/* *buf freed by caller */
					*buf = nbuf;
					*len = fill + l + f->seplen + BUFSIZ;
				}
				strncpy(*buf + fill, p, l);
				fill += l;
			}
			strncpy(*buf + fill, f->sep, f->seplen);
			fill += f->seplen;
		}
	}
	if (fd && mnstr_write(fd, *buf, 1, fill) != fill)
		return TABLET_error(fd);
	return 0;
}

static inline int
output_line_dense(char **buf, size_t *len, char **localbuf, size_t *locallen,
				  Column *fmt, stream *fd, BUN nr_attrs)
{
	BUN i;
	ssize_t fill = 0;

	for (i = 0; i < nr_attrs; i++) {
		Column *f = fmt + i;
		const char *p;
		ssize_t l;

		if (f->c) {
			p = BUNtail(f->ci, f->p);

			if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
				p = f->nullstr;
				l = (ssize_t) strlen(p);
			} else {
				l = f->tostr(f->extra, localbuf, locallen, f->adt, p);
				if (l < 0)
					return -1;
				p = *localbuf;
			}
			if (fill + l + f->seplen >= (ssize_t) * len) {
				/* extend the buffer */
				char *nbuf;
				nbuf = GDKrealloc(*buf, fill + l + f->seplen + BUFSIZ);
				if (nbuf == NULL)
					return -1;	/* *buf freed by caller */
				*buf = nbuf;
				*len = fill + l + f->seplen + BUFSIZ;
			}
			strncpy(*buf + fill, p, l);
			fill += l;
			f->p++;
		}
		strncpy(*buf + fill, f->sep, f->seplen);
		fill += f->seplen;
	}
	if (fd && mnstr_write(fd, *buf, 1, fill) != fill)
		return TABLET_error(fd);
	return 0;
}

static inline int
output_line_lookup(char **buf, size_t *len, Column *fmt, stream *fd,
				   BUN nr_attrs, oid id)
{
	BUN i;

	for (i = 0; i < nr_attrs; i++) {
		Column *f = fmt + i;

		if (f->c) {
			const void *p = BUNtail(f->ci, id - f->c->hseqbase);

			if (!p || ATOMcmp(f->adt, ATOMnilptr(f->adt), p) == 0) {
				size_t l = strlen(f->nullstr);
				if (mnstr_write(fd, f->nullstr, 1, l) != (ssize_t) l)
					return TABLET_error(fd);
			} else {
				ssize_t l = f->tostr(f->extra, buf, len, f->adt, p);

				if (l < 0 || mnstr_write(fd, *buf, 1, l) != l)
					return TABLET_error(fd);
			}
		}
		if (mnstr_write(fd, f->sep, 1, f->seplen) != f->seplen)
			return TABLET_error(fd);
	}
	return 0;
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
 * To simplify our world, we assume a single producer process.
 */

static int
output_file_default(Tablet *as, BAT *order, stream *fd, bstream *in)
{
	size_t len = BUFSIZ, locallen = BUFSIZ;
	int res = 0;
	char *buf = GDKmalloc(len);
	char *localbuf = GDKmalloc(len);
	BUN p, q;
	oid id;
	BUN offset = as->offset;

	if (buf == NULL || localbuf == NULL) {
		GDKfree(buf);
		GDKfree(localbuf);
		return -1;
	}
	for (q = offset + as->nr, p = offset, id = order->hseqbase + offset; p < q;
		 p++, id++) {
		if (((p - offset) & 8191) == 8191 && bstream_getoob(in)) {
			res = -5;
			break;
		}
		if ((res = output_line(&buf, &len, &localbuf, &locallen, as->format, fd, as->nr_attrs, id)) < 0) {
			break;
		}
	}
	GDKfree(localbuf);
	GDKfree(buf);
	return res;
}

static int
output_file_dense(Tablet *as, stream *fd, bstream *in)
{
	size_t len = BUFSIZ, locallen = BUFSIZ;
	int res = 0;
	char *buf = GDKmalloc(len);
	char *localbuf = GDKmalloc(len);
	BUN i = 0;

	if (buf == NULL || localbuf == NULL) {
		GDKfree(buf);
		GDKfree(localbuf);
		return -1;
	}
	for (i = 0; i < as->nr; i++) {
		if ((i & 8191) == 8191 && bstream_getoob(in)) {
			res = -5;			/* "Query aborted" */
			break;
		}
		if ((res = output_line_dense(&buf, &len, &localbuf, &locallen, as->format, fd, as->nr_attrs)) < 0) {
			break;
		}
	}
	GDKfree(localbuf);
	GDKfree(buf);
	return res;
}

static int
output_file_ordered(Tablet *as, BAT *order, stream *fd, bstream *in)
{
	size_t len = BUFSIZ;
	int res = 0;
	char *buf = GDKmalloc(len);
	BUN p, q;
	BUN i = 0;
	BUN offset = as->offset;

	if (buf == NULL)
		return -1;
	for (q = offset + as->nr, p = offset; p < q; p++, i++) {
		oid h = order->hseqbase + p;

		if (((p - offset) & 8191) == 8191 && bstream_getoob(in)) {
			res = -5;
			break;
		}
		if ((res = output_line_lookup(&buf, &len, as->format, fd, as->nr_attrs, h)) < 0) {
			GDKfree(buf);
			return res;
		}
	}
	GDKfree(buf);
	return res;
}

int
TABLEToutput_file(Tablet *as, BAT *order, stream *s, bstream *in)
{
	oid base = oid_nil;
	int ret = 0;

	/* only set nr if it is zero or lower (bogus) to the maximum value
	 * possible (BATcount), if already set within BATcount range,
	 * preserve value such that for instance SQL's reply_size still
	 * works
	 */
	if (order) {
		BUN maxnr = BATcount(order);
		if (as->nr == BUN_NONE || as->nr > maxnr)
			as->nr = maxnr;
	}
	assert(as->nr != BUN_NONE);

	base = check_BATs(as);
	if (!order || !is_oid_nil(base)) {
		if (!order || order->hseqbase == base)
			ret = output_file_dense(as, s, in);
		else
			ret = output_file_ordered(as, order, s, in);
	} else {
		ret = output_file_default(as, order, s, in);
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
 * file into chunks of distinct rows. Then multiple parallel threads
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

#define BREAKROW 1
#define UPDATEBAT 2
#define ENDOFCOPY 3

typedef struct {
	Client cntxt;
	int id;						/* for self reference */
	int state;					/* row break=1 , 2 = update bat */
	int workers;				/* how many concurrent ones */
	int error;					/* error during row break */
	int next;
	int limit;
	BUN cnt, maxrow;			/* first row in file chunk. */
	lng skip;					/* number of lines to be skipped */
	lng *time, wtime;			/* time per col + time per thread */
	int rounds;					/* how often did we divide the work */
	bool ateof;					/* io control */
	bool from_stdin;
	bool escape;				/* whether to handle \ escapes */
	bool besteffort;
	char quote;
	bstream *b;
	stream *out;
	MT_Id tid;
	MT_Sema producer;			/* reader waits for call */
	MT_Sema consumer;			/* reader waits for call */
	MT_Sema sema;				/* threads wait for work , negative next implies exit */
	MT_Sema reply;				/* let reader continue */
	Tablet *as;
	char *errbuf;
	const char *csep, *rsep;
	size_t seplen, rseplen;

	char *base[MAXBUFFERS], *input[MAXBUFFERS];	/* buffers for row splitter and tokenizer */
	size_t rowlimit[MAXBUFFERS];	/* determines maximal record length buffer */
	char **rows[MAXBUFFERS];
	lng *startlineno[MAXBUFFERS];
	int top[MAXBUFFERS];		/* number of rows in this buffer */
	int cur;					/* current buffer used by splitter and update threads */

	int *cols;					/* columns to handle */
	char ***fields;
	bte *rowerror;
	int errorcnt;
	bool aborted;
	bool set_qry_ctx;
} READERtask;

/* returns TRUE if there is/might be more */
static bool
tablet_read_more(READERtask *task)
{
	bstream *in = task->b;
	stream *out = task->out;
	size_t n =  task->b->size;
	if (out) {
		do {
			/* query is not finished ask for more */
			/* we need more query text */
			if (bstream_next(in) < 0) {
				if (mnstr_errnr(in->s) == MNSTR_INTERRUPT) {
					task->aborted = true;
					mnstr_clearerr(in->s);
				}
				return false;
			}
			if (in->eof) {
				if (bstream_getoob(in)) {
					task->aborted = true;
					return false;
				}
				if (mnstr_write(out, PROMPT2, sizeof(PROMPT2) - 1, 1) == 1)
					mnstr_flush(out, MNSTR_FLUSH_DATA);
				in->eof = false;
				/* we need more query text */
				if (bstream_next(in) < 0) {
					if (mnstr_errnr(in->s) == MNSTR_INTERRUPT) {
						task->aborted = true;
						mnstr_clearerr(in->s);
					}
					return false;
				}
				if (in->eof)
					return false;
			}
		} while (in->len <= in->pos);
	} else if (bstream_read(in, n) <= 0) {
		return false;
	}
	return true;
}

/* note, the column value that is passed here is the 0 based value; the
 * lineno value on the other hand is 1 based */
static void
tablet_error(READERtask *task, lng idx, lng lineno, int col, const char *msg,
			 const char *fcn)
{
	assert(is_int_nil(col) || col >= 0);
	assert(is_lng_nil(lineno) || lineno >= 1);
	MT_lock_set(&errorlock);
	if (task->cntxt->error_row != NULL
		&& (BUNappend(task->cntxt->error_row, &lineno, false) != GDK_SUCCEED
			|| BUNappend(task->cntxt->error_fld, &(int) { col + 1 },
						 false) != GDK_SUCCEED
			|| BUNappend(task->cntxt->error_msg, msg, false) != GDK_SUCCEED
			|| BUNappend(task->cntxt->error_input, fcn,
						 false) != GDK_SUCCEED)) {
		task->besteffort = false;
	}
	if (!is_lng_nil(idx) && task->rowerror && idx < task->limit)
		task->rowerror[idx]++;
	if (task->as->error == NULL) {
		const char *colnam = is_int_nil(col) || col < 0
				|| (BUN) col >= task->as->nr_attrs ? NULL : task->as->format[col].name;
		if (msg == NULL) {
			task->besteffort = false;
		} else if (!is_lng_nil(lineno)) {
			if (!is_int_nil(col)) {
				if (colnam)
					task->as->error = createException(MAL, "sql.copy_from",
													  "line " LLFMT ": column %d %s: %s",
													  lineno, col + 1, colnam, msg);
				else
					task->as->error = createException(MAL, "sql.copy_from",
													  "line " LLFMT ": column %d: %s",
													  lineno, col + 1, msg);
			} else {
				task->as->error = createException(MAL, "sql.copy_from",
												  "line " LLFMT ": %s", lineno, msg);
			}
		} else {
			if (!is_int_nil(col)) {
				if (colnam)
					task->as->error = createException(MAL, "sql.copy_from",
													  "column %d %s: %s", col + 1, colnam,
													  msg);
				else
					task->as->error = createException(MAL, "sql.copy_from",
													  "column %d: %s", col + 1, msg);
			} else {
				task->as->error = createException(MAL, "sql.copy_from", "%s", msg);
			}
		}
	}
	task->errorcnt++;
	MT_lock_unset(&errorlock);
}

/*
 * The row is broken into pieces directly on their field separators. It assumes that we have
 * the record in the cache already, so we can do most work quickly.
 * Furthermore, it assume a uniform (SQL) pattern, without whitespace skipping, but with quote and separator.
 */

static size_t
mystrlen(const char *s)
{
	/* Calculate and return the space that is needed for the function
	 * mycpstr below to do its work. */
	size_t len = 0;
	const char *s0 = s;

	while (*s) {
		if ((*s & 0x80) == 0) {
			;
		} else if ((*s & 0xC0) == 0x80) {
			/* continuation byte */
			len += 3;
		} else if ((*s & 0xE0) == 0xC0) {
			/* two-byte sequence */
			if ((s[1] & 0xC0) != 0x80)
				len += 3;
			else
				s += 2;
		} else if ((*s & 0xF0) == 0xE0) {
			/* three-byte sequence */
			if ((s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80)
				len += 3;
			else
				s += 3;
		} else if ((*s & 0xF8) == 0xF0) {
			/* four-byte sequence */
			if ((s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80
				|| (s[3] & 0xC0) != 0x80)
				len += 3;
			else
				s += 4;
		} else {
			/* not a valid start byte */
			len += 3;
		}
		s++;
	}
	len += s - s0;
	return len;
}

static char *
mycpstr(char *t, const char *s)
{
	/* Copy the string pointed to by s into the buffer pointed to by
	 * t, and return a pointer to the NULL byte at the end.  During
	 * the copy we translate incorrect UTF-8 sequences to escapes
	 * looking like <XX> where XX is the hexadecimal representation of
	 * the incorrect byte.  The buffer t needs to be large enough to
	 * hold the result, but the correct length can be calculated by
	 * the function mystrlen above.*/
	while (*s) {
		if ((*s & 0x80) == 0) {
			*t++ = *s++;
		} else if ((*s & 0xC0) == 0x80) {
			t += sprintf(t, "<%02X>", (uint8_t) * s++);
		} else if ((*s & 0xE0) == 0xC0) {
			/* two-byte sequence */
			if ((s[1] & 0xC0) != 0x80)
				t += sprintf(t, "<%02X>", (uint8_t) * s++);
			else {
				*t++ = *s++;
				*t++ = *s++;
			}
		} else if ((*s & 0xF0) == 0xE0) {
			/* three-byte sequence */
			if ((s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80)
				t += sprintf(t, "<%02X>", (uint8_t) * s++);
			else {
				*t++ = *s++;
				*t++ = *s++;
				*t++ = *s++;
			}
		} else if ((*s & 0xF8) == 0xF0) {
			/* four-byte sequence */
			if ((s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80
				|| (s[3] & 0xC0) != 0x80)
				t += sprintf(t, "<%02X>", (uint8_t) * s++);
			else {
				*t++ = *s++;
				*t++ = *s++;
				*t++ = *s++;
				*t++ = *s++;
			}
		} else {
			/* not a valid start byte */
			t += sprintf(t, "<%02X>", (uint8_t) * s++);
		}
	}
	*t = 0;
	return t;
}

static str
SQLload_error(READERtask *task, lng idx, BUN attrs)
{
	str line;
	char *s;
	size_t sz = 0;
	BUN i;

	for (i = 0; i < attrs; i++) {
		if (task->fields[i][idx])
			sz += mystrlen(task->fields[i][idx]);
		sz += task->seplen;
	}

	s = line = GDKmalloc(sz + task->rseplen + 1);
	if (line == NULL) {
		tablet_error(task, idx, lng_nil, int_nil, "SQLload malloc error",
					 "SQLload_error");
		return NULL;
	}
	for (i = 0; i < attrs; i++) {
		if (task->fields[i][idx])
			s = mycpstr(s, task->fields[i][idx]);
		if (i < attrs - 1)
			s = mycpstr(s, task->csep);
	}
	strcpy(s, task->rsep);
	return line;
}

/*
 * The parsing of the individual values is straightforward. If the value represents
 * the null-replacement string then we grab the underlying nil.
 * If the string starts with the quote identified from SQL, we locate the tail
 * and interpret the body.
 *
 * If inserting fails, we return -1; if the value cannot be parsed, we
 * return -1 if besteffort is not set, otherwise we return 0, but in
 * either case an entry is added to the error table.
 */
static inline int
SQLinsert_val(READERtask *task, int col, int idx)
{
	Column *fmt = task->as->format + col;
	const void *adt;
	char buf[BUFSIZ];
	char *s = task->fields[col][idx];
	char *err = NULL;
	int ret = 0;

	/* include testing on the terminating null byte !! */
	if (s == NULL) {
		adt = fmt->nildata;
		fmt->c->tnonil = false;
	} else {
		if (task->escape) {
			size_t slen = strlen(s) + 1;
			char *data = slen <= sizeof(buf) ? buf : GDKmalloc(strlen(s) + 1);
			if (data == NULL
				|| GDKstrFromStr((unsigned char *) data, (unsigned char *) s,
								 strlen(s), '\0') < 0)
				adt = NULL;
			else
				adt = fmt->frstr(fmt, fmt->adt, data);
			if (data != buf)
				GDKfree(data);
		} else
			adt = fmt->frstr(fmt, fmt->adt, s);
	}

	lng row = task->cnt + idx + 1;
	if (adt == NULL) {
		if (task->rowerror) {
			err = SQLload_error(task, idx, task->as->nr_attrs);
			if (s) {
				size_t slen = mystrlen(s);
				char *scpy = GDKmalloc(slen + 1);
				if (scpy == NULL) {
					tablet_error(task, idx, row, col,
								 SQLSTATE(HY013) MAL_MALLOC_FAIL, err);
					task->besteffort = false;	/* no longer best effort */
					GDKfree(err);
					return -1;
				}
				mycpstr(scpy, s);
				s = scpy;
			}
			snprintf(buf, sizeof(buf), "'%s' expected%s%s%s", fmt->type,
					 s ? " in '" : "", s ? s : "", s ? "'" : "");
			GDKfree(s);
			tablet_error(task, idx, row, col, buf, err);
			GDKfree(err);
			if (!task->besteffort)
				return -1;
		}
		ret = -!task->besteffort;	/* yep, two unary operators ;-) */
		/* replace it with a nil */
		adt = fmt->nildata;
		fmt->c->tnonil = false;
	}
	if (bunfastapp(fmt->c, adt) == GDK_SUCCEED)
		return ret;

	/* failure */
	if (task->rowerror) {
		char *msg = GDKerrbuf;
		err = SQLload_error(task, idx, task->as->nr_attrs);
		tablet_error(task, idx, row, col, msg
					 && *msg ? msg : "insert failed", err);
		GDKfree(err);
	}
	task->besteffort = false;	/* no longer best effort */
	return -1;
}

static int
SQLworker_column(READERtask *task, int col)
{
	int i;
	Column *fmt = task->as->format;

	if (fmt[col].c == NULL)
		return 0;

	/* watch out for concurrent threads */
	MT_lock_set(&mal_copyLock);
	if (!fmt[col].skip
		&& BATcapacity(fmt[col].c) < BATcount(fmt[col].c) + task->next) {
		if (BATextend(fmt[col].c, BATgrows(fmt[col].c) + task->limit) != GDK_SUCCEED) {
			tablet_error(task, lng_nil, lng_nil, col,
						 "Failed to extend the BAT\n", "SQLworker_column");
			MT_lock_unset(&mal_copyLock);
			return -1;
		}
	}
	MT_lock_unset(&mal_copyLock);

	for (i = 0; i < task->top[task->cur]; i++) {
		if (!fmt[col].skip && SQLinsert_val(task, col, i) < 0) {
			BATsetcount(fmt[col].c, BATcount(fmt[col].c));
			return -1;
		}
	}
	BATsetcount(fmt[col].c, BATcount(fmt[col].c));

	return 0;
}

/*
 * The rows are broken on the column separator. Any error is shown and reflected with
 * setting the reference of the offending row fields to NULL.
 * This allows the loading to continue, skipping the minimal number of rows.
 * The details about the locations can be inspected from the error table.
 * We also trim the quotes around strings.
 */
static int
SQLload_parse_row(READERtask *task, int idx)
{
	BUN i;
	char errmsg[BUFSIZ];
	char ch = *task->csep;
	char *row = task->rows[task->cur][idx];
	lng startlineno = task->startlineno[task->cur][idx];
	Tablet *as = task->as;
	Column *fmt = as->format;
	bool error = false;
	str errline = NULL;

	assert(idx < task->top[task->cur]);
	assert(row);
	errmsg[0] = 0;

	if (task->quote || task->seplen != 1) {
		for (i = 0; i < as->nr_attrs; i++) {
			bool quote = false;
			task->fields[i][idx] = row;
			/* recognize fields starting with a quote, keep them */
			if (*row && *row == task->quote) {
				quote = true;
				task->fields[i][idx] = row + 1;
				row = tablet_skip_string(row + 1, task->quote, task->escape);

				if (!row) {
					errline = SQLload_error(task, idx, i + 1);
					snprintf(errmsg, BUFSIZ, "Quote (%c) missing", task->quote);
					tablet_error(task, idx, startlineno, (int) i, errmsg,
								 errline);
					GDKfree(errline);
					error = true;
					goto errors1;
				} else
					*row++ = 0;
			}

			/* eat away the column separator */
			for (; *row; row++)
				if (*row == '\\' && task->escape) {
					if (row[1])
						row++;
				} else if (*row == ch
						   && (task->seplen == 1
							   || strncmp(row, task->csep,
										  task->seplen) == 0)) {
					*row = 0;
					row += task->seplen;
					goto endoffieldcheck;
				}

			/* not enough fields */
			if (i < as->nr_attrs - 1) {
				errline = SQLload_error(task, idx, i + 1);
				/* it's the next value that is missing */
				tablet_error(task, idx, startlineno, (int) i + 1,
							 "Column value missing", errline);
				GDKfree(errline);
				error = true;
  errors1:
				/* we save all errors detected  as NULL values */
				for (; i < as->nr_attrs; i++)
					task->fields[i][idx] = NULL;
				i--;
			}
  endoffieldcheck:
			;
			/* check for user defined NULL string */
			if ((!quote || !fmt->null_length) && fmt->nullstr
				&& task->fields[i][idx]
				&& GDKstrcasecmp(task->fields[i][idx], fmt->nullstr) == 0)
				task->fields[i][idx] = 0;
		}
	} else {
		assert(!task->quote);
		assert(task->seplen == 1);
		for (i = 0; i < as->nr_attrs; i++) {
			task->fields[i][idx] = row;

			/* eat away the column separator */
			for (; *row; row++)
				if (*row == '\\' && task->escape) {
					if (row[1])
						row++;
				} else if (*row == ch) {
					*row = 0;
					row++;
					goto endoffield2;
				}

			/* not enough fields */
			if (i < as->nr_attrs - 1) {
				errline = SQLload_error(task, idx, i + 1);
				/* it's the next value that is missing */
				tablet_error(task, idx, startlineno, (int) i + 1,
							 "Column value missing", errline);
				GDKfree(errline);
				error = true;
				/* we save all errors detected */
				for (; i < as->nr_attrs; i++)
					task->fields[i][idx] = NULL;
				i--;
			}
  endoffield2:
			;
			/* check for user defined NULL string */
			if (fmt->nullstr && task->fields[i][idx]
				&& GDKstrcasecmp(task->fields[i][idx], fmt->nullstr) == 0) {
				task->fields[i][idx] = 0;
			}
		}
	}
	/* check for too many values as well */
	if (row && *row && i == as->nr_attrs) {
		errline = SQLload_error(task, idx, task->as->nr_attrs);
		snprintf(errmsg, BUFSIZ, "Leftover data '%s'", row);
		tablet_error(task, idx, startlineno, (int) i, errmsg, errline);
		GDKfree(errline);
		error = true;
	}
	return error ? -1 : 0;
}

static void
SQLworker(void *arg)
{
	READERtask *task = (READERtask *) arg;
	unsigned int i;
	int j, piece;
	lng t0;

	GDKsetbuf(GDKmalloc(GDKMAXERRLEN));	/* where to leave errors */
	GDKclrerr();
	task->errbuf = GDKerrbuf;
	MT_thread_set_qry_ctx(task->set_qry_ctx ? &task->cntxt->qryctx : NULL);

	MT_sema_down(&task->sema);
	while (task->top[task->cur] >= 0) {
		/* stage one, break the rows spread the work over the workers */
		switch (task->state) {
		case BREAKROW:
			t0 = GDKusec();
			piece = (task->top[task->cur] + task->workers) / task->workers;

			for (j = piece * task->id;
				 j < task->top[task->cur] && j < piece * (task->id + 1); j++)
				if (task->rows[task->cur][j]) {
					if (SQLload_parse_row(task, j) < 0) {
						task->errorcnt++;
						// early break unless best effort
						if (!task->besteffort) {
							for (j++;
								 j < task->top[task->cur]
								 && j < piece * (task->id + 1); j++)
								for (i = 0; i < task->as->nr_attrs; i++)
									task->fields[i][j] = NULL;
							break;
						}
					}
				}
			task->wtime = GDKusec() - t0;
			break;
		case UPDATEBAT:
			if (!task->besteffort && task->errorcnt)
				break;
			/* stage two, updating the BATs */
			for (i = 0; i < task->as->nr_attrs; i++)
				if (task->cols[i]) {
					t0 = GDKusec();
					if (SQLworker_column(task, task->cols[i] - 1) < 0)
						break;
					t0 = GDKusec() - t0;
					task->time[i] += t0;
					task->wtime += t0;
				}
			break;
		case ENDOFCOPY:
			MT_sema_up(&task->reply);
			goto do_return;
		}
		MT_sema_up(&task->reply);
		MT_sema_down(&task->sema);
	}
	MT_sema_up(&task->reply);

  do_return:
	GDKfree(GDKerrbuf);
	GDKsetbuf(NULL);
	MT_thread_set_qry_ctx(NULL);
}

static void
SQLworkdivider(READERtask *task, READERtask *ptask, int nr_attrs, int threads)
{
	int i, j, mi;
	lng loc[MAXWORKERS];

	/* after a few rounds we stick to the work assignment */
	if (task->rounds > 8)
		return;
	/* simple round robin the first time */
	if (threads == 1 || task->rounds++ == 0) {
		for (i = j = 0; i < nr_attrs; i++, j++)
			ptask[j % threads].cols[i] = task->cols[i];
		return;
	}
	memset(loc, 0, sizeof(loc));
	/* use of load directives */
	for (i = 0; i < nr_attrs; i++)
		for (j = 0; j < threads; j++)
			ptask[j].cols[i] = 0;

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
}

/*
 * Reading is handled by a separate task as a preparation for more parallelism.
 * A buffer is filled with proper rows.
 * If we are reading from a file then a double buffering scheme ia activated.
 * Reading from the console (stdin) remains single buffered only.
 * If we end up with unfinished records, then the rowlimit will terminate the process.
 */

typedef unsigned char (*dfa_t)[256];

static dfa_t
mkdfa(const unsigned char *sep, size_t seplen)
{
	dfa_t dfa;
	size_t i, j, k;

	dfa = GDKzalloc(seplen * sizeof(*dfa));
	if (dfa == NULL)
		return NULL;
	/* Each character in the separator string advances the state by
	 * one.  If state reaches seplen, the separator was recognized.
	 *
	 * The first loop and the nested loop make sure that if in any
	 * state we encounter an invalid character, but part of what we've
	 * matched so far is a prefix of the separator, we go to the
	 * appropriate state. */
	for (i = 0; i < seplen; i++)
		dfa[i][sep[0]] = 1;
	for (j = 0; j < seplen; j++) {
		dfa[j][sep[j]] = (unsigned char) (j + 1);
		for (k = 0; k < j; k++) {
			for (i = 0; i < j - k; i++)
				if (sep[k + i] != sep[i])
					break;
			if (i == j - k && dfa[j][sep[i]] <= i)
				dfa[j][sep[i]] = (unsigned char) (i + 1);
		}
	}
	return dfa;
}

#ifdef __has_builtin
#if __has_builtin(__builtin_expect)
/* __builtin_expect returns its first argument; it is expected to be
 * equal to the second argument */
#define unlikely(expr)	__builtin_expect((expr) != 0, 0)
#define likely(expr)	__builtin_expect((expr) != 0, 1)
#endif
#endif
#ifndef unlikely
#ifdef _MSC_VER
#define unlikely(expr)	(__assume(!(expr)), (expr))
#define likely(expr)	(__assume((expr)), (expr))
#else
#define unlikely(expr)	(expr)
#define likely(expr)	(expr)
#endif
#endif

static void
SQLproducer(void *p)
{
	READERtask *task = (READERtask *) p;
	bool consoleinput = false;
	int cur = 0;				// buffer being filled
	bool blocked[MAXBUFFERS] = { false };
	bool ateof[MAXBUFFERS] = { false };
	BUN cnt = 0, bufcnt[MAXBUFFERS] = { 0 };
	char *end = NULL, *e = NULL, *s = NULL, *base;
	const char *rsep = task->rsep;
	size_t rseplen = strlen(rsep), partial = 0;
	char quote = task->quote;
	dfa_t rdfa;
	lng rowno = 0;
	lng lineno = 1;
	lng startlineno = 1;
	int more = 0;

	MT_sema_down(&task->producer);
	if (task->id < 0) {
		return;
	}

	MT_thread_set_qry_ctx(task->set_qry_ctx ? &task->cntxt->qryctx : NULL);
	rdfa = mkdfa((const unsigned char *) rsep, rseplen);
	if (rdfa == NULL) {
		tablet_error(task, lng_nil, lng_nil, int_nil, "cannot allocate memory",
					 "");
		ateof[cur] = true;
		goto reportlackofinput;
	}

/*	TRC_DEBUG(MAL_SERVER, "SQLproducer started size '%zu' and len '%zu'\n", task->b->size, task->b->len);*/

	base = end = s = task->input[cur];
	*s = 0;
	task->cur = cur;
	if (task->as->filename == NULL) {
		consoleinput = true;
		goto parseSTDIN;
	}
	for (;;) {
		startlineno = lineno;
		ateof[cur] = !tablet_read_more(task);

		// we may be reading from standard input and may be out of input
		// warn the consumers
		if (task->aborted || ((lineno & 8191) == 0 && bstream_getoob(task->cntxt->fdin))) {
			tablet_error(task, rowno, lineno, int_nil,
						 "problem reported by client", s);
			ateof[cur] = true;
			goto reportlackofinput;
		}

		if (ateof[cur] && partial) {
			if (unlikely(partial)) {
				tablet_error(task, rowno, lineno, int_nil,
							 "incomplete record at end of file", s);
				task->b->pos += partial;
			}
			goto reportlackofinput;
		}

		if (task->errbuf && task->errbuf[0]) {
			if (unlikely(GDKerrbuf && GDKerrbuf[0])) {
				tablet_error(task, rowno, lineno, int_nil, GDKerrbuf,
							 "SQLload_file");
/*				TRC_DEBUG(MAL_SERVER, "Bailout on SQLload\n");*/
				ateof[cur] = true;
				break;
			}
		}

  parseSTDIN:

		/* copy the stream buffer into the input buffer, which is guaranteed larger, but still limited */
		partial = 0;
		task->top[cur] = 0;
		s = task->input[cur];
		base = end;
		/* avoid too long records */
		if (unlikely(end - s + task->b->len - task->b->pos >= task->rowlimit[cur])) {
			/* the input buffer should be extended, but 'base' is not shared
			   between the threads, which we can not now update.
			   Mimic an ateof instead; */
			tablet_error(task, rowno, lineno, int_nil, "record too long", "");
			ateof[cur] = true;
/*			TRC_DEBUG(MAL_SERVER, "Bailout on SQLload confronted with too large record\n");*/
			goto reportlackofinput;
		}
		memcpy(end, task->b->buf + task->b->pos, task->b->len - task->b->pos);
		end = end + task->b->len - task->b->pos;
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
		if (task->from_stdin && *s == '\n' && task->maxrow == BUN_MAX) {
			ateof[cur] = true;
			goto reportlackofinput;
		}
		for (e = s; *e && e < end && cnt < task->maxrow;) {
			/* tokenize the record completely
			 *
			 * The format of the input should comply to the following
			 * grammar rule [ [[quote][[esc]char]*[quote]csep]*rsep]*
			 * where quote is a single user-defined character.
			 * Within the quoted fields a character may be escaped
			 * with a backslash.  The correct number of fields should
			 * be supplied.  In the first phase we simply break the
			 * rows at the record boundary. */
			int nutf = 0;
			int m = 0;
			bool bs = false;
			char q = 0;
			size_t i = 0;
			while (*e) {
				if (task->skip > 0) {
					/* no interpretation of data we're skipping, just
					 * look for newline */
					if (*e == '\n') {
						lineno++;
						break;
					}
				} else {
					/* check for correctly encoded UTF-8 */
					if (nutf > 0) {
						if (unlikely((*e & 0xC0) != 0x80))
							goto badutf8;
						if (unlikely(m != 0 && (*e & m) == 0))
							goto badutf8;
						m = 0;
						nutf--;
					} else if ((*e & 0x80) != 0) {
						if ((*e & 0xE0) == 0xC0) {
							nutf = 1;
							if (unlikely((e[0] & 0x1E) == 0))
								goto badutf8;
						} else if ((*e & 0xF0) == 0xE0) {
							nutf = 2;
							if ((e[0] & 0x0F) == 0)
								m = 0x20;
						} else if (likely((*e & 0xF8) == 0xF0)) {
							nutf = 3;
							if ((e[0] & 0x07) == 0)
								m = 0x30;
						} else {
							goto badutf8;
						}
					} else if (*e == '\n')
						lineno++;
					/* check for quoting and the row separator */
					if (bs) {
						bs = false;
					} else if (task->escape && *e == '\\') {
						bs = true;
						i = 0;
					} else if (*e == q) {
						q = 0;
					} else if (*e == quote) {
						q = quote;
						i = 0;
					} else if (q == 0) {
						i = rdfa[i][(unsigned char) *e];
						if (i == rseplen)
							break;
					}
				}
				e++;
			}
			if (*e == 0) {
				partial = e - s;
				/* found an incomplete record, saved for next round */
				if (unlikely(s + partial < end)) {
					/* found a EOS in the input */
					tablet_error(task, rowno, startlineno, int_nil,
								 "record too long (EOS found)", "");
					ateof[cur] = true;
					goto reportlackofinput;
				}
				break;
			} else {
				rowno++;
				if (task->skip > 0) {
					task->skip--;
				} else {
					if (cnt < task->maxrow) {
						task->startlineno[cur][task->top[cur]] = startlineno;
						task->rows[cur][task->top[cur]++] = s;
						startlineno = lineno;
						cnt++;
					}
					*(e + 1 - rseplen) = 0;
				}
				s = ++e;
				task->b->pos += (size_t) (e - base);
				base = e;
				if (task->top[cur] == task->limit)
					break;
			}
		}

  reportlackofinput:
/*	  TRC_DEBUG(MAL_SERVER, "SQL producer got buffer '%d' filled with '%d' records\n", cur, task->top[cur]);*/

		if (consoleinput) {
			task->cur = cur;
			task->ateof = ateof[cur];
			task->cnt = bufcnt[cur];
			/* tell consumer to go ahead */
			MT_sema_up(&task->consumer);
			/* then wait until it is done */
			MT_sema_down(&task->producer);
			if (cnt == task->maxrow) {
				GDKfree(rdfa);
				MT_thread_set_qry_ctx(NULL);
				return;
			}
		} else {
			assert(!blocked[cur]);
			if (blocked[(cur + 1) % MAXBUFFERS]) {
				/* first wait until other buffer is done */
/*				TRC_DEBUG(MAL_SERVER, "Wait for consumers to finish buffer: %d\n", (cur + 1) % MAXBUFFERS);*/

				MT_sema_down(&task->producer);
				blocked[(cur + 1) % MAXBUFFERS] = false;
				if (task->state == ENDOFCOPY) {
					GDKfree(rdfa);
					MT_thread_set_qry_ctx(NULL);
					return;
				}
			}
			/* other buffer is done, proceed with current buffer */
			assert(!blocked[(cur + 1) % MAXBUFFERS]);
			blocked[cur] = true;
			task->cur = cur;
			task->ateof = ateof[cur];
			task->cnt = bufcnt[cur];
			more = !ateof[cur] || (e && e < end
								   && task->top[cur] == task->limit);
/*			TRC_DEBUG(MAL_SERVER, "SQL producer got buffer '%d' filled with '%d' records\n", cur, task->top[cur]);*/

			MT_sema_up(&task->consumer);

			cur = (cur + 1) % MAXBUFFERS;
/*			TRC_DEBUG(MAL_SERVER, "May continue with buffer: %d\n", cur);*/

			if (cnt == task->maxrow) {
				MT_sema_down(&task->producer);
/*				TRC_DEBUG(MAL_SERVER, "Producer delivered all\n");*/
				GDKfree(rdfa);
				MT_thread_set_qry_ctx(NULL);
				return;
			}
		}
/*		TRC_DEBUG(MAL_SERVER, "Continue producer buffer: %d\n", cur);*/

		/* we ran out of input? */
		if (task->ateof && !more) {
/*			TRC_DEBUG(MAL_SERVER, "Producer encountered eof\n");*/
			GDKfree(rdfa);
			MT_thread_set_qry_ctx(NULL);
			return;
		}
		/* consumers ask us to stop? */
		if (task->state == ENDOFCOPY) {
			GDKfree(rdfa);
			MT_thread_set_qry_ctx(NULL);
			return;
		}
		bufcnt[cur] = cnt;
		/* move the non-parsed correct row data to the head of the next buffer */
		end = s = task->input[cur];
	}
	if (unlikely(cnt < task->maxrow && task->maxrow != BUN_NONE)) {
		char msg[256];
		snprintf(msg, sizeof(msg), "incomplete record at end of file:%s\n", s);
		task->as->error = GDKstrdup(msg);
		tablet_error(task, rowno, startlineno, int_nil,
					 "incomplete record at end of file", s);
		task->b->pos += partial;
	}
	GDKfree(rdfa);
	MT_thread_set_qry_ctx(NULL);

	return;

  badutf8:
	tablet_error(task, rowno, startlineno, int_nil,
				 "input not properly encoded UTF-8", "");
	ateof[cur] = true;
	goto reportlackofinput;
}

static void
create_rejects_table(Client cntxt)
{
	MT_lock_set(&mal_contextLock);
	if (cntxt->error_row == NULL) {
		cntxt->error_row = COLnew(0, TYPE_lng, 0, TRANSIENT);
		cntxt->error_fld = COLnew(0, TYPE_int, 0, TRANSIENT);
		cntxt->error_msg = COLnew(0, TYPE_str, 0, TRANSIENT);
		cntxt->error_input = COLnew(0, TYPE_str, 0, TRANSIENT);
		if (cntxt->error_row == NULL || cntxt->error_fld == NULL
			|| cntxt->error_msg == NULL || cntxt->error_input == NULL) {
			BBPreclaim(cntxt->error_row);
			BBPreclaim(cntxt->error_fld);
			BBPreclaim(cntxt->error_msg);
			BBPreclaim(cntxt->error_input);
			cntxt->error_row = cntxt->error_fld = cntxt->error_msg = cntxt->error_input = NULL;
		}
	}
	MT_lock_unset(&mal_contextLock);
}

BUN
SQLload_file(Client cntxt, Tablet *as, bstream *b, stream *out,
			 const char *csep, const char *rsep, char quote, lng skip,
			 lng maxrow, int best, bool from_stdin, const char *tabnam,
			 bool escape)
{
	BUN cnt = 0, cntstart = 0, leftover = 0;
	int res = 0;				/* < 0: error, > 0: success, == 0: continue processing */
	int j;
	BUN firstcol;
	BUN i, attr;
	READERtask task;
	READERtask ptask[MAXWORKERS];
	int threads = 1;
	lng tio, t1 = 0;
	char name[MT_NAME_LEN];

	if (maxrow < 0 || maxrow > (LL_CONSTANT(1) << 16)) {
		threads = GDKgetenv_int("tablet_threads", GDKnr_threads);
		if (threads > 1)
			threads = threads < MAXWORKERS ? threads - 1 : MAXWORKERS - 1;
		else
			threads = 1;
	}

/*	TRC_DEBUG(MAL_SERVER, "Prepare copy work for '%d' threads col '%s' rec '%s' quot '%c'\n", threads, csep, rsep, quote);*/

	memset(ptask, 0, sizeof(ptask));
	task = (READERtask) {
		.cntxt = cntxt,
		.from_stdin = from_stdin,
		.as = as,
		.escape = escape,	/* TODO: implement feature!!! */
		.set_qry_ctx = MT_thread_get_qry_ctx() != NULL,
	};

	/* create the reject tables */
	create_rejects_table(task.cntxt);
	if (task.cntxt->error_row == NULL || task.cntxt->error_fld == NULL
		|| task.cntxt->error_msg == NULL || task.cntxt->error_input == NULL) {
		tablet_error(&task, lng_nil, lng_nil, int_nil,
					 "SQLload initialization failed", "");
		/* nothing allocated yet, so nothing to free */
		return BUN_NONE;
	}

	assert(rsep);
	assert(csep);
	assert(maxrow < 0 || maxrow <= (lng) BUN_MAX);
	task.fields = (char ***) GDKzalloc(as->nr_attrs * sizeof(char **));
	task.cols = (int *) GDKzalloc(as->nr_attrs * sizeof(int));
	task.time = (lng *) GDKzalloc(as->nr_attrs * sizeof(lng));
	if (task.fields == NULL || task.cols == NULL || task.time == NULL) {
		tablet_error(&task, lng_nil, lng_nil, int_nil,
					 "memory allocation failed", "SQLload_file");
		goto bailout;
	}
	task.cur = 0;
	for (i = 0; i < MAXBUFFERS; i++) {
		task.base[i] = GDKmalloc(MAXROWSIZE(2 * b->size) + 2);
		task.rowlimit[i] = MAXROWSIZE(2 * b->size);
		if (task.base[i] == NULL) {
			tablet_error(&task, lng_nil, lng_nil, int_nil,
						 SQLSTATE(HY013) MAL_MALLOC_FAIL, "SQLload_file");
			goto bailout;
		}
		task.base[i][0] = task.base[i][b->size + 1] = 0;
		task.input[i] = task.base[i] + 1;	/* wrap the buffer with null bytes */
	}
	task.besteffort = best;

	if (maxrow < 0)
		task.maxrow = BUN_MAX;
	else
		task.maxrow = (BUN) maxrow;

	task.skip = skip;
	task.quote = quote;
	task.csep = csep;
	task.seplen = strlen(csep);
	task.rsep = rsep;
	task.rseplen = strlen(rsep);
	task.errbuf = cntxt->errbuf;

	MT_sema_init(&task.producer, 0, "task.producer");
	MT_sema_init(&task.consumer, 0, "task.consumer");
	task.ateof = false;
	task.b = b;
	task.out = out;

	as->error = NULL;

	/* there is no point in creating more threads than we have columns */
	if (as->nr_attrs < (BUN) threads)
		threads = (int) as->nr_attrs;

	/* allocate enough space for pointers into the buffer pool.  */
	/* the record separator is considered a column */
	task.limit = (int) (b->size / as->nr_attrs + as->nr_attrs);
	for (i = 0; i < as->nr_attrs; i++) {
		task.fields[i] = GDKmalloc(sizeof(char *) * task.limit);
		if (task.fields[i] == NULL) {
			if (task.as->error == NULL)
				as->error = createException(MAL, "sql.copy_from",
											SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto bailout;
		}
		task.cols[i] = (int) (i + 1);	/* to distinguish non initialized later with zero */
	}
	for (i = 0; i < MAXBUFFERS; i++) {
		task.rows[i] = GDKzalloc(sizeof(char *) * task.limit);
		task.startlineno[i] = GDKzalloc(sizeof(lng) * task.limit);
		if (task.rows[i] == NULL || task.startlineno[i] == NULL) {
			GDKfree(task.rows[i]);
			GDKfree(task.startlineno[i]);
			tablet_error(&task, lng_nil, lng_nil, int_nil,
						 SQLSTATE(HY013) MAL_MALLOC_FAIL,
						 "SQLload_file:failed to alloc buffers");
			goto bailout;
		}
	}
	task.rowerror = (bte *) GDKzalloc(sizeof(bte) * task.limit);
	if (task.rowerror == NULL) {
		tablet_error(&task, lng_nil, lng_nil, int_nil,
					 SQLSTATE(HY013) MAL_MALLOC_FAIL,
					 "SQLload_file:failed to alloc rowerror buffer");
		goto bailout;
	}

	task.id = 0;
	snprintf(name, sizeof(name), "prod-%s", tabnam);
	if (MT_create_thread(&task.tid, SQLproducer, (void *) &task, MT_THR_JOINABLE, name) < 0) {
		tablet_error(&task, lng_nil, lng_nil, int_nil,
					 SQLSTATE(42000) "failed to start producer thread",
					 "SQLload_file");
		goto bailout;
	}
/*	TRC_DEBUG(MAL_SERVER, "Parallel bulk load " LLFMT " - " BUNFMT "\n", skip, task.maxrow);*/

	task.workers = threads;
	for (j = 0; j < threads; j++) {
		ptask[j] = task;
		ptask[j].id = j;
		ptask[j].cols = (int *) GDKzalloc(as->nr_attrs * sizeof(int));
		if (ptask[j].cols == NULL) {
			tablet_error(&task, lng_nil, lng_nil, int_nil,
						 SQLSTATE(HY013) MAL_MALLOC_FAIL, "SQLload_file");
			task.id = -1;
			MT_sema_up(&task.producer);
			goto bailout;
		}
		snprintf(name, sizeof(name), "ptask%d.sema", j);
		MT_sema_init(&ptask[j].sema, 0, name);
		snprintf(name, sizeof(name), "ptask%d.repl", j);
		MT_sema_init(&ptask[j].reply, 0, name);
		snprintf(name, sizeof(name), "wrkr%d-%s", j, tabnam);
		if (MT_create_thread(&ptask[j].tid, SQLworker, (void *) &ptask[j], MT_THR_JOINABLE, name) < 0) {
			tablet_error(&task, lng_nil, lng_nil, int_nil,
						 SQLSTATE(42000) "failed to start worker thread",
						 "SQLload_file");
			threads = j;
			for (j = 0; j < threads; j++)
				ptask[j].workers = threads;
		}
	}
	if (threads == 0) {
		/* no threads started */
		task.id = -1;
		MT_sema_up(&task.producer);
		goto bailout;
	}
	MT_sema_up(&task.producer);

	tio = GDKusec();
	tio = GDKusec() - tio;
	t1 = GDKusec();
	for (firstcol = 0; firstcol < task.as->nr_attrs; firstcol++)
		if (task.as->format[firstcol].c != NULL)
			break;
	while (res == 0 && cnt < task.maxrow) {

		// track how many elements are in the aggregated BATs
		cntstart = BATcount(task.as->format[firstcol].c);
		/* block until the producer has data available */
		MT_sema_down(&task.consumer);
		cnt += task.top[task.cur];
		if (task.ateof && !task.top[task.cur])
			break;
		t1 = GDKusec() - t1;
/*		TRC_DEBUG(MAL_SERVER, "Break: %d rows\n", task.top[task.cur]);*/

		t1 = GDKusec();
		if (task.top[task.cur]) {
			/* activate the workers to break rows */
			for (j = 0; j < threads; j++) {
				/* stage one, break the rows in parallel */
				ptask[j].error = 0;
				ptask[j].state = BREAKROW;
				ptask[j].next = task.top[task.cur];
				ptask[j].fields = task.fields;
				ptask[j].limit = task.limit;
				ptask[j].cnt = task.cnt;
				ptask[j].cur = task.cur;
				ptask[j].top[task.cur] = task.top[task.cur];
				MT_sema_up(&ptask[j].sema);
			}
		}
		if (task.top[task.cur]) {
			/* await completion of row break phase */
			for (j = 0; j < threads; j++) {
				MT_sema_down(&ptask[j].reply);
				if (ptask[j].error) {
					res = -1;
/*					TRC_ERROR(MAL_SERVER, "Error in task: %d %d\n", j, ptask[j].error);*/
				}
			}
		}

/*		TRC_DEBUG(MAL_SERVER,
			"Fill the BATs '%d' " BUNFMT " cap " BUNFMT "\n",
			task.top[task.cur], task.cnt, BATcapacity(as->format[task.cur].c));*/

		if (task.top[task.cur]) {
			if (res == 0) {
				SQLworkdivider(&task, ptask, (int) as->nr_attrs, threads);

				/* activate the workers to update the BATs */
				for (j = 0; j < threads; j++) {
					/* stage two, update the BATs */
					ptask[j].state = UPDATEBAT;
					MT_sema_up(&ptask[j].sema);
				}
			}
		}
		tio = GDKusec();
		tio = t1 - tio;

		/* await completion of the BAT updates */
		if (res == 0 && task.top[task.cur]) {
			for (j = 0; j < threads; j++) {
				MT_sema_down(&ptask[j].reply);
				if (ptask[j].errorcnt > 0 && !ptask[j].besteffort) {
					res = -1;
					best = 0;
				}
			}
		}

		/* trim the BATs discarding error tuples */
#define trimerrors(TYPE)												\
		do {															\
			TYPE *src, *dst;											\
			leftover= BATcount(task.as->format[attr].c);				\
			limit = leftover - cntstart;								\
			dst =src= (TYPE *) BUNtloc(task.as->format[attr].ci,cntstart); \
			for(j = 0; j < (int) limit; j++, src++){					\
				if ( task.rowerror[j]){									\
					leftover--;											\
					continue;											\
				}														\
				*dst++ = *src;											\
			}															\
			BATsetcount(task.as->format[attr].c, leftover );			\
		} while (0)

/*		TRC_DEBUG(MAL_SERVER, "Trim bbest '%d' table size " BUNFMT " - rows found so far " BUNFMT "\n",
					 best, BATcount(as->format[firstcol].c), task.cnt); */

		if (best && BATcount(as->format[firstcol].c)) {
			BUN limit;
			int width;

			for (attr = 0; attr < as->nr_attrs; attr++) {
				if (as->format[attr].skip)
					continue;
				width = as->format[attr].c->twidth;
				as->format[attr].ci = bat_iterator_nolock(as->format[attr].c);
				switch (width) {
				case 1:
					trimerrors(bte);
					break;
				case 2:
					trimerrors(sht);
					break;
				case 4:
					trimerrors(int);
					break;
				case 8:
					trimerrors(lng);
					break;
#ifdef HAVE_HGE
				case 16:
					trimerrors(hge);
					break;
#endif
				default:
				{
					char *src, *dst;
					leftover = BATcount(task.as->format[attr].c);
					limit = leftover - cntstart;
					dst = src = BUNtloc(task.as->format[attr].ci, cntstart);
					for (j = 0; j < (int) limit; j++, src += width) {
						if (task.rowerror[j]) {
							leftover--;
							continue;
						}
						if (dst != src)
							memcpy(dst, src, width);
						dst += width;
					}
					BATsetcount(task.as->format[attr].c, leftover);
				}
					break;
				}
			}
			// re-initialize the error vector;
			memset(task.rowerror, 0, task.limit);
			task.errorcnt = 0;
		}

		if (res < 0) {
			/* producer should stop */
			task.maxrow = cnt;
			task.state = ENDOFCOPY;
			task.ateof = true;
		}
		if (task.ateof && task.top[task.cur] < task.limit && cnt != task.maxrow)
			break;
		task.top[task.cur] = 0;
		if (cnt == task.maxrow)
			task.ateof = true;
		MT_sema_up(&task.producer);
	}

/*	TRC_DEBUG(MAL_SERVER, "End of block stream eof=%d - res=%d\n", task.ateof, res);*/

	cnt = BATcount(task.as->format[firstcol].c);

	task.state = ENDOFCOPY;
/*	TRC_DEBUG(MAL_SERVER, "Activate sync on disk\n");*/

	if (!task.ateof || cnt < task.maxrow) {
/*		TRC_DEBUG(MAL_SERVER, "Shut down reader\n");*/
		MT_sema_up(&task.producer);
	}
	MT_join_thread(task.tid);

/*	TRC_DEBUG(MAL_SERVER, "Activate endofcopy\n");*/

	for (j = 0; j < threads; j++) {
		ptask[j].state = ENDOFCOPY;
		MT_sema_up(&ptask[j].sema);
	}
	/* wait for their death */
	for (j = 0; j < threads; j++)
		MT_sema_down(&ptask[j].reply);

/*	TRC_DEBUG(MAL_SERVER, "Kill the workers\n");*/

	for (j = 0; j < threads; j++) {
		MT_join_thread(ptask[j].tid);
		GDKfree(ptask[j].cols);
		MT_sema_destroy(&ptask[j].sema);
		MT_sema_destroy(&ptask[j].reply);
	}

/*	TRC_DEBUG(MAL_SERVER, "Found " BUNFMT " tuples\n", cnt);*/
/*	TRC_DEBUG(MAL_SERVER, "Leftover input: %.63s\n", task.b->buf + task.b->pos);*/

	for (i = 0; i < as->nr_attrs; i++) {
		BAT *b = task.as->format[i].c;
		if (b)
			BATsettrivprop(b);
		GDKfree(task.fields[i]);
	}
	GDKfree(task.fields);
	GDKfree(task.cols);
	GDKfree(task.time);
	for (i = 0; i < MAXBUFFERS; i++) {
		GDKfree(task.base[i]);
		GDKfree(task.rows[i]);
		GDKfree(task.startlineno[i]);
	}
	if (task.rowerror)
		GDKfree(task.rowerror);
	MT_sema_destroy(&task.producer);
	MT_sema_destroy(&task.consumer);

	return res < 0 ? BUN_NONE : cnt;

  bailout:
	if (task.fields) {
		for (i = 0; i < as->nr_attrs; i++)
			GDKfree(task.fields[i]);
		GDKfree(task.fields);
	}
	GDKfree(task.time);
	GDKfree(task.cols);
	GDKfree(task.base[task.cur]);
	GDKfree(task.rowerror);
	for (i = 0; i < MAXWORKERS; i++)
		GDKfree(ptask[i].cols);
	return BUN_NONE;
}

/* return the latest reject table */
str
COPYrejects(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *row = getArgReference_bat(stk, pci, 0);
	bat *fld = getArgReference_bat(stk, pci, 1);
	bat *msg = getArgReference_bat(stk, pci, 2);
	bat *inp = getArgReference_bat(stk, pci, 3);

	create_rejects_table(cntxt);
	if (cntxt->error_row == NULL)
		throw(MAL, "sql.rejects", "No reject table available");
	MT_lock_set(&errorlock);
	BAT *bn1 = COLcopy(cntxt->error_row, cntxt->error_row->ttype, true, TRANSIENT);
	BAT *bn2 = COLcopy(cntxt->error_fld, cntxt->error_fld->ttype, true, TRANSIENT);
	BAT *bn3 = COLcopy(cntxt->error_msg, cntxt->error_msg->ttype, true, TRANSIENT);
	BAT *bn4 = COLcopy(cntxt->error_input, cntxt->error_input->ttype, true, TRANSIENT);
	MT_lock_unset(&errorlock);
	if (bn1 == NULL || bn2 == NULL || bn3 == NULL || bn4 == NULL) {
		BBPreclaim(bn1);
		BBPreclaim(bn2);
		BBPreclaim(bn3);
		BBPreclaim(bn4);
		throw(MAL, "sql.rejects", GDK_EXCEPTION);
	}
	*row = bn1->batCacheid;
	*fld = bn2->batCacheid;
	*msg = bn3->batCacheid;
	*inp = bn4->batCacheid;
	BBPkeepref(bn1);
	BBPkeepref(bn2);
	BBPkeepref(bn3);
	BBPkeepref(bn4);
	(void) mb;
	return MAL_SUCCEED;
}

str
COPYrejects_clear(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	if (cntxt->error_row) {
		MT_lock_set(&errorlock);
		BATclear(cntxt->error_row, true);
		if (cntxt->error_fld)
			BATclear(cntxt->error_fld, true);
		if (cntxt->error_msg)
			BATclear(cntxt->error_msg, true);
		if (cntxt->error_input)
			BATclear(cntxt->error_input, true);
		MT_lock_unset(&errorlock);
	}
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}
