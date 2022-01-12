/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "tablet.h"

#include <string.h>

static oid
check_BATs(OutputTable *as)
{
	OutputColumn *fmt = as->format;
	BUN i = 0;
	BUN cnt;
	oid base;

	if (fmt[i].c == NULL)
		i++;
	cnt = BATcount(fmt[i].c);
	base = fmt[i].c->hseqbase;

	if (as->nr != cnt)
		return oid_nil;

	for (i = 0; i < as->nr_attrs; i++) {
		BAT *b;
		BUN offset;

		b = fmt[i].c;
		if (b == NULL)
			continue;
		offset = as->offset;

		if (BATcount(b) != cnt || b->hseqbase != base)
			return oid_nil;

		fmt[i].p = offset;
	}
	return base;
}

static int
TABLET_error(stream *s)
{
	char *err = mnstr_error(s);
	/* use free as stream allocates outside GDK */
	if (err)
		free(err);
	return -1;
}

/* The output line is first built before being sent. It solves a problem
   with UDP, where you may loose most of the information using short writes
*/
static inline int
output_line(char **buf, size_t *len, char **localbuf, size_t *locallen, OutputColumn *fmt, stream *fd, BUN nr_attrs, oid id)
{
	BUN i;
	ssize_t fill = 0;

	for (i = 0; i < nr_attrs; i++) {
		if (fmt[i].c == NULL)
			continue;
		if (id < fmt[i].c->hseqbase || id >= fmt[i].c->hseqbase + BATcount(fmt[i].c))
			break;
		fmt[i].p = id - fmt[i].c->hseqbase;
	}
	if (i == nr_attrs) {
		for (i = 0; i < nr_attrs; i++) {
			OutputColumn *f = fmt + i;
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
				if (fill + l + f->seplen >= (ssize_t) *len) {
					/* extend the buffer */
					char *nbuf;
					nbuf = GDKrealloc(*buf, fill + l + f->seplen + BUFSIZ);
					if( nbuf == NULL)
						return -1; /* *buf freed by caller */
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
output_line_dense(char **buf, size_t *len, char **localbuf, size_t *locallen, OutputColumn *fmt, stream *fd, BUN nr_attrs)
{
	BUN i;
	ssize_t fill = 0;

	for (i = 0; i < nr_attrs; i++) {
		OutputColumn *f = fmt + i;
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
			if (fill + l + f->seplen >= (ssize_t) *len) {
				/* extend the buffer */
				char *nbuf;
				nbuf = GDKrealloc(*buf, fill + l + f->seplen + BUFSIZ);
				if( nbuf == NULL)
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
output_line_lookup(char **buf, size_t *len, OutputColumn *fmt, stream *fd, BUN nr_attrs, oid id)
{
	BUN i;

	for (i = 0; i < nr_attrs; i++) {
		OutputColumn *f = fmt + i;

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

static int
output_file_default(OutputTable *as, BAT *order, stream *fd)
{
	size_t len = BUFSIZ, locallen = BUFSIZ;
	int res = 0;
	char *buf = GDKmalloc(len);
	char *localbuf = GDKmalloc(len);
	BUN p, q;
	oid id;
	BUN i = 0;
	BUN offset = as->offset;

	if (buf == NULL || localbuf == NULL) {
		GDKfree(buf);
		GDKfree(localbuf);
		return -1;
	}
	for (q = offset + as->nr, p = offset, id = order->hseqbase + offset; p < q; p++, id++) {
		if ((res = output_line(&buf, &len, &localbuf, &locallen, as->format, fd, as->nr_attrs, id)) < 0) {
			GDKfree(buf);
			GDKfree(localbuf);
			return res;
		}
		i++;
	}
	GDKfree(localbuf);
	GDKfree(buf);
	return res;
}

static int
output_file_dense(OutputTable *as, stream *fd)
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
		if ((res = output_line_dense(&buf, &len, &localbuf, &locallen, as->format, fd, as->nr_attrs)) < 0) {
			GDKfree(buf);
			GDKfree(localbuf);
			return res;
		}
	}
	GDKfree(localbuf);
	GDKfree(buf);
	return res;
}

static int
output_file_ordered(OutputTable *as, BAT *order, stream *fd)
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

		if ((res = output_line_lookup(&buf, &len, as->format, fd, as->nr_attrs, h)) < 0) {
			GDKfree(buf);
			return res;
		}
	}
	GDKfree(buf);
	return res;
}

int
TABLEToutput_file(OutputTable *as, BAT *order, stream *s)
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

	base = check_BATs(as);
	if (!is_oid_nil(base)) {
		if (order->hseqbase == base)
			ret = output_file_dense(as, s);
		else
			ret = output_file_ordered(as, order, s);
	} else {
		ret = output_file_default(as, order, s);
	}
	return ret;
}

void
TABLETdestroy_outputformat(OutputTable *as)
{
	BUN p;
	OutputColumn *fmt = as->format;

	for (p = 0; p < as->nr_attrs; p++) {
		if (fmt[p].c)
			BBPunfix(fmt[p].c->batCacheid);
	}
	GDKfree(fmt);
}
