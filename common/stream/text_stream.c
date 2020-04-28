/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"

/* When reading, text streams convert \r\n to \n regardless of operating system,
 * and they drop the leading UTF-8 BOM marker if found.
 * When writing on Windows, \n is translated back to \r\n.
 *
 * Currently, skipping the BOM happens when opening
 */

#define UTF8BOM		"\xEF\xBB\xBF"	/* UTF-8 encoding of Unicode BOM */
#define UTF8BOMLENGTH	3	/* length of above */


typedef struct text_stream_state {
	char putback_buf[UTF8BOMLENGTH];
	int putback_start;
	int putback_end;
} state;


static void
text_destroy(stream *s)
{
	if (s == NULL)
		return;

	free(s->stream_data.p);
	destroy_stream(s);
}

static ssize_t
text_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	return s->inner->read(s->inner, buf, elmsize, cnt);
}

static ssize_t
text_read_putback(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	state *st = (state*) s->stream_data.p;
	char *p = buf;
	size_t size = elmsize * cnt;
	size_t i;

	for (i = 0; i < size; i++) {
		if (st->putback_start == st->putback_end) {
			s->read = text_read;
			break;
		}
		*p++ = st->putback_buf[st->putback_start++];
	}

	return i;
}


static ssize_t
skip_bom(stream *s)
{
	state *st = (state*) s->stream_data.p;
	stream *inner = s->inner;

	ssize_t nread = mnstr_read(inner, st->putback_buf, 1, UTF8BOMLENGTH);
	if (nread < 0)
		return nread;

	if (nread == UTF8BOMLENGTH &&  memcmp(st->putback_buf, UTF8BOM, nread) == 0) {
		// Bingo! Skip it!
		s->isutf8 = true;
		return 3;
	}


	// We have consumed some bytes that have to be returned.
	// skip_bom left them in the putback_buf.
	// Switch to a read function that returns them.
	s->read = text_read_putback;
	st->putback_start = 0;
	st->putback_end = nread;

	return nread;
}


stream *
create_text_stream(stream *inner)
{
	state *st = malloc(sizeof(state));
	struct stream *s = create_wrapper_stream(NULL, inner);
	if (st == NULL)
		goto bail;
	if (s == NULL)
		goto bail;

	*st = (state) { .putback_start = 0, .putback_end = 0, };
	s->stream_data.p = st;

	s->binary = false;
	s->destroy = text_destroy;

	// We're still a no-op so we can just fall back to
	// whatever our inner is doing:
	//     bool isutf8;    /* known to be UTF-8 due to BOM */
	//     ssize_t (*read)(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt);
	//     ssize_t (*write)(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt);
	//     void (*close)(stream *s);
	//     void (*destroy)(stream *s);
	//     int (*flush)(stream *s);

	if (s->readonly)
		if (skip_bom(s) < 0)
			goto bail;
	return s;
bail:
	free(st);
	destroy_stream(s);
	return NULL;
}
