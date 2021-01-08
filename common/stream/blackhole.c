/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"



static ssize_t
stream_blackhole_write(stream *restrict s, const void *restrict buf, size_t elmsize, size_t cnt)
{
	(void) s;
	(void) buf;
	(void) elmsize;
	return (ssize_t) cnt;
}

static void
stream_blackhole_close(stream *s)
{
	(void) s;
	/* no resources to close */
}

stream *
stream_blackhole_create(void)
{
	stream *s;
	if ((s = create_stream("blackhole")) == NULL) {
		return NULL;
	}

	s->read = NULL;
	s->write = stream_blackhole_write;
	s->close = stream_blackhole_close;
	s->flush = NULL;
	s->readonly = false;
	return s;
}
