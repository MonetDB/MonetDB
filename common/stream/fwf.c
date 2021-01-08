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



/* fixed-width format streams */
#define STREAM_FWF_NAME "fwf_ftw"

typedef struct {
	stream *s;
	bool eof;
	/* config */
	size_t num_fields;
	size_t *widths;
	char filler;
	/* state */
	size_t line_len;
	char *in_buf;
	char *out_buf;
	size_t out_buf_start;
	size_t out_buf_remaining;
} stream_fwf_data;


static ssize_t
stream_fwf_read(stream *restrict s, void *restrict buf, size_t elmsize, size_t cnt)
{
	stream_fwf_data *fsd;
	size_t to_write = cnt;
	size_t buf_written = 0;
	char nl_buf;

	fsd = (stream_fwf_data *) s->stream_data.p;
	if (fsd == NULL || elmsize != 1) {
		return -1;
	}
	if (fsd->eof)
		return 0;

	while (to_write > 0) {
		/* input conversion */
		if (fsd->out_buf_remaining == 0) {	/* need to convert next line */
			size_t field_idx, in_buf_pos = 0, out_buf_pos = 0;
			ssize_t actually_read = fsd->s->read(fsd->s, fsd->in_buf, 1, fsd->line_len);
			if (actually_read < (ssize_t) fsd->line_len) {	/* incomplete last line */
				if (actually_read < 0) {
					return actually_read;	/* this is an error */
				}
				fsd->eof = true;
				return (ssize_t) buf_written;	/* skip last line */
			}
			/* consume to next newline */
			while (fsd->s->read(fsd->s, &nl_buf, 1, 1) == 1 &&
			       nl_buf != '\n')
				;

			for (field_idx = 0; field_idx < fsd->num_fields; field_idx++) {
				char *val_start, *val_end;
				val_start = fsd->in_buf + in_buf_pos;
				in_buf_pos += fsd->widths[field_idx];
				val_end = fsd->in_buf + in_buf_pos - 1;
				while (*val_start == fsd->filler)
					val_start++;
				while (*val_end == fsd->filler)
					val_end--;
				while (val_start <= val_end) {
					if (*val_start == STREAM_FWF_FIELD_SEP) {
						fsd->out_buf[out_buf_pos++] = STREAM_FWF_ESCAPE;
					}
					fsd->out_buf[out_buf_pos++] = *val_start++;
				}
				fsd->out_buf[out_buf_pos++] = STREAM_FWF_FIELD_SEP;
			}
			fsd->out_buf[out_buf_pos++] = STREAM_FWF_RECORD_SEP;
			fsd->out_buf_remaining = out_buf_pos;
			fsd->out_buf_start = 0;
		}
		/* now we know something is in output_buf so deliver it */
		if (fsd->out_buf_remaining <= to_write) {
			memcpy((char *) buf + buf_written, fsd->out_buf + fsd->out_buf_start, fsd->out_buf_remaining);
			to_write -= fsd->out_buf_remaining;
			buf_written += fsd->out_buf_remaining;
			fsd->out_buf_remaining = 0;
		} else {
			memcpy((char *) buf + buf_written, fsd->out_buf + fsd->out_buf_start, to_write);
			fsd->out_buf_start += to_write;
			fsd->out_buf_remaining -= to_write;
			buf_written += to_write;
			to_write = 0;
		}
	}
	return (ssize_t) buf_written;
}


static void
stream_fwf_close(stream *s)
{
	stream_fwf_data *fsd = (stream_fwf_data *) s->stream_data.p;

	if (fsd != NULL) {
		stream_fwf_data *fsd = (stream_fwf_data *) s->stream_data.p;
		close_stream(fsd->s);
		free(fsd->widths);
		free(fsd->in_buf);
		free(fsd->out_buf);
		free(fsd);
		s->stream_data.p = NULL;
	}
}

static void
stream_fwf_destroy(stream *s)
{
	stream_fwf_close(s);
	destroy_stream(s);
}

stream *
stream_fwf_create(stream *restrict s, size_t num_fields, size_t *restrict widths, char filler)
{
	stream *ns;
	stream_fwf_data *fsd = malloc(sizeof(stream_fwf_data));

	if (fsd == NULL) {
		mnstr_set_open_error(STREAM_FWF_NAME, errno, NULL);
		return NULL;
	}
	*fsd = (stream_fwf_data) {
		.s = s,
		.num_fields = num_fields,
		.widths = widths,
		.filler = filler,
		.line_len = 0,
		.eof = false,
	};
	for (size_t i = 0; i < num_fields; i++) {
		fsd->line_len += widths[i];
	}
	fsd->in_buf = malloc(fsd->line_len);
	if (fsd->in_buf == NULL) {
		close_stream(fsd->s);
		free(fsd);
		mnstr_set_open_error(STREAM_FWF_NAME, errno, NULL);
		return NULL;
	}
	fsd->out_buf = malloc(fsd->line_len * 3);
	if (fsd->out_buf == NULL) {
		close_stream(fsd->s);
		free(fsd->in_buf);
		free(fsd);
		mnstr_set_open_error(STREAM_FWF_NAME, errno, NULL);
		return NULL;
	}
	if ((ns = create_stream(STREAM_FWF_NAME)) == NULL) {
		close_stream(fsd->s);
		free(fsd->in_buf);
		free(fsd->out_buf);
		free(fsd);
		return NULL;
	}
	ns->read = stream_fwf_read;
	ns->close = stream_fwf_close;
	ns->destroy = stream_fwf_destroy;
	ns->write = NULL;
	ns->flush = NULL;
	ns->readonly = true;
	ns->stream_data.p = fsd;
	return ns;
}
