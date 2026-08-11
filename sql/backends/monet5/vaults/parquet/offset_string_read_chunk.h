/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

static int64_t
offset_string_read_chunk( pqc_reader_t *r, pqc_creader_t *cr, T *output, char *voutput, int64_t nrows, int64_t pos, size_t offset)
{
	T *rc = output;
	char *buf = voutput;
	char *data = cr->data + pos;

	if (cr->cc->cur_page.stat.null_count) {
		memcpy(buf, r->nil, strlen(r->nil));
		buf += strlen(r->nil);
	}
	if (cr->cc->cur_page.stat.null_count == cr->cc->cur_page.num_values) { /* all null */
		assert(cr->cc->cur_page.num_nulls == cr->cc->cur_page.num_values);
       	for (int64_t i=0; i<nrows; i++)
			rc[i] = (T)offset;
		return nrows;
	}
	for (int64_t i=0; i<nrows; i++) {
		unsigned int len = get_uint32((uint8_t*)data);

		data += sizeof(int);
		memcpy(buf, data, len);
		buf[len] = 0;
		assert((int64_t) (offset + (buf-voutput)) < (INT64_C(1) << (8 * sizeof(T))));
		rc[i] = (T) (offset + (buf-voutput));
		buf += len+1;
		data += len;
	}
	cr->pos = (data - cr->data);
	return nrows;
}
