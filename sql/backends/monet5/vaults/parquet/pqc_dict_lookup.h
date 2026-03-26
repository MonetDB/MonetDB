/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

static int64_t
pqc_dict_lookup( pqc_creader_t *cr, void *output, int64_t nrows, int pos)
{
	uchar *data = (uchar*)cr->data;
	int nr_bits = cr->nr_bits;
	int64_t i = 0;
	bool mul8 = ((8/nr_bits)*nr_bits == 8);
	T *dst = output;
	uint32_t idx = cr->idx;
	uint32_t j = 0;
	int mask = (1<<nr_bits) -1;
	if (cr->remaining) {
		if (cr->is_rle) {
			for(; i < nrows && j < cr->remaining; j++, i++)
				dst[i] = ((T*)cr->dict)[idx];
		} else {
			int sh = idx;

			if (mul8) {
				for(; i < nrows && j < cr->remaining; j++, i++) {
					uchar v = data[pos];
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh = 0;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = ((T*)cr->dict)[idx];
				}
				if ((cr->remaining - j) == 0)
					pos += (sh)/8;
			} else if (nr_bits < 8) {
				for(; i < nrows && j < cr->remaining; j++, i++) {
					uchar v = data[pos];
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh -= 8;
						uchar v = data[pos];
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = ((T*)cr->dict)[idx];
				}
				if ((cr->remaining - j) == 0)
					pos += (sh)/8;
			} else if (nr_bits < 16) {
				for(; i < nrows && j < cr->remaining; j++, i++) {
					usht v = *(usht*)(data+pos);
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 16) {
						pos+=2;
						sh -= 16;
						usht v = *(usht*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = ((T*)cr->dict)[idx];
				}
				if ((cr->remaining - j) == 0)
					pos += (sh)/8;
			} else if (nr_bits < 32) {
				for(; i < nrows && j < cr->remaining; j++, i++) {
					uint v = *(uint*)(data+pos);
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 32) {
						pos+=4;
						sh -= 32;
						uint v = *(uint*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = ((T*)cr->dict)[idx];
				}
				if ((cr->remaining - j) == 0)
					pos += (sh)/8;
			}
			cr->idx = sh;
		}
		cr->remaining -= j;
	}
	for(; i<nrows; ) {
		uint32_t len = 0;
		pos += pqc_get_int32((char*)data+pos, &len);
		if (len & 1) {
			len>>=1;
			/* only 2 bits for now */
			int sh = 0;
			int64_t j = 0;
			int m = len*8;
			if (mul8) {
				for (; i < nrows && j < m; j++, i++) {
					uchar v = data[pos];
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh = 0;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = ((T*)cr->dict)[idx];
				}
				if (j < m) {
					cr->is_rle = false;
					cr->remaining = m - j;
					cr->idx = sh;
				} else if (sh) {
					pos++;
				}
			} else if (nr_bits < 8) {
				for (; i < nrows && j < m; j++, i++) {
					uchar v = data[pos];
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh -= 8;
						uchar v = data[pos];
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = ((T*)cr->dict)[idx];
				}
				if (j < m) {
					cr->is_rle = false;
					cr->remaining = m - j;
					cr->idx = sh;
				} else if (sh) {
					pos++;
				}
			} else if (nr_bits < 16) {
				for (; i < nrows && j < m; j++, i++) {
					usht v = *(usht*)(data+pos);
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 16) {
						pos+=2;
						sh -= 16;
						usht v = *(usht*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = ((T*)cr->dict)[idx];
				}
				if (j < m) {
					cr->is_rle = false;
					cr->remaining = m - j;
					cr->idx = sh;
				} else {
					pos+=(sh/8);
				}
			} else if (nr_bits < 32) {
				int64_t j = 0;
				int m = len*8;
				for (; i < nrows && j < m; j++, i++) {
					uint v = *(uint*)(data+pos);
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 32) {
						pos+=4;
						sh -= 32;
						uint v = *(uint*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = ((T*)cr->dict)[idx];
				}
				if (j < m) {
					cr->is_rle = false;
					cr->remaining = m - j;
					cr->idx = sh;
				} else {
					pos+=(sh/8);
				}
			}
		} else if (nr_bits <= 8) { /* rle */
			len>>=1;
			uchar idx = data[pos++];
			idx &= mask;
			uint32_t j = 0;
			for(; i < nrows && j < len; j++, i++)
				dst[i] = ((T*)cr->dict)[idx];
			if (j < len) {
				cr->is_rle = true;
				cr->remaining = len - j;
				cr->idx = idx;
			}
		} else if (nr_bits <= 16) { /* rle */
			len>>=1;
			usht idx = *(usht*)(data+pos);
			idx &= mask;
			uint32_t j = 0;
			pos += 2;
			for(; i < nrows && j < len; j++, i++)
				dst[i] = ((T*)cr->dict)[idx];
			if (j < len) {
				cr->is_rle = true;
				cr->remaining = len - j;
				cr->idx = idx;
			}
		} else if (nr_bits <= 24) { /* rle */
			len>>=1;
			uint idx = *(uint*)(data+pos);
			idx &= mask;
			uint32_t j = 0;
			pos += 3;
			for(; i < nrows && j < len; j++, i++)
				dst[i] = ((T*)cr->dict)[idx];
			if (j < len) {
				cr->is_rle = true;
				cr->remaining = len - j;
				cr->idx = idx;
			}
		}
		assert(nr_bits <= 24);
	}
	cr->pos = pos;
	return nrows;
}
