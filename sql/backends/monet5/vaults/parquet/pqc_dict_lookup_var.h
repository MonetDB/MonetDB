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
pqc_dict_lookup( pqc_creader_t *cr, void *output, int64_t nrows, int pos, size_t *ssize)
{
	uint8_t *data = (uint8_t*)cr->data;
	int nr_bits = cr->nr_bits;
	int64_t i = 0;
	int *offsets = (int*)(cr->dict + cr->dict_num_values * sizeof(char*));
	bool mul8 = nr_bits?((8/nr_bits)*nr_bits == 8):false;
	T *dst = output;
	T offset = (T)*ssize;
	uint32_t idx = cr->idx;
	uint32_t j = 0;
	if (cr->remaining) {
		if (cr->is_rle) {
			for(; i < nrows && j < cr->remaining; j++, i++)
				dst[i] = offset+offsets[idx];
		} else {
			int sh = idx;
			int mask = (1<<nr_bits) -1;

			if (mul8) {
				for(; i < nrows && j < cr->remaining; j++, i++) {
					uint8_t v = data[pos];
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh = 0;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
				}
				if ((cr->remaining - j) == 0)
					pos += (sh)/8;
			} else if (nr_bits < 8) {
				for(; i < nrows && j < cr->remaining; j++, i++) {
					uint8_t v = data[pos];
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh -= 8;
						uint8_t v = data[pos];
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
				}
				if ((cr->remaining - j) == 0)
					pos += (sh)/8;
			} else if (nr_bits < 16) {
				for(; i < nrows && j < cr->remaining; j++, i++) {
					uint16_t v = *(uint16_t*)(data+pos);
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 16) {
						pos+=2;
						sh -= 16;
						uint16_t v = *(uint16_t*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
				}
				if ((cr->remaining - j) == 0)
					pos += (sh)/8;
			} else if (nr_bits < 32) {
				for(; i < nrows && j < cr->remaining; j++, i++) {
					uint32_t v = *(uint32_t*)(data+pos);
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 32) {
						pos+=4;
						sh -= 32;
						uint32_t v = *(uint32_t*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					//assert(((int64_t*)cr->dict)[idx] < ((int64_t) 1 << 8*sizeof(T)));
					dst[i] = (T) ((int64_t*)cr->dict)[idx];
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
			uint32_t mask = (1<<nr_bits) -1;
			uint32_t m = len*8;
			if (nr_bits == 0) {
				for (; i < nrows && j < m; j++, i++) {
					dst[i] = offset+offsets[0];
				}
				if (j < m) {
					cr->is_rle = false;
					cr->remaining = m - j;
					cr->idx = 0;
				}
			} else if (mul8) {
				for (; i < nrows && j < m; j++, i++) {
					uint8_t v = data[pos];
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh = 0;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
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
					uint8_t v = data[pos];
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh -= 8;
						uint8_t v = data[pos];
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
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
					uint16_t v = *(uint16_t*)(data+pos);
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 16) {
						pos+=2;
						sh -= 16;
						uint16_t v = *(uint16_t*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
				}
				if (j < m) {
					cr->is_rle = false;
					cr->remaining = m - j;
					cr->idx = sh;
				} else {
					pos+=(sh/8);
				}
			} else if (nr_bits < 32) {
				for (; i < nrows && j < m; j++, i++) {
					uint32_t v = *(uint32_t*)(data+pos);
					uint32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 32) {
						pos+=4;
						sh -= 32;
						uint32_t v = *(uint32_t*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					//assert(((int64_t*)cr->dict)[idx] < ((int64_t) 1 << 8*sizeof(T)));
					dst[i] = (T) ((int64_t*)cr->dict)[idx];
				}
				if (j < m) {
					cr->is_rle = false;
					cr->remaining = m - j;
					cr->idx = sh;
				} else {
					pos+=(sh/8);
				}
			}
		} else if (nr_bits == 0) {
			len>>=1;
			for(; i < nrows && j < len; j++, i++)
				dst[i] = offset+offsets[0];
			if (j < len) {
				cr->is_rle = true;
				cr->remaining = len - j;
				cr->idx = idx;
			}
		} else if (nr_bits <= 8) { /* rle */
			len>>=1;
			uint8_t idx = data[pos++];
			for(; i < nrows && j < len; j++, i++)
				dst[i] = offset+offsets[idx];
			if (j < len) {
				cr->is_rle = true;
				cr->remaining = len - j;
				cr->idx = idx;
			}
		} else if (nr_bits <= 16) { /* rle */
			len>>=1;
			uint16_t idx = *(uint16_t*)(data+pos);
			pos += 2;
			for(; i < nrows && j < len; j++, i++)
				dst[i] = offset+offsets[idx];
			if (j < len) {
				cr->is_rle = true;
				cr->remaining = len - j;
				cr->idx = idx;
			}
		}
		j = 0;
	}
	cr->pos = pos;
	return nrows;
}
