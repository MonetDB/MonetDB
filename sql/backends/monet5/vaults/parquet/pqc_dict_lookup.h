
static int64_t
pqc_dict_lookup( pqc_creader_t *cr, void *output, int64_t nrows, int pos, int *ssize)
{
	uchar *data = (uchar*)cr->data;
	int nr_bits = cr->nr_bits;
	int64_t i = 0;
	int *offsets = (int*)(cr->dict + cr->dict_num_values * sizeof(char*));
	bool mul8 = ((8/nr_bits)*nr_bits == 8);
	T *dst = output;
	T offset = *ssize;
	u_int32_t idx = cr->idx;
	if (cr->remaining) {
		u_int32_t j = 0;
		for(; i < nrows && j < cr->remaining; j++, i++)
			dst[i] = offset+offsets[idx];
		cr->remaining -= j;
	}
	for(; i<nrows; ) {
		u_int32_t len = 0;
		pos += pqc_get_int32((char*)data+pos, &len);
		if (len & 1) {
			len>>=1;
			/* only 2 bits for now */
			int sh = 0;
			int mask = (1<<nr_bits) -1;
			if (mul8) { /* todo handle 16 / 32 multiples */
				int m = len*8;
				for (int64_t j = 0; i < nrows && j < m; j++, i++) {
					uchar v = data[pos];
					u_int32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh = 0;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
				}
			} else if (nr_bits < 8) {
				int m = len*8;
				for (int64_t j = 0; i < nrows && j < m; j++, i++) {
					uchar v = data[pos];
					u_int32_t idx = (v >> sh)&mask;
					sh += nr_bits;
					if (sh >= 8) {
						pos++;
						sh -= 8;
						uchar v = data[pos];
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
					pos++;
				}
			} else if (nr_bits < 16) {
				int m = len*8;
				for (int64_t j = 0; i < nrows && j < m; j++, i++) {
					usht v = *(usht*)(data+pos);
					u_int32_t idx = (v >> sh)&mask;
					sh += nr_bits;
						if (sh >= 16) {
						pos+=2;
						sh -= 16;
						usht v = *(usht*)(data+pos);
						idx |= (v << (nr_bits-sh))&mask;
					}
					assert(idx < cr->dict_num_values);
					dst[i] = offset+offsets[idx];
				}
				pos += (sh/8);
			}
		} else if (nr_bits <= 8) { /* rle */
			len>>=1;
			uchar idx = data[pos++];
			u_int32_t j = 0;
			for(; i < nrows && j < len; j++, i++)
				dst[i] = offset+offsets[idx];
			if (j < len) {
				cr->remaining = len - j;
				cr->idx = idx;
			}
		} else if (nr_bits <= 16) { /* rle */
			len>>=1;
			usht idx = *(usht*)(data+pos);
			u_int32_t j = 0;
			pos += 2;
			for(; i < nrows && j < len; j++, i++)
				dst[i] = offset+offsets[idx];
			if (j < len) {
				cr->remaining = len - j;
				cr->idx = idx;
			}
		}
	}
	cr->pos = pos;
	return nrows;
}

