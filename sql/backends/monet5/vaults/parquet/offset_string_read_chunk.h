
static int
offset_string_read_chunk( pqc_creader_t *cr, T *output, char *voutput, int64_t nrows, int pos, int offset)
{
	T *rc = output;
	char *buf = voutput;
	char *data = cr->data + pos;

	memcpy(buf, "NULL", 5);
	if (cr->cc->cur_page.stat.null_count == cr->cc->cur_page.num_values) { /* all null */
		assert(cr->cc->cur_page.num_nulls == cr->cc->cur_page.num_values);
       	for (int64_t i=0; i<nrows; i++)
			rc[i] = offset;
		return nrows;
	}
	for (int64_t i=0; i<nrows; i++) {
		unsigned int len = get_uint32((uchar*)data);

		data += sizeof(int);
		memcpy(buf, data, len);
		buf[len] = 0;
		rc[i] = offset + (buf-voutput);
		buf += len+1;
		data += len;
	}
	cr->pos = (data - cr->data);
	return nrows;
}
