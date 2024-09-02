//#include <hdb_config.h>
#include <monetdb_config.h>
//#include <gdk.h>
#include "pqc_thrift.h"
#include "pqc_filemetadata.h"
#include "pqc_reader.h"

#include <stdbool.h>
#include <stdio.h>  // printf
#include <string.h> // memcpy
#include <unistd.h> // read
#include <assert.h>
#include <gdk.h>
#include <gdk_time.h>
#include <sql_mem.h>

//#include <utils/mem.h>
//#include <utils/str.h>
//#include <utils/file.h>
//#include <utils/sink.h>
//#include <utils/resource.h>
//#include <vector.h>
//#ifdef HAVE_SNAPPY
#include <snappy-c.h>
//#endif
//#ifdef HAVE_ZSTD
#include <zstd.h>
//#endif
#ifdef HAVE_LIBLZ4
#include <lz4.h>
#endif
#ifdef HAVE_LIBZ
#include <zlib.h>

static int
gzip_uncompress( char *dest, size_t ul, char *src, size_t cl)
{
	z_stream z = { 0 };
        z.next_in = (unsigned char *)src;
        z.avail_in = cl;
	z.zalloc = Z_NULL;
	z.zfree = Z_NULL;
	z.opaque = Z_NULL;

	if (inflateInit2(&z, 31) != Z_OK) { /* 15 bits window and 16 for gzip format */
            	//error("Failed to initialize z_stream");
		return -1;
        }

        z.next_out = (unsigned char *)dest;
        z.avail_out = ul;

	int res = inflate(&z, Z_FINISH);
	if (res != Z_OK && res != Z_STREAM_END) {
            printf("Failed to decompress GZIP block %d\n", res);
	    return -10;
        }
	inflateEnd(&z); /* cleanup */
	return 0;
}
#endif

//#ifdef HAVE_BROTLI
#include <brotli/decode.h>
//#endif

typedef unsigned char uchar;
typedef unsigned short usht;

typedef enum compressioncodec {
	CC_UNCOMPRESSED = 0,
	CC_SNAPPY = 1,
	CC_GZIP = 2,
  	CC_LZO = 3,
  	CC_BROTLI = 4,  // Added in 2.4
  	CC_LZ4 = 5,     // DEPRECATED (Added in 2.4)
  	CC_ZSTD = 6,    // Added in 2.4
  	CC_LZ4_RAW = 7, // Added in 2.9
} compressioncodec;

typedef struct pqc_creader_t {
	pqc_file *pq;
	pqc_columnchunk *cc;

	int rowgroup;
	int64_t curnr;	/* row number with in current rowgroup */
	int64_t pos;	/* pos of buffered data in file */
	int nr_bits;
	char *data;
	char *dict;
	char *odict;
	char first_definition; 	/* first definition (alternating 0/1) */
	int *definition;	/* definition lengths */
	int definitionsize;	/* size of definition lengths */
	int curdef;
	int cursubdef;
	char *buffer;
	size_t bufsize;
	size_t datasize;
	bool data_allocated;
	bool dict_allocated;
	size_t dict_num_values;
	size_t dictsize;
	/* left over rle */
	u_int32_t idx;
	u_int32_t remaining;
} pqc_creader_t;

typedef struct pqc_reader_t {
	struct pqc_reader_t *p;
	pqc_filemetadata *fmd;
	pqc_schema_element *pse;
	int rowgroup;
	int colnr;		/* colnr of this reader */
	int level;
	u_int64_t sz;
	u_int64_t rownr;

	int nrworkers;
	pqc_file *spq;
	pqc_creader_t *creader; /* per worker readers */
} pqc_reader_t;

static int
pqc_statistics( pqc_reader_t *r, pqc_creader_t *pr, pqc_stat *stat, int pos )
{
	(void)r;
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case 1: {
			if (type == 8) {
				char *t;
				int len;
				int res = pqc_get_string(pr->buffer+pos, &t, &len);
				//int res = pqc_string(pr, pr->buffer+pos, &stat->max_string);
				if (res < 0)
					return -1;
				pos += res;
				//TRC_INFO(PARQUET, "max_string '%s'\n", stat->max_string);
				TRC_INFO(PARQUET, "max_string '%s'\n", t);
			} else {
				assert (0);
#if 0
			} else if (type == 5) {
				pos += pqc_get_zint32(pr->buffer+pos, &stat->max);
				TRC_INFO(PARQUET, "max %d'\n", stat->max);
#endif
			}
		} break;
		case 2: {
			if (type == 8) {
				char *t;
				int len;
				int res = pqc_get_string(pr->buffer+pos, &t, &len);
				//int res = pqc_string(pr, pr->buffer+pos, &stat->min_string);
				if (res < 0)
					return -1;
				pos += res;
				//TRC_INFO(PARQUET, "min_string '%s'\n", stat->min_string);
				TRC_INFO(PARQUET, "min_string '%s'\n", t);
			} else {
				assert(0);
#if 0
			} else if (type == 5) {
				pos += pqc_get_zint32(pr->buffer+pos, &stat->min);
				TRC_INFO(PARQUET, "min %d'\n", stat->min);
#endif
			}
		} break;
		case 3: {
			pos += pqc_get_int64(pr->buffer+pos, &stat->null_count);
			TRC_INFO(PARQUET, "null_count %" PRId64 "\n", stat->null_count);
		}	break;
		case 4: {
			pos += pqc_get_int64(pr->buffer+pos, &stat->distinct_count);
			TRC_INFO(PARQUET, "distinct_count %" PRId64 "\n", stat->distinct_count);
		}	break;
		case 5: {
			if (type == 8) {
				char *t;
				int len;
				int res = pqc_get_string(pr->buffer+pos, &t, &len);
				//int res = pqc_string(pr, pr->buffer+pos, &stat->max_value);
				if (res < 0)
					return -1;
				pos += res;
				//TRC_INFO(PARQUET, "max_value '%s'\n", stat->max_value);
				TRC_INFO(PARQUET, "max_value '%s'\n", t);
			} else {
				assert(0);
#if 0
			} else if (type == 12) {
				pqc_keyvalue kv;
				pos = pqc_read_keyvalue(pr, &kv, pos);
#endif
			}
		} break;
		case 6: {
			if (type == 8) {
				char *t;
				int len;
				int res = pqc_get_string(pr->buffer+pos, &t, &len);
				//int res = pqc_string(pr, pr->buffer+pos, &stat->min_value);
				if (res < 0)
					return -1;
				pos += res;
				//TRC_INFO(PARQUET, "min_value '%s'\n", stat->min_value);
				TRC_INFO(PARQUET, "min_value '%s'\n", t);
			} else {
				assert(0);
#if 0
			} else if (type == 12) {
				pqc_keyvalue kv;
				pos = pqc_read_keyvalue(pr, &kv, pos);
#endif
			}
		} break;
		}
	}
	return pos;
}

static int
pqc_index_page(pqc_reader_t *r, pqc_creader_t *pr, int64_t pos, u_int32_t *num_values)
{
	(void)r;
	int fieldid = 0, type = 0;

	u_int32_t encoding = 0;

	assert(0);
	/* list stats */
	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case 1:
			pos += pqc_get_zint32(pr->buffer+pos, num_values);
			TRC_INFO(PARQUET, "num_values %d\n", *num_values);
			break;
		case 2:
			pos += pqc_get_zint32(pr->buffer+pos, &encoding);
			TRC_INFO(PARQUET, "encoding %d\n", encoding);
			break;
		case 3:
			TRC_INFO(PARQUET, "isSorted %d\n", type!=2);
			break;
		}
	}
	return pos;
}

static int
pqc_dictionary_page(pqc_reader_t *r, pqc_creader_t *pr, int64_t pos, u_int32_t *num_values)
{
	(void)r;
	int fieldid = 0, type = 0;

	u_int32_t encoding = 0;

	/* list stats */
	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case 1:
			pos += pqc_get_zint32(pr->buffer+pos, num_values);
			TRC_INFO(PARQUET, "num_values %d\n", *num_values);
			break;
		case 2:
			pos += pqc_get_zint32(pr->buffer+pos, &encoding);
			TRC_INFO(PARQUET, "encoding %u\n", encoding);
			break;
		case 3:
			TRC_INFO(PARQUET, "isSorted %d\n", type!=2);
			break;
		}
	}
	return pos;
}

static int
pqc_data_page(pqc_reader_t *r, pqc_creader_t *pr, int64_t pos, u_int32_t *num_values)
{
	int fieldid = 0, type = 0;

	u_int32_t encoding = 0;
	u_int32_t definition_level_encoding = 0;
	u_int32_t repetition_level_encoding = 0;
	/* list stats */

	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case 1:
			pos += pqc_get_zint32(pr->buffer+pos, num_values);
			pr->cc->num_values = *num_values;
			TRC_INFO(PARQUET, "num_values %d\n", *num_values);
			break;
		case 2:
			pos += pqc_get_zint32(pr->buffer+pos, &encoding);
			pr->cc->pageencodings[0].page_encoding = encoding;
			TRC_INFO(PARQUET, "page_encoding %u\n", encoding);
			break;
		case 3:
			pos += pqc_get_zint32(pr->buffer+pos, &definition_level_encoding);
			pr->cc->pageencodings[1].page_encoding = definition_level_encoding;
			TRC_INFO(PARQUET, "definition_level_encoding %u\n", definition_level_encoding);
			break;
		case 4:
			pos += pqc_get_zint32(pr->buffer+pos, &repetition_level_encoding);
			pr->cc->pageencodings[2].page_encoding = repetition_level_encoding;
			TRC_INFO(PARQUET, "repetition_level_encoding %u\n", repetition_level_encoding);
			break;
		case 5:
			assert(type == 12);
			pqc_stat stat;
			pos = pqc_statistics(r, pr, &stat, pos);
			break;
		}
	}
	return pos;
}

static int
pqc_data_pageV2(pqc_reader_t *r, pqc_creader_t *pr, int64_t pos, u_int32_t *num_values)
{
	int fieldid = 0, type = 0;

	u_int32_t encoding = 0;
	u_int32_t definition_levels_byte_length = 0;
	u_int32_t repetition_levels_byte_length = 0;
	/* list stats */
	u_int32_t num_nulls = 0, num_rows = 0;
	pr->cc->is_compressed=1;

	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case 1:
			pos += pqc_get_zint32(pr->buffer+pos, num_values);
			pr->cc->num_values = *num_values;
			TRC_INFO(PARQUET, "num_values %u\n", *num_values);
			break;
		case 2:
			pos += pqc_get_zint32(pr->buffer+pos, &num_nulls);
			pr->cc->num_nulls = num_nulls;
			TRC_INFO(PARQUET, "num_nulls %u\n", num_nulls);
			break;
		case 3:
			pos += pqc_get_zint32(pr->buffer+pos, &num_rows);
			pr->cc->num_rows = num_rows;
			TRC_INFO(PARQUET, "num_rows %u\n", num_rows);
			break;
		case 4:
			pos += pqc_get_zint32(pr->buffer+pos, &encoding);
			pr->cc->pageencodings[0].page_encoding = encoding;
			TRC_INFO(PARQUET, "page_encoding %u\n", encoding);
			break;
		case 5:
			pos += pqc_get_zint32(pr->buffer+pos, &definition_levels_byte_length);
			pr->cc->definition_levels_byte_length = definition_levels_byte_length;
			TRC_INFO(PARQUET, "definition_levels_byte_length %u\n", definition_levels_byte_length);
			break;
		case 6:
			pos += pqc_get_zint32(pr->buffer+pos, &repetition_levels_byte_length);
			pr->cc->repetition_levels_byte_length = repetition_levels_byte_length;
			TRC_INFO(PARQUET, "repetition_levels_byte_length %u\n", repetition_levels_byte_length);
			break;
		case 7:
			pr->cc->is_compressed = (type != 2);
			TRC_INFO(PARQUET, "is_compressed %d\n", pr->cc->is_compressed);
			break;
		case 8:
			assert(type == 12);
			pqc_stat stat;
			pos = pqc_statistics(r, pr, &stat, pos);
			break;
		}
	}
	return pos;
}

static int
string_read_dict( pqc_creader_t *cr, u_int32_t num_values)
	/* in 32 bit len, string (without zero) */
{
	u_int32_t i, hsz = 0;
	u_int8_t *data = (u_int8_t*)cr->dict;

	for (i=0; i<num_values; i++) {
		unsigned int len = get_uint32(data);

		data += len + sizeof(int);
		hsz += len+1;
	}
	if (num_values < 1)
		return -1;
	if (num_values > 100000 || hsz > 2000000)
		return -1;
	/* make one error to simplify memory management */
	char *mem = NEW_ARRAY(char, (sizeof(char*) * num_values + sizeof(int) * num_values) + hsz);
	if (!mem)
		return -1;
	char **rc = (char**)mem;
	char *buf = mem + sizeof(char*) * num_values + sizeof(int) * num_values, *obuf = buf;
	int *offsets = (int*)(mem + sizeof(char*) * num_values);
	data = (u_int8_t*)cr->dict;
    for (i=0; i<num_values; i++) {
        unsigned int len = get_uint32(data);

		data += sizeof(int);
        memcpy(buf, data, len);
		buf[len] = 0;
		offsets[i] = buf - obuf;
        rc[i] = buf;
        buf += len+1;
		data += len;
    }
	if (cr->dict_allocated)
		_DELETE(cr->dict);
	cr->dict = mem;
	cr->dictsize = buf - obuf;
	cr->dict_num_values = num_values;
	cr->dict_allocated = true;
    return num_values;
}

static int
pqc_page_header( pqc_reader_t *r, pqc_creader_t *pr, int64_t pos)
{
	int fieldid = 0, type = 0, res = 0;

	/* list stats */
	u_int32_t num_values = 0;
	u_int32_t page_type = 0;
	u_int32_t uncompressed_size = 0;
	u_int32_t compressed_size = 0;
	u_int32_t crc = 0;
	while(res >= 0) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case 1:
			pos += (res = pqc_get_zint32(pr->buffer+pos, &page_type));
			TRC_INFO(PARQUET, "page type %u\n", page_type);
			break;
		case 2:
			pos += (res = pqc_get_zint32(pr->buffer+pos, &uncompressed_size));
			TRC_INFO(PARQUET, "uncompressed_size %u\n", uncompressed_size);
			break;
		case 3:
			pos += (res = pqc_get_zint32(pr->buffer+pos, &compressed_size));
			TRC_INFO(PARQUET, "compressed_size %u\n", compressed_size);
			break;
		case 4:
			pos += (res = pqc_get_zint32(pr->buffer+pos, &crc));
			TRC_INFO(PARQUET, "crc %u\n", crc);
			break;
		case 5:
			assert(type == 12);
			pos = res = pqc_data_page(r, pr, pos, &num_values);
			break;
		case 6:
			assert(type == 12);
			pos = res = pqc_index_page(r, pr, pos, &num_values);
			break;
		case 7:
			assert(type == 12);
			pos = res = pqc_dictionary_page(r, pr, pos, &num_values);
			break;
		case 8:
			assert(type == 12);
			pos = res = pqc_data_pageV2(r, pr, pos, &num_values);
			break;
		}
	}
	if (res < 0)
		return pos;
	assert(page_type == 0 || page_type == 2 || page_type == 3 /*data v2*/);
	if (page_type == 0 || page_type == 3) {
		if (pos >= 0 && pr->cc->codec && (page_type != 3 || pr->cc->is_compressed)) {
			assert(pr->data == NULL);
			pr->data = NEW_ARRAY(char, uncompressed_size);
			if (!pr->data)
				return -1;
			pr->datasize = uncompressed_size;
			pr->data_allocated = true;
			size_t ul = uncompressed_size;
			if (pr->cc->codec == CC_SNAPPY) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				if (snappy_uncompress(pr->buffer+pos + v2, compressed_size - v2, pr->data, &ul) != SNAPPY_OK)
					return -10;
				assert(uncompressed_size == ul);
				pos += compressed_size;
			} else if (pr->cc->codec == CC_GZIP) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				if (gzip_uncompress(pr->data, ul, pr->buffer+pos + v2, compressed_size - v2))
					return -10;
				pos += compressed_size;
			} else if (pr->cc->codec == CC_ZSTD) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				if (ZSTD_decompress(pr->data, ul, pr->buffer+pos + v2, compressed_size - v2) != ul)
					return -10;
				pos += compressed_size;
			} else if (pr->cc->codec == CC_LZ4_RAW) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				int iul = (int)ul;
				if (LZ4_decompress_safe(pr->buffer+pos + v2, pr->data, compressed_size - v2, iul) != iul)
					return -10;
				pos += compressed_size;
			} else if (pr->cc->codec == CC_BROTLI) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				if (BrotliDecoderDecompress(compressed_size - v2, (u_int8_t*)pr->buffer+pos + v2, &ul, (u_int8_t*)pr->data) != BROTLI_DECODER_RESULT_SUCCESS)
					return -10;
				pos += compressed_size;
			} else
				assert(0);
		} else {
			pr->data_allocated = false;
			pr->data = pr->buffer+pos;
		//	pr->datasize = pr->bufsize-pos;
			pr->datasize = uncompressed_size;
			pos += uncompressed_size;
		}
		assert(pr->datasize);
	}
	if (page_type == 2) {
		if (pos >= 0 && pr->cc->codec) {
			assert(pr->dict == NULL);
			pr->dict = NEW_ARRAY(char, uncompressed_size);
			if (!pr->dict)
				return -1;
			pr->dict_allocated = true;
			pr->dictsize = uncompressed_size;
			size_t ul = uncompressed_size;
			if (pr->cc->codec == CC_SNAPPY) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				if (snappy_uncompress(pr->buffer+pos + v2, compressed_size - v2, pr->dict, &ul) != SNAPPY_OK)
					return -10;
				assert(uncompressed_size == ul);
				pos += compressed_size;
			} else if (pr->cc->codec == CC_GZIP) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				if (gzip_uncompress(pr->dict, ul, pr->buffer+pos + v2, compressed_size - v2))
					return -10;
				pos += compressed_size;
			} else if (pr->cc->codec == CC_ZSTD) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				if (ZSTD_decompress(pr->dict, ul, pr->buffer+pos + v2, compressed_size - v2) != ul)
					return -10;
				pos += compressed_size;
			} else if (pr->cc->codec == CC_LZ4_RAW) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				int iul = (int)ul;
				if (LZ4_decompress_safe(pr->buffer+pos + v2, pr->dict, compressed_size - v2, iul) != iul)
					return -10;
				pos += compressed_size;
			} else if (pr->cc->codec == CC_BROTLI) {
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->definition_levels_byte_length + pr->cc->repetition_levels_byte_length;
				if (BrotliDecoderDecompress(compressed_size - v2, (u_int8_t*)pr->buffer+pos + v2, &ul, (u_int8_t*)pr->dict) != BROTLI_DECODER_RESULT_SUCCESS)
					return -10;
				pos += compressed_size;
			} else
				assert(0);
		} else {
			pr->dict_allocated = false;
			pr->dict = pr->buffer+pos;
			//pr->dictsize = pr->bufsize-pos;
			pr->dictsize = uncompressed_size;
			pos += uncompressed_size;
		}
		if (r->pse->precision)
			pr->dict_num_values = pr->dictsize / (r->pse->size/8);
		if (num_values && r->pse->type == stringtype) {
			if (string_read_dict(pr, num_values) < 0)
				return -1;
		}
	}
	return pos;
}

pqc_reader_t *
pqc_reader( pqc_reader_t *p, pqc_file *pq, int nrworkers, /*pqc_columnchunk *cc, pqc_schema_element *pse,*/ pqc_filemetadata *fmd, int colnr, int64_t nrows)
{
	pqc_reader_t *r = MNEW(pqc_reader_t);
	pqc_creader_t *cr = NEW_ARRAY(pqc_creader_t, nrworkers);
	if (!r || !cr) {
		_DELETE(r);
		return r;
	}
	r->p = p;
	r->creader = cr;
	r->fmd = fmd;
	r->pse = fmd->elements+colnr+1;
	r->sz = nrows;
	r->colnr = colnr;
	r->rowgroup = -1;
	r->rownr = 0;
	r->nrworkers = nrworkers;
	r->spq = pq;
	assert(colnr < fmd->rowgroups->ncolumnchunks);
	for(int i = 0; i<nrworkers; i++) {
		cr = r->creader+i;

		cr->pq = pqc_copy(pq);
		cr->rowgroup = -1;
		cr->cc = NULL;
		cr->curnr = 0;
		cr->pos = -1;
		cr->odict = cr->dict = 0;
		cr->dictsize = 0;
		cr->dict_allocated = false;
		cr->first_definition = cr->curdef = cr->cursubdef = 0;
		cr->definition = 0;
		cr->definitionsize = 0;
		cr->data = 0;
		cr->datasize = 0;
		cr->data_allocated = false;
		cr->buffer = NULL;
		cr->bufsize = 0;
		cr->remaining = 0;
	}
	return r;
}

void
pqc_reader_destroy( pqc_reader_t *r )
{
	for(int i = 0; i<r->nrworkers; i++ ) {
		pqc_creader_t *cr = r->creader+i;
		pqc_close(cr->pq);
		if (cr->definition)
			_DELETE(cr->definition);
		if (cr->odict)
			_DELETE(cr->odict);
		if (cr->dict && !(cr->dict >= cr->buffer && cr->dict < (cr->buffer+cr->bufsize))) {
			assert(cr->dict_allocated);
			_DELETE(cr->dict);
		}
		if (cr->data && !(cr->data >= cr->buffer && cr->data < (cr->buffer+cr->bufsize))) {
			assert(cr->data_allocated);
			_DELETE(cr->data);
		}
		if (cr->bufsize)
			_DELETE(cr->buffer);
	}
	_DELETE(r->creader);
	pqc_close(r->spq);
	_DELETE(r);
}

static int64_t
pqc_read_dict( pqc_reader_t *r, pqc_creader_t *cr)
{
	int pos = pqc_page_header(r, cr, 0);
	if (pos < 0)
		return -1;
	return pos;
}

static int64_t
pqc_definition( pqc_reader_t *r, pqc_creader_t *cr, void *output, u_int32_t num_values, u_int32_t pos)
{
	(void)r;
	/* Run Length Encoding / Bit-Packing Hybrid (RLE = 3
	   rle-bit-packed-hybrid: <length> <encoded-data>
	   length := length of the <encoded-data> in bytes stored as 4 bytes little endian (unsigned int32)
	   encoded-data := <run>*
	   run := <bit-packed-run> | <rle-run>
	   bit-packed-run := <bit-packed-header> <bit-packed-values>
	   bit-packed-header := varint-encode(<bit-pack-scaled-run-len> << 1 | 1)
	   // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
	   bit-pack-scaled-run-len := (bit-packed-run-len) / 8
	   bit-packed-run-len := *see 3 below*
	   bit-packed-values := *see 1 below*
	   rle-run := <rle-header> <repeated-value>
	   rle-header := varint-encode( (rle-run-len) << 1)
	   rle-run-len := *see 3 below*
	   repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
	*/

	/* definition level without repetition is 1 bit (0 or 1) */
	(void)output;
	//int bits = 1;
	char *data = cr->data;
	u_int32_t nr_bytes = get_uint32((u_int8_t*)data+pos);
    u_int32_t spos = pos, len;
	u_int64_t null = 0, j = 0;

	if (cr->definition)
		_DELETE(cr->definition);
	cr->definition = NULL;
	cr->first_definition = cr->curdef = cr->cursubdef = 0;

	pos += sizeof(nr_bytes);
	for(u_int32_t i = 0; i<num_values && ((pos-spos) < nr_bytes); ) {
		pos += pqc_get_int32(data+pos, &len);
		if (len & 1) {
			len>>=1;
			assert(0);
			/* TODO handle single bits */
		} else { /* rle */
			len>>=1;
			char val = data[pos++];
			assert(val == 0 || val == 1);
			if (i == 0 && (val != 1 || len < num_values)) {
				cr->definitionsize = 0;
				cr->first_definition = val;
				cr->definition = NEW_ARRAY(int, nr_bytes/2); /* should be enough */
				if (!cr->definition)
					return -1;
			}
			if (cr->definition)
				cr->definition[j++] = len;
			assert(len);
			i+=len;
			null += (!val)*len;
		}
	}
	if (cr->definition)
		cr->definitionsize = j;

	TRC_DEBUG(PARQUET, "nulls %" PRIu64 " rows %u (%p)\n", null, num_values, cr->definition);
	/* return definition level as structure 0/1 + len */
	return nr_bytes + sizeof(nr_bytes);
}

static int64_t
pqc_dict_lookup( pqc_reader_t *r, pqc_creader_t *cr, void *output, void *voutput, int64_t nrows, int pos, int *ssize, int *dict)
{
	uchar *data = (uchar*)cr->data;
	/* asume rle data page */
	if (r->pse->precision == 0 && !output) {
		if (ssize) {
			*ssize = cr->dictsize;
			if (!cr->curnr)
				cr->nr_bits = data[pos];
			*dict = cr->nr_bits<=8?1:2; /* later include size of dict offsets */
			assert(cr->dictsize);
			return 0;
		}
	}

	/* rle / bit packed */
	if (!cr->curnr)
		cr->nr_bits = data[pos++];
	int nr_bits = cr->nr_bits;

	//printf("precision %d bits %d dict %d\n", r->pse->precision, nr_bits, (int)dict);
	assert(nr_bits < 16);
	if (r->pse->precision == 0 && !dict) {
		int64_t i = 0;
		char **dst = output;
		bool mul8 = ((8/nr_bits)*nr_bits == 8);
		uchar idx = cr->idx;
		if (cr->remaining) {
			u_int32_t j = 0;
			for(; i < nrows && j < cr->remaining; j++, i++)
				dst[i] = ((char**)cr->dict)[idx];
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
						dst[i] = ((char**)cr->dict)[idx];
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
							if (j==(m-1) && sh > 0)
								pos++;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = ((char**)cr->dict)[idx];
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
							if (j==(m-1) && sh >= 8)
								pos++;
							else if (j==(m-1) && sh > 0)
								pos+=2;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = ((char**)cr->dict)[idx];
					}
				}
			} else if (nr_bits <= 8) { /* rle */
				len>>=1;
				uchar idx = data[pos++];
				u_int32_t j = 0;
				for(; i < nrows && j < len; j++, i++)
					dst[i] = ((char**)cr->dict)[idx];
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
					dst[i] = ((char**)cr->dict)[idx];
				if (j < len) {
					cr->remaining = len - j;
					cr->idx = idx;
				}
			}
		}
	} else if (r->pse->precision == 0 && dict) { /* offsets */
		assert (*dict == 2);
		memcpy(voutput, cr->dict + cr->dict_num_values * (sizeof(char*)+sizeof(int)), cr->dictsize);
		int64_t i = 0;
		int *offsets = (int*)(cr->dict + cr->dict_num_values * sizeof(char*));
		bool mul8 = ((8/nr_bits)*nr_bits == 8);
		usht *dst = output;
		/* start offset (passed voutput ptr is not start of vheap) */
		usht offset = *ssize;
		uchar idx = cr->idx;
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
							if (j==(m-1) && sh > 0)
								pos++;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = offset+offsets[idx];
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
							if (j==(m-1) && sh >= 8)
								pos++;
							else if (j==(m-1) && sh > 0)
								pos+=2;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = offset+offsets[idx];
					}
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
	} else if (r->pse->precision == 8) {
		assert(0);
		char *dst = output;

		for(int64_t i = 0; i<nrows; ) {
			u_int32_t len = 0;
			pos += pqc_get_int32((char*)data+pos, &len);
			if (len & 1) {
				len>>=1;
				/* only 2 bits for now */
				int sh = 0;
				int mask = (1<<nr_bits) -1;
				if ((8/nr_bits)*nr_bits == 8) {
					int m = (len+1)*8;
					for (int64_t j = 0; i < nrows && j < m; j++, i++) {
						uchar v = data[pos];
						u_int32_t idx = (v >> sh)&mask;
						sh += nr_bits;
						if (sh >= 8) {
							pos++;
							sh = 0;
						}
						dst[i] = ((int*)cr->dict)[idx];
					}
				} else {
					int m = (len+1)*8;
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
						dst[i] = ((int*)cr->dict)[idx];
					}
				}
			} else { /* rle */
				len>>=1;
				uchar val = data[pos++];
				for(int64_t j = 0; i < nrows && j < len; j++, i++) {
					dst[i] = ((int*)cr->dict)[val];
				}
			}
		}
	} else if (r->pse->precision == 16) {
		assert(0);
		short *dst = output;

		for(int64_t i = 0; i<nrows; ) {
			u_int32_t len = 0;
			pos += pqc_get_int32((char*)data+pos, &len);
			if (len & 1) {
				len>>=1;
				/* only 2 bits for now */
				int sh = 0;
				int mask = (1<<nr_bits) -1;
				if ((8/nr_bits)*nr_bits == 8) {
					int m = len*8;
					for (int64_t j = 0; i < nrows && j < m; j++, i++) {
						uchar v = data[pos];
						u_int32_t idx = (v >> sh)&mask;
						sh += nr_bits;
						if (sh >= 8) {
							pos++;
							sh = 0;
						}
						dst[i] = ((int*)cr->dict)[idx];
					}
				} else {
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
						dst[i] = ((int*)cr->dict)[idx];
					}
				}
			} else { /* rle */
				len>>=1;
				uchar val = data[pos++];
				for(int64_t j = 0; i < nrows && j < len; j++, i++) {
					dst[i] = ((int*)cr->dict)[val];
				}
			}
		}
	} else if (r->pse->precision == 32) {
		int *dst = output;

		for(int64_t i = 0; i<nrows; ) {
			u_int32_t len = 0;
			pos += pqc_get_int32((char*)data+pos, &len);
			if (len & 1) {
				len>>=1;
				/* only 2 bits for now */
				int sh = 0;
				int mask = (1<<nr_bits) -1;
				if ((8/nr_bits)*nr_bits == 8) {
					int m = len*8;
					for (int64_t j = 0; i < nrows && j < m; j++, i++) {
						uchar v = data[pos];
						u_int32_t idx = (v >> sh)&mask;
						sh += nr_bits;
						if (sh >= 8) {
							pos++;
							sh = 0;
						}
						dst[i] = ((int*)cr->dict)[idx];
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
							if (j==(m-1) && sh > 0)
								pos++;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = ((int*)cr->dict)[idx];
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
							if (j==(m-1) && sh >= 8)
								pos++;
							else if (j==(m-1) && sh > 0)
								pos+=2;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = ((int*)cr->dict)[idx];
					}
				}
			} else { /* rle */
				len>>=1;
				uchar val = data[pos++];
				for(int64_t j = 0; i < nrows && j < len; j++, i++) {
					dst[i] = ((int*)cr->dict)[val];
				}
			}
		}
	} else if (r->pse->precision == 64) {
		int64_t *dst = output;

		for(int64_t i = 0; i<nrows; ) {
			u_int32_t len = 0;
			pos += pqc_get_int32((char*)data+pos, &len);
			if (len & 1) {
				len>>=1;
				/* only 2 bits for now */
				int sh = 0;
				int mask = (1<<nr_bits) -1;
				if ((8/nr_bits)*nr_bits == 8) {
					int m = len*8;
					for (int64_t j = 0; i < nrows && j < m; j++, i++) {
						uchar v = data[pos];
						u_int32_t idx = (v >> sh)&mask;
						sh += nr_bits;
						if (sh >= 8) {
							pos++;
							sh = 0;
						}
						dst[i] = ((int64_t*)cr->dict)[idx];
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
							if (j==(m-1) && sh > 0)
								pos++;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = ((int64_t*)cr->dict)[idx];
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
							if (j==(m-1) && sh >= 8)
								pos++;
							else if (j==(m-1) && sh > 0)
								pos+=2;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = ((int64_t*)cr->dict)[idx];
					}
				}
			} else { /* rle */
				len>>=1;
				uchar val = data[pos++];
				for(int64_t j = 0; i < nrows && j < len; j++, i++) {
					dst[i] = ((int64_t*)cr->dict)[val];
				}
			}
		}
	} else {
		printf("later\n");
	}
	cr->pos = pos;
	return nrows;

}

static int
string_read_chunk_withnulls( pqc_creader_t *cr, char **rc, char *buf, int64_t nrows, int pos)
{
	char *nil = buf;
	memcpy(nil,"NULL", 5);
	buf += 5;
	char *data = cr->data + pos;
	int cur = cr->curdef;
	int sdef = cr->cursubdef;
	int def = cr->first_definition;
	for(int64_t i = 0; i<nrows; ) {
		int len = cr->definition[cur];
		len -= sdef;
		if (i + len > nrows) {
			len = nrows-i;
			sdef += len;
		} else {
			sdef = 0;
		}
		if (def) {
                	int slen = *(int*)data;

			data += sizeof(int);
                	memcpy(buf, data, slen);
			buf[slen] = 0;
			for (int j=0; j<len; j++)
                		rc[i++] = buf;
                	buf += slen+1;
			data += slen;
		} else {
			for (int j=0; j<len; j++)
                		rc[i++] = nil;
        	}
		if (!sdef) {
			def = !def;
			cur++;
		}
	}
	cr->curdef = cur;
	cr->cursubdef = sdef;
	cr->first_definition = def;
	cr->pos = (data - cr->data);
        return nrows;
}

static int
string_read_chunk( pqc_creader_t *cr, char **rc, char *buf, int64_t nrows, int pos)
{
	if (cr->definition)
		return string_read_chunk_withnulls( cr, rc, buf, nrows, pos);

	if (nrows > cr->cc->num_values)
		nrows = cr->cc->num_values;

	char *data = cr->data + pos;
	for (int64_t i=0; i<nrows; i++) {
		unsigned int len = get_uint32((uchar*)data);

		data += sizeof(int);
		memcpy(buf, data, len);
		buf[len] = 0;
		rc[i] = buf;
		buf += len+1;
		data += len;
	}
	cr->pos = (data - cr->data);
	return nrows;
}

static int
string_size_chunk_withnulls( pqc_creader_t *cr, int64_t nrows, int pos, int *ssize, int *dict)
{
	(void)dict;
	/* reuse strings */
	size_t hsz = 5; /* 5 for NULL */
	char *data = cr->data;

	data += pos;
	int cur = cr->curdef, sdef = cr->cursubdef;
	int def = cr->first_definition;
	for(int64_t i = 0; i<nrows; ) {
		int len = cr->definition[cur];
		len -= sdef;
		if (i + len > nrows) {
			len = nrows-i;
			sdef += len;
		} else {
			sdef = 0;
		}
		if (def) {
			int slen = *(int*)data;
			data += slen + sizeof(int);
			hsz += slen+1;
		}
		i+=len;
		if (!sdef) {
			def = !def;
			cur++;
		}
	}
	*ssize = hsz;
	return 0;
}

static int
string_size_chunk( pqc_creader_t *cr, int64_t nrows, int pos, int *ssize, int *dict)
{
	/* based on dict call right version */
	if (cr->definition)
		return string_size_chunk_withnulls( cr, nrows, pos, ssize, dict);

	/* reuse strings */
	size_t hsz = 0;
	char *data = cr->data;

	data += pos;
	if (nrows > cr->cc->num_values)
		nrows = cr->cc->num_values;

	if (cr->cc->stat.null_count == cr->cc->num_values) { /* all null */
		*ssize = 5;
		return 0;
	}

	for (int64_t i=0; i<nrows; i++) {
		unsigned int len = get_uint32((uchar*)data);

		data += len + sizeof(int);
		hsz += len+1;
	}
	*ssize = hsz;
	return 0;
}


static int
offset_string_read_chunk_sht( pqc_creader_t *cr, usht *output, char *voutput, int64_t nrows, int pos, int offset)
{
	usht *rc = output;
	char *buf = voutput;
	char *data = cr->data + pos;

	if (cr->cc->stat.null_count == cr->cc->num_values) { /* all null */
		memcpy(buf, "NULL", 5);
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

static int
offset_string_read_chunk( pqc_creader_t *cr, void *output, void *voutput, int64_t nrows, int pos, int offset, int width)
{
	if (cr->definition)
		assert(0);
		//return offset_string_read_chunk_withnulls( cr, output, nrows, pos);

	if (width == 1) {
		assert(0);
	} else if (width == 2) {
		return offset_string_read_chunk_sht( cr, output, voutput, nrows, pos, offset);
	} else {
		assert(0);
	}
	return -1;
}

static int64_t
pqc_read_delta( pqc_creader_t *cr, int *prefixes, int64_t nrows, int pos)
{
	u_int64_t blocks = 0, miniblocks = 0, count = 0;
	u_int64_t val = 0;

	(void)prefixes;
	(void)nrows;
	if ((pos += pqc_get_int64(cr->data+pos, &blocks)) < 0) {
		return -1;
	}
	if ((pos += pqc_get_int64(cr->data+pos, &miniblocks)) < 0) {
		return -1;
	}
	assert((miniblocks * 32) == blocks);
	if ((pos += pqc_get_int64(cr->data+pos, &count)) < 0) {
		return -1;
	}
	if ((pos += pqc_get_zint64(cr->data+pos, &val)) < 0) {
		return -1;
	}
	if (count <= 1) {
		*prefixes = val;
		return pos;
	}
	return pos;
}

static int64_t
pqc_read_strings( pqc_creader_t *cr, char **rc, char *buf, int *lengths, int64_t nrows, int pos)
{
	char *data = cr->data + pos;

        for (int64_t i=0; i<nrows; i++) {
                int len = lengths[i];

                memcpy(buf, data, len);
		buf[len] = 0;
                rc[i] = buf;
                buf += len+1;
		data += len;
        }
	return (data - cr->data);
}

/* todo other offsets */
static int64_t
offset_read_strings_sht( pqc_creader_t *cr, usht *rc, char *buf, int *lengths, int64_t nrows, int pos, int offset)
{
	char *data = cr->data + pos;

        for (int64_t i=0; i<nrows; i++) {
                int len = lengths[i];

                memcpy(buf, data, len);
		buf[len] = 0;
                rc[i] = offset;
                buf += len+1;
		offset += len+1;
		data += len;
        }
	return (data - cr->data);
}

static int
offset_read_strings( pqc_creader_t *cr, void *output, void *voutput, int *lengths, int64_t nrows, int pos, int offset, int width)
{
	if (width == 1) {
		assert(0);
	} else if (width == 2) {
		return offset_read_strings_sht( cr, output, voutput, lengths, nrows, pos, offset);
	} else {
		assert(0);
	}
	return -1;
}

static int64_t
pqc_size_strings( pqc_creader_t *cr, int *lengths, int64_t nrows, int pos, int *ssize)
{
        size_t hsz = 0;
	char *data = cr->data + pos;

        for (int64_t i = 0; i<nrows; i++) {
                int len = lengths[i];

		data += len;
                hsz += len+1;
        }
	*ssize = hsz;
	return 0;
}

static int64_t
pqc_read_delta_strings( pqc_creader_t *cr, void *output, void *voutput, int64_t nrows, int pos, int *ssize, int *dict)
{
	/* how to keep state!
	 * for prefixes keep pos and counter
	 * for suffixes keep pos and counter for lengths
	 * 		and for suffix strings
	 */
	/* read prefixes */
	int prefixes = 0;
	if ((pos = pqc_read_delta( cr, &prefixes, nrows, pos)) < 0)
		return -1;
	(void)voutput;
	(void)ssize;
	(void)dict;
	/* read lengths */
	int lengths = 0;
	if ((pos = pqc_read_delta( cr, &lengths, nrows, pos)) < 0)
		return -1;
	if (!voutput)
		return pqc_size_strings( cr, &lengths, nrows, pos, ssize);
	/* read strings */
	else if (dict) {
		if ((pos = offset_read_strings( cr, output, voutput, &lengths, nrows, pos, *ssize, *dict)) < 0)
			return -1;
	} else if ((pos = pqc_read_strings( cr, output, voutput, &lengths, nrows, pos)) < 0)
		return -1;
	cr->pos = pos;
	return nrows;
}

int64_t
pqc_mark_chunk( pqc_reader_t *r, int nr_workers, int wnr, u_int64_t nrows)
{
	u_int64_t orows = nrows;
	if (r->rownr >= r->sz)
		return 0;

	pqc_creader_t *cr = r->creader+wnr;
	if (cr->cc && cr->curnr + nrows > cr->cc->nrows)
		nrows = cr->cc->nrows - cr->curnr;
	while (!cr->cc || nrows == 0) {
		/* next vector */
		int rg = -1;
		rg = cr->rowgroup;
		if (rg < 0)
			rg = wnr;
		else
			rg += nr_workers ;
		if (rg < r->fmd->nrowgroups) {
			assert(rg >= 0);
			cr->rowgroup = rg;
			nrows = orows;
			cr->pos = -1;
			cr->curnr = 0;
			cr->cc = r->fmd->rowgroups[rg].columnchunks+r->colnr;

			if (cr->data && cr->data_allocated)
				_DELETE(cr->data);
			cr->data = NULL;
			cr->data_allocated = false;

			if (cr->odict) { /* values may still be in flight */
				_DELETE(cr->odict);
				cr->odict = NULL;
			}

			if (cr->dict && cr->dict_allocated)
				cr->odict = cr->dict;
			cr->dict = NULL;
			cr->dict_allocated = false;

			if (cr->definition)
				_DELETE(cr->definition);
		} else {
			cr->rowgroup = rg;
			return 0;
		}
	}
	return 1;
}

#define YEAR_OFFSET -(-4712)
#define DTDAY_WIDTH             5               /* 1..28/29/30/31, depending on month/year */
#define DTDAY_SHIFT             0
#define DTMONTH_WIDTH   21              /* enough for 174761 years (and 8 months) */
#define DTMONTH_SHIFT   (DTDAY_WIDTH+DTDAY_SHIFT)
#define mkdate(y, m, d) (((uint32_t) (((y) + YEAR_OFFSET) * 12 + (m) - 1) << DTMONTH_SHIFT) | ((uint32_t) (d) << DTDAY_SHIFT))

int64_t
pqc_read_chunk( pqc_reader_t *r, int wnr, void *output /*fixed sized atom storage */, void *voutput /* var storage */, u_int64_t nrows, int *ssize, int *dict)
{
	if (r->rownr >= r->sz)
		return 0;

	pqc_creader_t *cr = r->creader+wnr;
	if (cr->rowgroup >= r->fmd->nrowgroups)
		return 0;
	if (cr->cc && cr->curnr + nrows > cr->cc->nrows)
		nrows = cr->cc->nrows - cr->curnr;

	if (r->rownr + nrows > r->sz)
		nrows = r->sz - r->rownr;
	if (cr->pos < 0) {
		TRC_INFO(PARQUET, "%" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
					r->fmd->rowgroups->file_offset,
					cr->cc->file_offset,
					cr->cc->data_page_offset,
					cr->cc->index_page_offset,
					cr->cc->dictionary_page_offset);
		if (cr->bufsize < cr->cc->total_compressed_size) {
			if (cr->bufsize)
				_DELETE(cr->buffer);
			cr->bufsize = cr->cc->total_compressed_size;
			cr->buffer = NEW_ARRAY(char, cr->bufsize);
		}

		int pos = 0;
		assert (cr->cc->index_page_offset == 0); /* we currently don't handle parquet indices */
		if (cr->cc->dictionary_page_offset) { /* load dictionary */
			if ((cr->pos = pqc_read(cr->pq, cr->cc->dictionary_page_offset, cr->buffer, cr->cc->total_compressed_size)) < 0)
				return -3;

			assert(cr->pos == (int64_t)cr->cc->dictionary_page_offset);
			if ((pos = pqc_read_dict(r, cr)) < 0)
				return -4;
		} else { /* read data page */
			if ((cr->pos = pqc_read(cr->pq, cr->cc->data_page_offset, cr->buffer, cr->cc->total_compressed_size)) < 0)
				return -3;
			assert(cr->pos == (int64_t)cr->cc->data_page_offset);
		}

		/*
		if (pos > (cr->cc->data_page_offset - cr->cc->dictionary_page_offset)) {
			printf("input broken pos > (data_page_offset-dictionary_page_offset %d!=%d %lld)\n", pos, cr->cc->data_page_offset - cr->cc->dictionary_page_offset, cr->cc->data_page_offset);
		}
		*/
		pos = pqc_page_header(r, cr, pos);
		if (pos < 0) {
			assert(0);
			return -1;
		}
		/* read data */
		if (cr->data) {
			pos = 0;
			cr->pos = pos;
			if (r->pse->repetition == 1 /*OPTIONAL*/) {
				/* bit vector for null's */
				pos = pqc_definition(r, cr, output, cr->cc->num_values, (u_int32_t)pos);
				cr->pos = pos;
			}
			if (pos < 0) {
				assert(0);
				return pos;
			}
			if (cr->dict) {
				nrows = pqc_dict_lookup(r, cr, output, voutput, nrows, pos, ssize, dict);
			} else {
				if (cr->cc->pageencodings[0].page_encoding) {
					/* delta strings == delta_encoded (encoding==5) prefixes (lengths),
					 * delta_length_byte_array((encoding==6) == (delta encoded lengths, back2back strings)) suffix strings */
					nrows = pqc_read_delta_strings(cr, output, voutput, nrows, pos, ssize, dict);
				} else if (r->pse->type != stringtype) {
					/* fixed types, plain encoding */
					if (!cr->definition) {
						memcpy(output, ((char*)cr->data)+pos, nrows*(r->pse->size/8));
						pos += (r->pse->size/8)*nrows;
					} else {
						int cur = cr->curdef, sdef = cr->cursubdef;
						int def = cr->first_definition;
						for(u_int64_t i = 0; i<nrows; ) {
							int len = cr->definition[cur];
							len -= sdef;
							if (i + len > nrows) {
								len = nrows-i;
								sdef += len;
							} else {
								sdef = 0;
							}
							if (def) {
								memcpy((char*)output+(r->pse->size/8)*i, ((char*)cr->data)+pos, len*(r->pse->size/8));
								pos += (r->pse->size/8)*len;
							} else {
								memset((char*)output+(r->pse->size/8)*i, 0, len*(r->pse->size/8));
							}
							i+=len;
							if (!sdef) {
								def = !def;
								cur++;
							}
						}
						cr->curdef = cur;
						cr->cursubdef = sdef;
						cr->first_definition = def;
					}
					cr->pos = pos;
				} else if (r->pse->type == stringtype) {
					/* int32 len, string (no zero) encoding */
					if (!ssize)
						string_read_chunk(cr, output, voutput, nrows, pos);
					else if (!voutput)
						return string_size_chunk(cr, nrows, pos, ssize, dict);
					else
						offset_string_read_chunk(cr, output, voutput, nrows, pos, *ssize, *dict);
				}
			}
			/* convert data */
			if (r->pse->type == datetype) {
				/* days since epoch ie 1-1-1970 */
				uint32_t epoch_date = mkdate(1970, 1, 1);
				if (r->pse->precision == 32) {
					uint32_t *l = output;
					for(u_int64_t i=0; i< nrows; i++) {
						l[i] = date_add_day(epoch_date, l[i]);
					}
				}
			} else // todo timetype
			if (r->pse->type == timestamptype) {
				if (r->pse->precision == 6) {
					int64_t *l = output;
					for(u_int64_t i=0; i< nrows; i++) {
						l[i] = timestamp_fromusec(l[i]);
					}
				}
			}
			ATOMIC_ADD(&r->rownr, nrows);
			cr->curnr += nrows;
			return nrows;
		}
	} else {
		/* next vector */
		if (cr->data) {
			int pos = cr->pos;
			if (cr->dict) {
				nrows = pqc_dict_lookup(r, cr, output, voutput, nrows, pos, ssize, dict);
			} else {
				if (cr->cc->pageencodings[0].page_encoding) {
					/* delta strings == delta_encoded (encoding==5) prefixes (lengths),
					 * delta_length_byte_array((encoding==6) == (delta encoded lengths, back2back strings)) suffix strings */
					nrows = pqc_read_delta_strings(cr, output, voutput, nrows, pos, ssize, dict);
				} else if (r->pse->type != stringtype) {
					/* fixed types, plain encoding */
					if (!cr->definition) {
						memcpy(output, ((char*)cr->data)+pos, nrows*(r->pse->size/8));
						pos += (r->pse->size/8)*nrows;
					} else {
						int cur = cr->curdef, sdef = cr->cursubdef;
						int def = cr->first_definition;
						for(u_int64_t i = 0; i<nrows; ) {
							int len = cr->definition[cur];
							len -= sdef;
							if (i + len > nrows) {
								len = nrows-i;
								sdef += len;
							} else {
								sdef = 0;
							}
							if (def) {
								memcpy((char*)output+(r->pse->size/8)*i, ((char*)cr->data)+pos, len*(r->pse->size/8));
								pos += (r->pse->size/8)*len;
							} else {
								memset((char*)output+(r->pse->size/8)*i, 0, len*(r->pse->size/8));
							}
							i+=len;
							if (!sdef) {
								def = !def;
								cur++;
							}
						}
						cr->curdef = cur;
						cr->cursubdef = sdef;
						cr->first_definition = def;
					}
					cr->pos = pos;
				} else if (r->pse->type == stringtype) {
					/* int32 len, string (no zero) encoding */
					if (!ssize)
						string_read_chunk(cr, output, voutput, nrows, pos);
					else if (!voutput)
						return string_size_chunk(cr, nrows, pos, ssize, dict);
					else
						offset_string_read_chunk(cr, output, voutput, nrows, pos, *ssize, *dict);
				}
			}
			/* convert data */
			if (r->pse->type == datetype) {
				/* days since epoch ie 1-1-1970 */
				uint32_t epoch_date = mkdate(1970, 1, 1);
				if (r->pse->precision == 32) {
					uint32_t *l = output;
					for(u_int64_t i=0; i< nrows; i++) {
						l[i] = date_add_day(epoch_date, l[i]);
					}
				}
			} else // todo timetype
			if (r->pse->type == timestamptype) {
				if (r->pse->precision == 6) {
					int64_t *l = output;
					for(u_int64_t i=0; i< nrows; i++) {
						l[i] = timestamp_fromusec(l[i]);
					}
				}
			}
			ATOMIC_ADD(&r->rownr, nrows);
			cr->curnr += nrows;
			return nrows;
		}
	}
	return 0;
}
