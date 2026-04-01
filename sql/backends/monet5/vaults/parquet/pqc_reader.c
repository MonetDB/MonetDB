/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include <monetdb_config.h>
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

#ifdef HAVE_SNAPPY
#include <snappy-c.h>
#endif
#ifdef HAVE_ZSTD
#include <zstd.h>
#endif
#ifdef HAVE_LIBLZ4
#include <lz4.h>
#endif
#ifdef HAVE_LIBZ
#include <zlib.h>

#ifdef _MSC_VER
/* use intrinsic functions on Windows */
#define short_int_SWAP(s)	((int16_t) _byteswap_ushort((uint16_t) (s)))
#else
#define short_int_SWAP(s)				\
	((int16_t) (((0x00ff & (uint16_t) (s)) << 8) |	\
		  ((0xff00 & (uint16_t) (s)) >> 8)))
#endif

static int
gzip_uncompress( char *dest, size_t ul, char *src, size_t cl)
{
	z_stream z = { 0 };
	z.next_in = (unsigned char *)src;
	z.avail_in = (uint32_t) cl;
	z.zalloc = Z_NULL;
	z.zfree = Z_NULL;
	z.opaque = Z_NULL;

	if (inflateInit2(&z, 31) != Z_OK) { /* 15 bits window and 16 for gzip format */
		//error("Failed to initialize z_stream");
		return -1;
	}

	z.next_out = (unsigned char *)dest;
	z.avail_out = (uint32_t) ul;

	int res = inflate(&z, Z_FINISH);
	if (res != Z_OK && res != Z_STREAM_END) {
		printf("Failed to decompress GZIP block %d\n", res);
		return -10;
	}
	inflateEnd(&z); /* cleanup */
	return 0;
}
#endif

#ifdef HAVE_BROTLI
#include <brotli/decode.h>
#endif

typedef struct pqc_creader_t {
	pqc_file *pq;
	pqc_columnchunk *cc;

	int rowgroup;
	int64_t curnr;	/* row number with in current rowgroup */
	int64_t pos;	/* pos of buffered data in file */
	int64_t bufpos;	/* pos of per multipage buffer */
	int nr_bits;
	char *data;
	char *dict;
	char *odict;
	char first_definition;	/* first definition (alternating 0/1)  TODO handle levels */
	int *definition;	/* definition lengths */
	int definitionsize;	/* size of definition lengths */
	char first_repetition;	/* first repetition (alternating 0/1)  TODO handle levels */
	int *repetition;	/* repetition lengths */
	int repetitionsize;	/* size of repetition lengths */
	int curdef;
	int cursubdef;
	char *buffer;
	size_t bufsize;
	size_t datasize;
	bool data_allocated;
	bool dict_allocated;
	size_t dict_num_values;
	size_t dictsize;
	/* left over rle/bit packed */
	bool is_rle;
	uint32_t idx;
	uint32_t remaining;
} pqc_creader_t;

struct pqc_reader_t {
	struct pqc_reader_t *p;
	pqc_filemetadata *fmd;
	pqc_schema_element *pse;
	int rowgroup;
	int colnr;		/* colnr of this reader */
	int level;
	uint64_t sz;
	ATOMIC_TYPE rownr;
	const void *nil;

	int nrworkers;
	MT_Lock l;
	char *error;
	pqc_file *spq;
	pqc_creader_t *creader; /* per worker readers */
};

static void
pqc_set_error( pqc_reader_t *r, char *error )
{
	MT_lock_set(&r->l);
	if (!r->error)
		r->error = error;
	MT_lock_unset(&r->l);
}

char *
pqc_get_error( pqc_reader_t *r)
{
	return r->error;
}

static int64_t
pqc_statistics( pqc_reader_t *r, pqc_creader_t *pr, pqc_stat *stat, int64_t pos )
{
	(void)r;
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case STATISTICS_MAX: {
			assert(type == T_BINARY);
			char *t;
			int len;
			int res = pqc_get_string(pr->buffer+pos, &t, &len);
			//int res = pqc_string(pr, pr->buffer+pos, &stat->max_string);
			if (res < 0)
				return -1;
			pos += res;
			//TRC_INFO(PARQUET, "max_string '%s'\n", stat->max_string);
			TRC_INFO(PARQUET, "max_string '%s'\n", t);
#if 0
			} else if (type == 5) {
				pos += pqc_get_zint32(pr->buffer+pos, &stat->max);
				TRC_INFO(PARQUET, "max %d'\n", stat->max);
#endif
			} break;
		case STATISTICS_MIN: {
			assert(type == T_BINARY);
			char *t;
			int len;
			int res = pqc_get_string(pr->buffer+pos, &t, &len);
			//int res = pqc_string(pr, pr->buffer+pos, &stat->min_string);
			if (res < 0)
				return -1;
			pos += res;
			//TRC_INFO(PARQUET, "min_string '%s'\n", stat->min_string);
			TRC_INFO(PARQUET, "min_string '%s'\n", t);
#if 0
			} else if (type == 5) {
				pos += pqc_get_zint32(pr->buffer+pos, &stat->min);
				TRC_INFO(PARQUET, "min %d'\n", stat->min);
#endif
			} break;
		case STATISTICS_NULL_COUNT: {
			pos += pqc_get_int64(pr->buffer+pos, &stat->null_count);
			TRC_INFO(PARQUET, "null_count %" PRIu64 "\n", stat->null_count);
		}	break;
		case STATISTICS_DISTINCT_COUNT: {
			pos += pqc_get_int64(pr->buffer+pos, &stat->distinct_count);
			TRC_INFO(PARQUET, "distinct_count %" PRIu64 "\n", stat->distinct_count);
		}	break;
		case STATISTICS_MAX_VALUE: {
			assert(type == T_BINARY);
			char *t;
			int len;
			int res = pqc_get_string(pr->buffer+pos, &t, &len);
			//int res = pqc_string(pr, pr->buffer+pos, &stat->max_value);
			if (res < 0)
				return -1;
			pos += res;
			//TRC_INFO(PARQUET, "max_value '%s'\n", stat->max_value);
			TRC_INFO(PARQUET, "max_value '%s'\n", t);
#if 0
			} else if (type == 12) {
				pqc_keyvalue kv;
				pos = pqc_read_keyvalue(pr, &kv, pos);
#endif
			} break;
		case STATISTICS_MIN_VALUE: {
			assert(type == T_BINARY);
			char *t;
			int len;
			int res = pqc_get_string(pr->buffer+pos, &t, &len);
			//int res = pqc_string(pr, pr->buffer+pos, &stat->min_value);
			if (res < 0)
				return -1;
			pos += res;
			//TRC_INFO(PARQUET, "min_value '%s'\n", stat->min_value);
			TRC_INFO(PARQUET, "min_value '%s'\n", t);
#if 0
			} else if (type == 12) {
				pqc_keyvalue kv;
				pos = pqc_read_keyvalue(pr, &kv, pos);
#endif
			} break;
		case STATISTICS_IS_MAX_VALUE_EXACT:
			TRC_ERROR(PARQUET, "not handled field_id STATISTICS_IS_MAX_VALUE_EXACT");
			return -1;
		case STATISTICS_IS_MIN_VALUE_EXACT:
			TRC_ERROR(PARQUET, "not handled field_id STATISTICS_IS_MIN_VALUE_EXACT");
			return -1;
		default:
			TRC_ERROR(PARQUET, "UNKNOWN statistic field_id '%d'\n", fieldid);
			return -1;
		}
	}
	return pos;
}

static int64_t
pqc_index_page(pqc_reader_t *r, pqc_creader_t *pr, int64_t pos, uint32_t *num_values)
{
	(void)r;
	int fieldid = 0, type = 0;

	uint32_t encoding = 0;

	assert(0);
	/* list stats */
	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		// FIX these field ids seems not defined yet
		// https://github.com/apache/parquet-format/blob/730ab5de028a1e5aa2eb7537ed753cf3b2083200/src/main/thrift/parquet.thrift#L610
		switch (fieldid) {
		case 1:
			pos += pqc_get_zint32(pr->buffer+pos, num_values);
			TRC_INFO(PARQUET, "num_values %u\n", *num_values);
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

static int64_t
pqc_dictionary_page(pqc_reader_t *r, pqc_creader_t *pr, int64_t pos, uint32_t *num_values)
{
	(void)r;
	int fieldid = 0, type = 0;

	uint32_t encoding = 0;

	/* list stats */
	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case DICTIONARY_PAGE_HEADER_NUM_VALUES:
			pos += pqc_get_zint32(pr->buffer+pos, num_values);
			TRC_INFO(PARQUET, "num_values %u\n", *num_values);
			break;
		case DICTIONARY_PAGE_HEADER_ENCODING:
			pos += pqc_get_zint32(pr->buffer+pos, &encoding);
			TRC_INFO(PARQUET, "encoding %u\n", encoding);
			break;
		case DICTIONARY_PAGE_HEADER_IS_SORTED:
			TRC_INFO(PARQUET, "isSorted %d\n", type!=2);
			break;
		}
	}
	return pos;
}

static int64_t
pqc_data_page(pqc_reader_t *r, pqc_creader_t *pr, int64_t pos, uint32_t *num_values)
{
	int fieldid = 0, type = 0;

	uint32_t encoding = 0;
	uint32_t definition_level_encoding = 0;
	uint32_t repetition_level_encoding = 0;
	/* list stats */

	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case DATA_PAGE_HEADER_NUM_VALUES:
			pos += pqc_get_zint32(pr->buffer+pos, num_values);
			pr->cc->cur_page.num_values = *num_values;
			TRC_INFO(PARQUET, "num_values %u\n", *num_values);
			break;
		case DATA_PAGE_HEADER_ENCODING:
			pos += pqc_get_zint32(pr->buffer+pos, &encoding);
			pr->cc->cur_page.pageencodings[0].page_encoding = encoding;
			TRC_INFO(PARQUET, "page_encoding %u\n", encoding);
			break;
		case DATA_PAGE_HEADER_DEFINITION_LEVEL_ENCODING:
			pos += pqc_get_zint32(pr->buffer+pos, &definition_level_encoding);
			pr->cc->cur_page.pageencodings[1].page_encoding = definition_level_encoding;
			TRC_INFO(PARQUET, "definition_level_encoding %u\n", definition_level_encoding);
			break;
		case DATA_PAGE_HEADER_REPETITION_LEVEL_ENCODING:
			pos += pqc_get_zint32(pr->buffer+pos, &repetition_level_encoding);
			pr->cc->cur_page.pageencodings[2].page_encoding = repetition_level_encoding;
			TRC_INFO(PARQUET, "repetition_level_encoding %u\n", repetition_level_encoding);
			break;
		case DATA_PAGE_HEADER_STATISTICS:
			assert(type == T_STRUCT);
			pos = pqc_statistics(r, pr, &pr->cc->cur_page.stat, pos);
			break;
		}
	}
	return pos;
}

static int64_t
pqc_data_pageV2(pqc_reader_t *r, pqc_creader_t *pr, int64_t pos, uint32_t *num_values)
{
	int fieldid = 0, type = 0;

	uint32_t encoding = 0;
	uint32_t definition_levels_byte_length = 0;
	uint32_t repetition_levels_byte_length = 0;
	/* list stats */
	uint32_t num_nulls = 0, num_rows = 0;
	pr->cc->cur_page.is_compressed=1;

	while(true) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case DATA_PAGE_HEADER_V2_NUM_VALUES:
			pos += pqc_get_zint32(pr->buffer+pos, num_values);
			pr->cc->cur_page.num_values = *num_values;
			TRC_INFO(PARQUET, "num_values %u\n", *num_values);
			break;
		case DATA_PAGE_HEADER_V2_NUM_NULLS:
			pos += pqc_get_zint32(pr->buffer+pos, &num_nulls);
			pr->cc->cur_page.num_nulls = num_nulls;
			TRC_INFO(PARQUET, "num_nulls %u\n", num_nulls);
			break;
		case DATA_PAGE_HEADER_V2_NUM_ROWS:
			pos += pqc_get_zint32(pr->buffer+pos, &num_rows);
			pr->cc->cur_page.num_rows = num_rows;
			TRC_INFO(PARQUET, "num_rows %u\n", num_rows);
			break;
		case DATA_PAGE_HEADER_V2_ENCODING:
			pos += pqc_get_zint32(pr->buffer+pos, &encoding);
			pr->cc->cur_page.pageencodings[0].page_encoding = encoding;
			TRC_INFO(PARQUET, "page_encoding %u\n", encoding);
			break;
		case DATA_PAGE_HEADER_V2_DEFINITION_LEVELS_BYTE_LENGTH:
			pos += pqc_get_zint32(pr->buffer+pos, &definition_levels_byte_length);
			pr->cc->cur_page.definition_levels_byte_length = definition_levels_byte_length;
			TRC_INFO(PARQUET, "definition_levels_byte_length %u\n", definition_levels_byte_length);
			break;
		case DATA_PAGE_HEADER_V2_REPETITION_LEVELS_BYTE_LENGTH:
			pos += pqc_get_zint32(pr->buffer+pos, &repetition_levels_byte_length);
			pr->cc->cur_page.repetition_levels_byte_length = repetition_levels_byte_length;
			TRC_INFO(PARQUET, "repetition_levels_byte_length %u\n", repetition_levels_byte_length);
			break;
		case DATA_PAGE_HEADER_V2_IS_COMPRESSED:
			pr->cc->cur_page.is_compressed = (type != 2);
			TRC_INFO(PARQUET, "is_compressed %d\n", pr->cc->cur_page.is_compressed);
			break;
		case DATA_PAGE_HEADER_V2_STATISTICS:
			assert(type == T_STRUCT);
			assert(0);
			pqc_stat stat;
			pos = pqc_statistics(r, pr, &stat, pos);
			break;
		}
	}
	return pos;
}

static int
string_read_dict( pqc_creader_t *cr, uint32_t num_values)
/* in 32 bit len, string (without zero) */
{
	uint32_t i, hsz = 0;
	uint8_t *data = (uint8_t*)cr->dict;

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
	data = (uint8_t*)cr->dict;
	for (i=0; i<num_values; i++) {
		unsigned int len = get_uint32(data);

		data += sizeof(int);
		memcpy(buf, data, len);
		buf[len] = 0;
		offsets[i] = (int) (buf - obuf);
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

static int64_t
pqc_page_header( pqc_reader_t *r, pqc_creader_t *pr, int64_t pos)
{
	int fieldid = 0, type = 0;
	int64_t res = 0;

	/* list stats */
	uint32_t num_values = 0;
	uint32_t page_type = 0;
	uint32_t uncompressed_size = 0;
	uint32_t compressed_size = 0;
	uint32_t crc = 0;
	while(res >= 0) {
		pos += pqc_get_field(pr->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case PAGE_HEADER_TYPE:
			pos += (res = pqc_get_zint32(pr->buffer+pos, &page_type));
			TRC_INFO(PARQUET, "page type %u\n", page_type);
			break;
		case PAGE_HEADER_UNCOMPRESSED_PAGE_SIZE:
			pos += (res = pqc_get_zint32(pr->buffer+pos, &uncompressed_size));
			TRC_INFO(PARQUET, "uncompressed_size %u\n", uncompressed_size);
			break;
		case PAGE_HEADER_COMPRESSED_PAGE_SIZE:
			pos += (res = pqc_get_zint32(pr->buffer+pos, &compressed_size));
			TRC_INFO(PARQUET, "compressed_size %u\n", compressed_size);
			break;
		case PAGE_HEADER_CRC:
			pos += (res = pqc_get_zint32(pr->buffer+pos, &crc));
			TRC_INFO(PARQUET, "crc %u\n", crc);
			break;
		case PAGE_HEADER_DATA_PAGE_HEADER:
			assert(type == T_STRUCT);
			pos = res = pqc_data_page(r, pr, pos, &num_values);
			break;
		case PAGE_HEADER_INDEX_PAGE_HEADER:
			assert(type == T_STRUCT);
			pos = res = pqc_index_page(r, pr, pos, &num_values);
			break;
		case PAGE_HEADER_DICTIONARY_PAGE_HEADER:
			assert(type == T_STRUCT);
			pos = res = pqc_dictionary_page(r, pr, pos, &num_values);
			break;
		case PAGE_HEADER_DATA_PAGE_HEADER_V2:
			assert(type == T_STRUCT);
			pos = res = pqc_data_pageV2(r, pr, pos, &num_values);
			break;
		}
	}
	if (res < 0)
		return pos;
	assert(page_type == DATA_PAGE || page_type == DICTIONARY_PAGE || page_type == DATA_PAGE_V2);
	if (page_type == DATA_PAGE || page_type == DATA_PAGE_V2) {
		if (uncompressed_size && pos >= 0 && pr->cc->codec && (page_type != DATA_PAGE_V2 || pr->cc->cur_page.is_compressed)) {
			assert(pr->data == NULL);
			pr->data = NEW_ARRAY(char, uncompressed_size);
			if (!pr->data)
				return -1;
			pr->datasize = uncompressed_size;
			pr->data_allocated = true;
			size_t ul = uncompressed_size;
			if (pr->cc->codec == CC_SNAPPY) {
#ifdef HAVE_SNAPPY
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				if (snappy_uncompress(pr->buffer+pos + v2, compressed_size - v2, pr->data, &ul) != SNAPPY_OK)
					return -10;
				assert(uncompressed_size == ul);
				pos += compressed_size;
#else
				pqc_set_error(r, "Snappy compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_GZIP) {
#ifdef HAVE_LIBZ
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				if (gzip_uncompress(pr->data, ul, pr->buffer+pos + v2, compressed_size - v2))
					return -10;
				pos += compressed_size;
#else
				pqc_set_error(r, "gzip compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_ZSTD) {
#ifdef HAVE_ZSTD
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				if (ZSTD_decompress(pr->data, ul, pr->buffer+pos + v2, compressed_size - v2) != ul)
					return -10;
				pos += compressed_size;
#else
				pqc_set_error(r, "zstd compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_LZ4_RAW) {
#ifdef HAVE_LIBLZ4
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				int iul = (int)ul;
				if (LZ4_decompress_safe(pr->buffer+pos + v2, pr->data, compressed_size - v2, iul) != iul)
					return -10;
				pos += compressed_size;
#else
				pqc_set_error(r, "lz4 compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_BROTLI) {
#ifdef HAVE_BROTLI
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				if (BrotliDecoderDecompress(compressed_size - v2, (uint8_t*)pr->buffer+pos + v2, &ul, (uint8_t*)pr->data) != BROTLI_DECODER_RESULT_SUCCESS)
					return -10;
				pos += compressed_size;
#else
				pqc_set_error(r, "brotli compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_LZO) {
				pqc_set_error(r, "lzo compression support is not supported\n");
				return -1;
			} else if (pr->cc->codec == CC_LZ4) {
				pqc_set_error(r, "lz4 compression support is depricated use lz4_raw instead\n");
				return -1;
			}
		} else {
			pr->data_allocated = false;
			pr->data = pr->buffer+pos;
			pr->datasize = uncompressed_size;
			pos += uncompressed_size;
			if (!uncompressed_size)
				pos += compressed_size;
		}
		assert(!uncompressed_size || pr->datasize);
	}
	if (page_type == DICTIONARY_PAGE) {
		if (uncompressed_size && pos >= 0 && pr->cc->codec) {
			assert(pr->dict == NULL);
			pr->dict = NEW_ARRAY(char, uncompressed_size);
			if (!pr->dict)
				return -1;
			pr->dict_allocated = true;
			pr->dictsize = uncompressed_size;
			size_t ul = uncompressed_size;
			if (pr->cc->codec == CC_SNAPPY) {
#ifdef HAVE_SNAPPY
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				if (snappy_uncompress(pr->buffer+pos + v2, compressed_size - v2, pr->dict, &ul) != SNAPPY_OK)
					return -10;
				assert(uncompressed_size == ul);
				pos += compressed_size;
#else
				pqc_set_error(r, "Snappy compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_GZIP) {
#ifdef HAVE_LIBZ
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				if (gzip_uncompress(pr->dict, ul, pr->buffer+pos + v2, compressed_size - v2))
					return -10;
				pos += compressed_size;
#else
				pqc_set_error(r, "gzip compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_ZSTD) {
#ifdef HAVE_ZSTD
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				if (ZSTD_decompress(pr->dict, ul, pr->buffer+pos + v2, compressed_size - v2) != ul)
					return -10;
				pos += compressed_size;
#else
				pqc_set_error(r, "zstd compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_LZ4_RAW) {
#ifdef HAVE_LIBLZ4
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				int iul = (int)ul;
				if (LZ4_decompress_safe(pr->buffer+pos + v2, pr->dict, compressed_size - v2, iul) != iul)
					return -10;
				pos += compressed_size;
#else
				pqc_set_error(r, "lz4 compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_BROTLI) {
#ifdef HAVE_BROTLI
				/* for v2 add definition and repetition lengths */
				int v2 = pr->cc->cur_page.definition_levels_byte_length + pr->cc->cur_page.repetition_levels_byte_length;
				if (BrotliDecoderDecompress(compressed_size - v2, (uint8_t*)pr->buffer+pos + v2, &ul, (uint8_t*)pr->dict) != BROTLI_DECODER_RESULT_SUCCESS)
					return -10;
				pos += compressed_size;
#else
				pqc_set_error(r, "brotli compression support is not available\n");
				return -1;
#endif
			} else if (pr->cc->codec == CC_LZO) {
				pqc_set_error(r, "lzo compression support is not supported\n");
				return -1;
			} else if (pr->cc->codec == CC_LZ4) {
				pqc_set_error(r, "lz4 compression support is depricated use lz4_raw instead\n");
				return -1;
			}
		} else {
			pr->dict_allocated = false;
			pr->dict = pr->buffer+pos;
			pr->dictsize = uncompressed_size;
			pos += uncompressed_size;
			if (!uncompressed_size)
				pos += compressed_size;
		}
		if (r->pse->precision)
			pr->dict_num_values = pr->dictsize / (r->pse->size/8);
		if (num_values && r->pse->type == stringtype) {
			if (string_read_dict(pr, num_values) < 0)
				return -1;
		}
	}
	if (pos >= 0 && !pr->cc->dictionary_page_offset && pr->bufsize > (uint64_t)pos)
		pos = pqc_page_header( r, pr, pos);
	return pos;
}

pqc_reader_t *
pqc_reader( pqc_reader_t *p, pqc_file *pq, int nrworkers, /*pqc_columnchunk *cc, pqc_schema_element *pse,*/ pqc_filemetadata *fmd, int colnr, int64_t nrows, const void *nil)
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
	colnr = r->pse->ccnr;
	r->sz = nrows;
	r->colnr = colnr;
	r->rowgroup = -1;
	ATOMIC_INIT(&r->rownr, 0);
	r->nrworkers = nrworkers;
	r->spq = pq;
	r->nil = nil;
	r->error = NULL;
	MT_lock_init(&r->l, "pqc_reader");
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
		cr->first_repetition = 0;
		cr->repetition = 0;
		cr->repetitionsize = 0;
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
		if (cr->repetition)
			_DELETE(cr->repetition);
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
	MT_lock_destroy(&r->l);
	_DELETE(r->creader);
	pqc_close(r->spq);
	_DELETE(r);
}

static int64_t
pqc_read_dict( pqc_reader_t *r, pqc_creader_t *cr)
{
	int64_t pos = pqc_page_header(r, cr, 0);
	if (pos < 0)
		return -1;
	return pos;
}

static int64_t
pqc_repetition( pqc_reader_t *r, pqc_creader_t *cr, void *output, uint32_t num_values, uint32_t pos, int max)
{
	(void)max;
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

	(void)output;
	//int bits = 1;
	char *data = cr->data;
	uint32_t nr_bytes = get_uint32((uint8_t*)data+pos), len;
	uint32_t null = 0;
	int j = 0;

	if (cr->repetition)
		_DELETE(cr->repetition);
	cr->repetition = NULL;
	cr->first_repetition = 0;

	pos += sizeof(nr_bytes);
	uint32_t spos = pos;
	char prv = 0;
	for(uint32_t i = 0; i<num_values && ((pos-spos) < nr_bytes); ) {
		pos += pqc_get_int32(data+pos, &len);
		if (len & 1) {
			len>>=1;
			for(unsigned int b = 0; b < len; b++) {
				char val = data[pos++];
				for (unsigned int k=0; k<8; ) {
					char v = (val>>k)&1;
					int nlen = 1;
					for (k++; k<8; k++, nlen++) {
						if (v != ((val>>k)&1))
							break;
					}
					if (i == 0 && (val != 1 || len < num_values)) {
						cr->repetitionsize = 0;
						cr->first_repetition = v;
						prv = !v;
						cr->repetition = NEW_ARRAY(int, nr_bytes*8); /* should be enough */
						if (!cr->repetition)
							return -1;
					}
					if (cr->repetition) {
						if (v == prv) {
							cr->repetition[j-1] += nlen;
						} else {
							prv = v;
							cr->repetition[j++] = nlen;
						}
					}
					i+=nlen;
					null += (!v)*nlen;
				}
			}
		} else { /* rle */
			len>>=1;
			char val = data[pos++];
			assert(val == 0 || val == 1);
			if (i == 0 && (val != 1 || len < num_values)) {
				cr->repetitionsize = 0;
				cr->first_repetition = val;
				prv = !val;
				cr->repetition = NEW_ARRAY(int, nr_bytes*8); /* should be enough */
				if (!cr->repetition)
					return -1;
			}
			if (cr->repetition) {
				if (val == prv) {
					cr->repetition[j-1] += len;
				} else {
					prv = val;
					cr->repetition[j++] = len;
				}
			}
			assert(len);
			i+=len;
			null += (!val)*len;
		}
	}
	if (cr->repetition)
		cr->repetitionsize = j;

	TRC_DEBUG(PARQUET, "nulls %" PRIu32 " rows %u (%p)\n", null, num_values, cr->repetition);
	// printf( "nulls %" PRIu64 " rows %u (%p)\n", null, num_values, cr->repetition);
	cr->cc->cur_page.num_nulls = null;
	/* return repetition level as structure 0/1 + len */
	return pos; //nr_bytes + sizeof(nr_bytes);
}

static int64_t
pqc_definition( pqc_reader_t *r, pqc_creader_t *cr, void *output, uint32_t num_values, uint32_t pos, int max)
{
	(void)max;
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
	uint32_t nr_bytes = get_uint32((uint8_t*)data+pos), len;
	uint32_t null = 0;
	int j = 0;

	if (cr->definition)
		_DELETE(cr->definition);
	cr->definition = NULL;
	cr->first_definition = cr->curdef = cr->cursubdef = 0;

	pos += sizeof(nr_bytes);
	uint32_t spos = pos;
	char prv = 0;
	for(uint32_t i = 0; i<num_values && ((pos-spos) < nr_bytes); ) {
		pos += pqc_get_int32(data+pos, &len);
		if (len & 1) {
			len>>=1;
			for(unsigned int b = 0; b < len; b++) {
				char val = data[pos++];
				for (unsigned int k=0; k<8; ) {
					char v = (val>>k)&1;
					int nlen = 1;
					for (k++; k<8; k++, nlen++) {
						if (v != ((val>>k)&1))
							break;
					}
					if (i == 0 && (val != 1 || len < num_values)) {
						cr->definitionsize = 0;
						cr->first_definition = v;
						prv = !v;
						cr->definition = NEW_ARRAY(int, nr_bytes*8); /* should be enough */
						if (!cr->definition)
							return -1;
					}
					if (cr->definition) {
						if (v == prv) {
							cr->definition[j-1] += nlen;
						} else {
							prv = v;
							cr->definition[j++] = nlen;
						}
					}
					i+=nlen;
					null += (!v)*nlen;
				}
			}
		} else { /* rle */
			len>>=1;
			char val = data[pos++];
			if (i == 0 && (val != max || len < num_values)) {
				assert(max == 1); /* need to implement levels */
				cr->definitionsize = 0;
				cr->first_definition = val;
				prv = !val;
				cr->definition = NEW_ARRAY(int, nr_bytes*8); /* should be enough */
				if (!cr->definition)
					return -1;
			}
			if (cr->definition) {
				if (val == prv) {
					cr->definition[j-1] += len;
				} else {
					prv = val;
					cr->definition[j++] = len;
				}
			}
			assert(len);
			i+=len;
			null += (!val)*len;
		}
	}
	if (cr->definition)
		cr->definitionsize = j;

	TRC_DEBUG(PARQUET, "nulls %" PRIu32 " rows %u (%p)\n", null, num_values, cr->definition);
	cr->cc->cur_page.num_nulls = null;
	/* return definition level as structure 0/1 + len */
	return pos; //nr_bytes + sizeof(nr_bytes);
}

#define T uint8_t
#define pqc_dict_lookup pqc_dict_lookup_uchr
#include "pqc_dict_lookup.h"
#undef T
#undef pqc_dict_lookup

#define T uint16_t
#define pqc_dict_lookup pqc_dict_lookup_usht
#include "pqc_dict_lookup.h"
#undef T
#undef pqc_dict_lookup

#define T uint32_t
#define pqc_dict_lookup pqc_dict_lookup_uint
#include "pqc_dict_lookup.h"
#undef T
#undef pqc_dict_lookup

#define T ulng
#define pqc_dict_lookup pqc_dict_lookup_ulng
#include "pqc_dict_lookup.h"
#undef T
#undef pqc_dict_lookup


#define T uint8_t
#define pqc_dict_lookup pqc_dict_lookup_var_uchr
#include "pqc_dict_lookup_var.h"
#undef T
#undef pqc_dict_lookup

#define T uint16_t
#define pqc_dict_lookup pqc_dict_lookup_var_usht
#include "pqc_dict_lookup_var.h"
#undef T
#undef pqc_dict_lookup

#define T uint32_t
#define pqc_dict_lookup pqc_dict_lookup_var_uint
#include "pqc_dict_lookup_var.h"
#undef T
#undef pqc_dict_lookup
/*
 *  TODO: multi page columnchunk, will have multiple dicts
 */
static int64_t
pqc_dict_lookup( pqc_reader_t *r, pqc_creader_t *cr, void *output, void *voutput, int64_t nrows, int pos, int *ssize, int *dict)
{
	uint8_t *data = (uint8_t*)cr->data;
	/* asume rle data page */
	if (r->pse->precision == 0 && !output) {
		if (ssize) {
			*ssize = cr->dictsize;
			if (!cr->curnr)
				cr->nr_bits = data[pos];
			assert(cr->dictsize);
			return 0;
		}
	}

	/* rle / bit packed */
	if (!cr->curnr)
		cr->nr_bits = data[pos++];
	int nr_bits = cr->nr_bits;

	if (r->pse->precision == 0 && !dict) {
		int64_t i = 0;
		char **dst = output;
		bool mul8 = ((8/nr_bits)*nr_bits == 8);
		uint32_t idx = cr->idx;
		if (cr->remaining) {
			if (cr->is_rle) {
				uint32_t j = 0;
				for(; i < nrows && j < cr->remaining; j++, i++)
					dst[i] = ((char**)cr->dict)[idx];
				cr->remaining -= j;
			} else {
				assert(0);
			}
		}
		for(; i<nrows; ) {
			uint32_t len = 0;
			pos += pqc_get_int32((char*)data+pos, &len);
			if (len & 1) {
				len>>=1;
				/* only 2 bits for now */
				int sh = 0;
				int mask = (1<<nr_bits) -1;
				if (mul8) { /* todo handle 16 / 32 multiples */
					int m = len*8;
					for (int64_t j = 0; i < nrows && j < m; j++, i++) {
						uint8_t v = data[pos];
						uint32_t idx = (v >> sh)&mask;
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
						uint8_t v = data[pos];
						uint32_t idx = (v >> sh)&mask;
						sh += nr_bits;
						if (sh >= 8) {
							pos++;
							sh -= 8;
							uint8_t v = data[pos];
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
						uint16_t v = *(uint16_t*)(data+pos);
						uint32_t idx = (v >> sh)&mask;
						sh += nr_bits;
						if (sh >= 16) {
							pos+=2;
							sh -= 16;
							uint16_t v = *(uint16_t*)(data+pos);
							idx |= (v << (nr_bits-sh))&mask;
							if (j==(m-1) && sh >= 8)
								pos++;
							else if (j==(m-1) && sh > 0)
								pos+=2;
						}
						assert(idx < cr->dict_num_values);
						dst[i] = ((char**)cr->dict)[idx];
					}
				} else if (nr_bits < 32) {
					int m = len*8;
					for (int64_t j = 0; i < nrows && j < m; j++, i++) {
						uint32_t v = *(uint32_t*)(data+pos);
						uint32_t idx = (v >> sh)&mask;
						sh += nr_bits;
						if (sh >= 32) {
							pos+=2;
							sh -= 32;
							uint32_t v = *(uint32_t*)(data+pos);
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
				uint8_t idx = data[pos++];
				uint32_t j = 0;
				for(; i < nrows && j < len; j++, i++)
					dst[i] = ((char**)cr->dict)[idx];
				if (j < len) {
					cr->is_rle = true;
					cr->remaining = len - j;
					cr->idx = idx;
				}
			} else if (nr_bits <= 16) { /* rle */
				len>>=1;
				uint16_t idx = *(uint16_t*)(data+pos);
				uint32_t j = 0;
				pos += 2;
				for(; i < nrows && j < len; j++, i++)
					dst[i] = ((char**)cr->dict)[idx];
				if (j < len) {
					cr->is_rle = true;
					cr->remaining = len - j;
					cr->idx = idx;
				}
			} else if (nr_bits <= 32) { /* rle */
				len>>=1;
				uint32_t idx = *(uint32_t*)(data+pos);
				uint32_t j = 0;
				pos += 4;
				for(; i < nrows && j < len; j++, i++)
					dst[i] = ((char**)cr->dict)[idx];
				if (j < len) {
					cr->is_rle = true;
					cr->remaining = len - j;
					cr->idx = idx;
				}
			}
		}
	} else if (r->pse->precision == 0 && dict) { /* offsets */
		memcpy(voutput, cr->dict + cr->dict_num_values * (sizeof(char*)+sizeof(int)), cr->dictsize);
		if (*dict == 1) {
			return pqc_dict_lookup_var_uchr( cr, output, nrows, pos, ssize);
		} else if (*dict == 2) {
			return pqc_dict_lookup_var_usht( cr, output, nrows, pos, ssize);
		} else {
			assert(*dict == 4);
			return pqc_dict_lookup_var_uint( cr, output, nrows, pos, ssize);
		}
	} else if (r->pse->precision == 8) {
		return pqc_dict_lookup_uchr( cr, output, nrows, pos);
	} else if (r->pse->precision == 16) {
		return pqc_dict_lookup_usht( cr, output, nrows, pos);
	} else if (r->pse->precision == 32) {
		return pqc_dict_lookup_uint( cr, output, nrows, pos);
	} else if (r->pse->precision == 64 || (r->pse->precision < 8 && r->pse->size == 64)) {
		return pqc_dict_lookup_ulng( cr, output, nrows, pos);
	} else {
		printf("later %d\n", r->pse->precision);
	}
	cr->pos = pos;
	return nrows;
}

static int64_t
string_read_chunk( pqc_creader_t *cr, char **rc, char *buf, int64_t nrows, int pos)
{
	if (nrows > cr->cc->cur_page.num_values)
		nrows = cr->cc->cur_page.num_values;

	char *data = cr->data + pos;
	for (int64_t i=0; i<nrows; i++) {
		unsigned int len = get_uint32((uint8_t*)data);

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

	if (cr->cc->cur_page.stat.null_count == cr->cc->cur_page.num_values) { /* all null */
		assert(cr->cc->cur_page.num_nulls == cr->cc->cur_page.num_values);
		*ssize = 5;
		return 0;
	}

	data += pos;
	int cur = cr->curdef, sdef = cr->cursubdef;
	int def = cr->first_definition;
	int dend = cr->definitionsize;
	for(int64_t i = 0; i<nrows && cur<dend; ) {
		int len = cr->definition[cur];
		len -= sdef;
		if (i + len > nrows) {
			len = nrows-i;
			sdef += len;
		} else {
			sdef = 0;
		}
		if (def) {
			unsigned int slen = get_uint32((uint8_t*)data);
			data += slen + sizeof(int);
			hsz += slen+1;
		}
		i+=len;
		if (!sdef) {
			def = !def;
			cur++;
		}
	}
	assert(cur<=dend);
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
	if (nrows > cr->cc->cur_page.num_values)
		nrows = cr->cc->cur_page.num_values;

	if (cr->cc->cur_page.stat.null_count == cr->cc->cur_page.num_values) { /* all null */
		assert(cr->cc->cur_page.num_nulls == cr->cc->cur_page.num_values);
		*ssize = 5;
		return 0;
	}

	for (int64_t i=0; i<nrows; i++) {
		unsigned int len = get_uint32((uint8_t*)data);

		data += len + sizeof(int);
		hsz += len+1;
	}
	*ssize = hsz;
	return 0;
}

#define T uint8_t
#define offset_string_read_chunk offset_string_read_chunk_uchr
#include "offset_string_read_chunk.h"
#undef T
#undef offset_string_read_chunk

#define T uint16_t
#define offset_string_read_chunk offset_string_read_chunk_usht
#include "offset_string_read_chunk.h"
#undef T
#undef offset_string_read_chunk

#define T uint32_t
#define offset_string_read_chunk offset_string_read_chunk_uint
#include "offset_string_read_chunk.h"
#undef T
#undef offset_string_read_chunk

static int64_t
offset_string_read_chunk( pqc_reader_t *r, pqc_creader_t *cr, void *output, void *voutput, int64_t nrows,
						  int pos, int offset, int width)
{
	if (width == 1) {
		return offset_string_read_chunk_uchr( r, cr, output, voutput, nrows, pos, offset);
	} else if (width == 2) {
		return offset_string_read_chunk_usht( r, cr, output, voutput, nrows, pos, offset);
	} else {
		return offset_string_read_chunk_uint( r, cr, output, voutput, nrows, pos, offset);
	}
	return -1;
}

static int64_t
pqc_read_delta( pqc_creader_t *cr, int *prefixes, int64_t nrows, int pos)
{
	uint64_t blocks = 0, miniblocks = 0, count = 0;
	uint64_t val = 0;

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
offset_read_strings_sht( pqc_creader_t *cr, uint16_t *rc, char *buf, int *lengths, int64_t nrows, int pos, int offset)
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
	(void) cr;
	(void) pos;

	for (int64_t i = 0; i<nrows; i++) {
		int len = lengths[i];

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
pqc_mark_chunk( pqc_reader_t *r, int nr_workers, int wnr, uint64_t nrows)
{
	uint64_t orows = nrows;
	if (ATOMIC_GET(&r->rownr) >= r->sz)
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

static uint64_t
pqc_nrows( pqc_creader_t *cr, uint64_t nrows)
{
	if (cr->cc->cur_page.num_nulls == nrows)
		return 0;

	int cur = cr->curdef, sdef = cr->cursubdef;
	int def = cr->first_definition;

	if (cr->definition) {
		/* first find last */
		for (uint64_t i = 0; i<nrows; ) {
			int len = cr->definition[cur];
			len -= sdef;
			if (i + len > nrows) {
				sdef = len;
				len = nrows-i;
				sdef -= len;
			} else {
				sdef = 0;
			}
			i += len;
			if (!sdef) {
				def = !def;
				cur++;
			}
		}
		cr->curdef = cur;
		cr->cursubdef = sdef;
		cr->first_definition = def;
	}
	return nrows;
}

static int
pqc_project( char *output, char *data, uint64_t nrows, int w)
{
	if (w == 1) {
		int sh = 0, j = 0;
		for(uint64_t i = 0; i < nrows; i++) {
			output[i] = (data[j]>>sh)&1;
			sh++;
			if (sh == 8) {
				sh = 0;
				j++;
			}
		}
		return j;
	} else {
		assert(0);
	}
	return 0;
}

static int
pqc_add_nil( pqc_reader_t *r, pqc_creader_t *cr, char *output, char *data, uint64_t nrows, uint64_t nils, int w)
{
	int cur = cr->curdef, sdef = cr->cursubdef;
	int def = cr->first_definition;
	int pos = 0;

	if (nils == nrows){ // all null
		switch(w) {
		case 1:
			for(uint64_t i = 0; i<nrows; i++)
				output[i] = *(char*)r->nil;
			break;
		case 2:
			for(uint64_t i = 0; i<nrows; i++)
				((sht*)output)[i] = *(sht*)r->nil;
			break;
		case 4:
			for(uint64_t i = 0; i<nrows; i++)
				((int*)output)[i] = *(int*)r->nil;
			break;
		case 8:
			for(uint64_t i = 0; i<nrows; i++)
				((lng*)output)[i] = *(lng*)r->nil;
			break;
#ifdef HAVE_HGE
		case 16:
			for(uint64_t i = 0; i<nrows; i++)
				((hge*)output)[i] = *(hge*)r->nil;
			break;
#endif
		default:
			for(uint64_t i = 0, j = 0; i<nrows; i++, j+=w)
				memcpy(output+j, r->nil, w);
			break;
		}
		return 0;
	}
	cur = cr->curdef;
	sdef = cr->cursubdef;
	def = cr->first_definition; /* set by pqc_nrows at the end of this block */
	if (!sdef)
		def = !def;
	pos = nrows*w;
	for(uint64_t i = nrows; i > 0 && cur >= 0; cur--){
		int len = cr->definition[cur];
		len -= sdef;
		sdef = 0;
		if (i < (uint64_t)len)
			len = i;
		i -= len;
		if (def) {
			pos -= w*len;
			memcpy(output+w*i, data+pos, len*w);
		} else {
			for(int j = 0; j<len; j++)
				memcpy(output+w*(i+j), r->nil, w);
		}
		def = !def;
	}
	return pos;
}

static int
pqc_max_repetition( pqc_schema_element *pse)
{
	int repetition = 0;
	for(; pse; pse = pse->parent) {
		if (pse->repetition == 2)
			repetition++;
	}
	return repetition;
}

static int
pqc_max_definition( pqc_schema_element *pse)
{
	int definition = 0;
	for(; pse; pse = pse->parent) {
		if (pse->repetition > 0)
			definition++;
	}
	return definition;
}

#define YEAR_OFFSET -(-4712)
#define DTDAY_WIDTH			 5			   /* 1..28/29/30/31, depending on month/year */
#define DTDAY_SHIFT			 0
#define DTMONTH_WIDTH   21			  /* enough for 174761 years (and 8 months) */
#define DTMONTH_SHIFT   (DTDAY_WIDTH+DTDAY_SHIFT)
#define mkdate(y, m, d) (((uint32_t) (((y) + YEAR_OFFSET) * 12 + (m) - 1) << DTMONTH_SHIFT) | ((uint32_t) (d) << DTDAY_SHIFT))

int64_t
pqc_read_chunk( pqc_reader_t *r, int wnr, void *output /*fixed sized atom storage */, void *voutput /* var storage */, uint64_t nrows, int *ssize, int *dict)
{
	if (ATOMIC_GET(&r->rownr) >= r->sz)
		return 0;

	pqc_creader_t *cr = r->creader+wnr;
	if (cr->rowgroup >= r->fmd->nrowgroups)
		return 0;
	if (cr->cc && cr->curnr + nrows > cr->cc->nrows)
		nrows = cr->cc->nrows - cr->curnr;

	if (ATOMIC_GET(&r->rownr) + nrows > r->sz)
		nrows = r->sz - ATOMIC_GET(&r->rownr);
	if (nrows == 0)
		return 0;
	if (cr->pos < 0 || cr->cc->cur_page.num_read == cr->cc->cur_page.num_values) {
		cr->cc->cur_page.num_read = 0;
		int64_t pos = cr->bufpos;
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

			pos = 0;
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
		} else {
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
			cr->dict = NULL; /* TODO possible multi page dict usage (check encoding!) */
			cr->dict_allocated = false;

			if (cr->definition)
				_DELETE(cr->definition);

		}
		pos = pqc_page_header(r, cr, pos);
		if (pos < 0)
			return -1;
		/* read data */
		if (cr->cc->cur_page.num_values < nrows)
			nrows = cr->cc->cur_page.num_values;
		cr->bufpos = pos;
		if (cr->data) {
			pos = 0;
			cr->pos = pos;
			int repetition = pqc_max_repetition(r->pse);
			int definition = pqc_max_definition(r->pse);
			if (repetition) {
				/* bit vector for null's */
				pos = pqc_repetition(r, cr, output, cr->cc->cur_page.num_values, (uint32_t)pos, repetition);
				cr->pos = pos;
			}
			if (definition) {
				/* bit vector for null's */
				pos = pqc_definition(r, cr, output, cr->cc->cur_page.num_values, (uint32_t)pos, definition);
				cr->pos = pos;
			}
			if (pos < 0)
				return pos;
			uint64_t orows = nrows;
			if (r->pse->precision == 0 && !output)
				orows = 0;
			else
				nrows = pqc_nrows(cr, nrows);
			if (cr->dict) {
				if (cr->cc->cur_page.num_values - cr->cc->cur_page.num_nulls)
					nrows = pqc_dict_lookup(r, cr, output, voutput, nrows, pos, ssize, dict);
				if (cr->definition)
					(void)pqc_add_nil(r, cr, output, output, orows, cr->cc->cur_page.num_nulls, r->pse->size/8);
			} else {
				if (cr->cc->cur_page.pageencodings[0].page_encoding) {
					/* delta strings == delta_encoded (encoding==5) prefixes (lengths),
					 * delta_length_byte_array((encoding==6) == (delta encoded lengths, back2back strings)) suffix strings */
					nrows = pqc_read_delta_strings(cr, output, voutput, nrows, pos, ssize, dict);
				} else if (r->pse->type != stringtype) {
					/* fixed types, plain encoding */
					if (!cr->definition) {
						if ((r->pse->size/8)*8 != r->pse->size) {
							pos += pqc_project(output, ((char*)cr->data)+pos, nrows, r->pse->size);
						} else if (r->pse->physical_type == PT_BYTE_ARRAY ||
								   r->pse->physical_type == PT_FIXED_LEN_BYTE_ARRAY) {
							char *dst = output;
							/* FIX ARRAY little endian len followed by big endian data,  what where they smoking */
							for (uint32_t i = 0; i < nrows; i++, dst += 2) {
								int len = *(int*)(((char*)cr->data)+pos);
								pos += 4;
								*(sht*)dst = 0;
								memcpy(dst, ((char*)cr->data)+pos, len);
								if (len == 2)
									*(sht*)dst = short_int_SWAP(*(sht*)dst);
								pos += len;
							}
						} else {
							memcpy(output, ((char*)cr->data)+pos, nrows*(r->pse->size/8));
							pos += (r->pse->size/8)*nrows;
						}
					} else {
						pos += pqc_add_nil(r, cr, output, cr->data+pos, orows, cr->cc->cur_page.num_nulls, r->pse->size/8);
					}
					cr->pos = pos;
				} else if (r->pse->type == stringtype) {
					/* int32 len, string (no zero) encoding */
					if (!ssize) {
						string_read_chunk(cr, output, voutput, nrows, pos);
						if (cr->definition)
							(void)pqc_add_nil(r, cr, output, output, orows, cr->cc->cur_page.num_nulls, r->pse->size/8);
					} else if (!voutput) {
						return string_size_chunk(cr, nrows, pos, ssize, dict);
					} else
						offset_string_read_chunk(r, cr, output, voutput, nrows, pos, *ssize, *dict);
				}
			}
			/* convert data */
			if (r->pse->type == datetype) {
				/* days since epoch ie 1-1-1970 */
				uint32_t epoch_date = mkdate(1970, 1, 1);
				if (r->pse->precision == 32) {
					uint32_t *l = output;
					for(uint64_t i=0; i< nrows; i++) {
						l[i] = date_add_day(epoch_date, l[i]);
					}
				}
			} else // todo timetype
				if (r->pse->type == timestamptype) {
					if (r->pse->precision == 6) {
						int64_t *l = output;
						for(uint64_t i=0; i< nrows; i++) {
							l[i] = timestamp_fromusec(l[i]);
						}
					}
				}
			ATOMIC_ADD(&r->rownr, orows);
			cr->curnr += orows;
			cr->cc->cur_page.num_read += orows;
			return orows;
		}
	} else {
		assert(cr->cc->cur_page.num_read < cr->cc->cur_page.num_values);
		if (cr->cc->cur_page.num_values - cr->cc->cur_page.num_read < nrows)
			nrows = cr->cc->cur_page.num_values - cr->cc->cur_page.num_read;
		/* next vector */
		if (cr->data) {
			int pos = cr->pos;
			uint64_t orows = nrows;
			if (r->pse->precision == 0 && !output)
				orows = 0;
			else
				nrows = pqc_nrows(cr, nrows);
			if (cr->dict) {
				if (cr->cc->cur_page.num_values - cr->cc->cur_page.num_nulls)
					nrows = pqc_dict_lookup(r, cr, output, voutput, nrows, pos, ssize, dict);
				if (cr->definition)
					(void)pqc_add_nil(r, cr, output, output, orows, cr->cc->cur_page.num_nulls, r->pse->size/8);
			} else {
				if (cr->cc->cur_page.pageencodings[0].page_encoding) {
					/* delta strings == delta_encoded (encoding==5) prefixes (lengths),
					 * delta_length_byte_array((encoding==6) == (delta encoded lengths, back2back strings)) suffix strings */
					nrows = pqc_read_delta_strings(cr, output, voutput, nrows, pos, ssize, dict);
				} else if (r->pse->type != stringtype) {
					/* fixed types, plain encoding */
					if (!cr->definition) {
						if ((r->pse->size/8)*8 != r->pse->size) {
							pos += pqc_project(output, ((char*)cr->data)+pos, nrows, r->pse->size);
						} else if (r->pse->physical_type == PT_BYTE_ARRAY ||
								   r->pse->physical_type == PT_FIXED_LEN_BYTE_ARRAY) {
							/* FIX ARRAY little endian len followed by big endian data,  what where they smoking */
							char *dst = output;
							for (uint32_t i = 0; i < nrows; i++, dst += 2) {
								int len = *(int*)(((char*)cr->data)+pos);
								pos += 4;
								*(sht*)dst = 0;
								memcpy(dst, ((char*)cr->data)+pos, len);
								if (len == 2)
									*(sht*)dst = short_int_SWAP(*(sht*)dst);
								pos += len;
							}
						} else {
							memcpy(output, ((char*)cr->data)+pos, nrows*(r->pse->size/8));
							pos += (r->pse->size/8)*nrows;
						}
					} else {
						pos += pqc_add_nil(r, cr, output, cr->data+pos, orows, cr->cc->cur_page.num_nulls, r->pse->size/8);
					}
					cr->pos = pos;
				} else if (r->pse->type == stringtype) {
					/* int32 len, string (no zero) encoding */
					if (!ssize) {
						string_read_chunk(cr, output, voutput, nrows, pos);
						if (cr->definition)
							(void)pqc_add_nil(r, cr, output, output, orows, cr->cc->cur_page.num_nulls, r->pse->size/8);
					} else if (!voutput) {
						return string_size_chunk(cr, nrows, pos, ssize, dict);
					} else
						offset_string_read_chunk(r, cr, output, voutput, nrows, pos, *ssize, *dict);
				}
			}
			/* convert data */
			if (r->pse->type == datetype) {
				/* days since epoch ie 1-1-1970 */
				uint32_t epoch_date = mkdate(1970, 1, 1);
				if (r->pse->precision == 32) {
					uint32_t *l = output;
					for(uint64_t i=0; i< nrows; i++) {
						l[i] = date_add_day(epoch_date, l[i]);
					}
				}
			} else // todo timetype
				if (r->pse->type == timestamptype) {
					if (r->pse->precision == 6) {
						int64_t *l = output;
						for(uint64_t i=0; i< nrows; i++) {
							l[i] = timestamp_fromusec(l[i]);
						}
					}
				}
			ATOMIC_ADD(&r->rownr, orows);
			cr->curnr += orows;
			cr->cc->cur_page.num_read += orows;
			return orows;
		}
	}
	return 0;
}
