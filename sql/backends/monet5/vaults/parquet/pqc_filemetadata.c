/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "pqc_thrift.h"
#include "pqc_filemetadata.h"

#include <stdbool.h>
#include <stdio.h>  // printf
#include <string.h> // memcpy
#include <unistd.h> // read
#include <fcntl.h>  // open
#include <assert.h>

#include <mutils.h>
#include <gdk.h>
#include <sql_mem.h>

#define PQC_SMALL 64*1024

/* Types */
typedef enum oldtype {
  OLD_BOOLEAN = 0,
  OLD_INT32 = 1,
  OLD_INT64 = 2,
  OLD_INT96 = 3,  // deprecated, only used by legacy implementations.
  OLD_FLOAT = 4,
  OLD_DOUBLE = 5,
  OLD_BYTE_ARRAY = 6,
  OLD_FIXED_LEN_BYTE_ARRAY = 7
} oldtype;


/*
 * pqc_file
 */
struct pqc_file {
	int refcnt;
	char *filename;
	int fd;
	int level;
	pqc_filemetadata *fmd;
	bool read_schema;

	size_t sz;
	size_t pos;	/* pos of buffered data in file */
	allocator *pa;
	size_t bsz;
	char *buffer;
	MT_Lock lock;
};

#define PQC_MAGIC(c) ((c)[0] == 'P' && (c)[1] == 'A' && (c)[2] == 'R' && (c)[3] == '1')

static int
pqc_magic( pqc_file *pq)
{
	if (pq->sz < 8) {
		TRC_ERROR(PARQUET, "This is a broken parquet file %s (size too small)\n", pq->filename);
		return -1;
	} else if (pq->sz <= pq->bsz &&
		(!PQC_MAGIC(pq->buffer) ||
				!PQC_MAGIC(pq->buffer+pq->bsz-4))) {
		TRC_ERROR(PARQUET, "This is a broken parquet file %s (missing magic number)\n", pq->filename);
		return -1;
	} else if (!PQC_MAGIC(pq->buffer+pq->bsz-4)) {
		TRC_ERROR(PARQUET, "This is a broken parquet file %s (missing magic number)\n", pq->filename);
		return -1;
	}
	return 0;
}

static int
pqc_string( pqc_file *pq, char *in, char **s)
{
	char *t = NULL;
	int len = 0;
	int pos = pqc_get_string(in, &t, &len);

	if (len > 0) {
		*s = ma_alloc(pq->pa, len+1);
		memcpy(*s, t, len);
		(*s)[len] = 0;
		if (!(*s))
			return -1;
	}
	return pos;
}

static pqc_file *
pqc_create(void)
{
	pqc_file *pq = ZNEW(pqc_file);
	if (!pq)
		return pq;
	pq->refcnt = 1;
	pq->fd = -1;
	pq->pa = create_allocator("pqc_create", false);
	if (!pq->pa) {
		_DELETE(pq);
		pq = NULL;
	}
	MT_lock_init(&pq->lock, "pqc_file");
	return pq;
}

static void
pqc_destroy(pqc_file *pq)
{
	if (--pq->refcnt > 0)
		return;
	MT_lock_destroy(&pq->lock);
	if (pq->pa)
		ma_destroy(pq->pa);
	if (pq->fd >= 0)
		close(pq->fd);
	pq->fmd = NULL;
	_DELETE(pq);
}

pqc_file *
pqc_dup(pqc_file *pq)
{
	pq->refcnt++;
	return pq;
}

#ifndef O_BINARY
#define O_BINARY 0
#endif
#ifdef NATIVE_WIN32
static inline int64_t
mylseek(int fd, int64_t offset, int whence)
{
	return (int64_t) _lseeki64(fd, (__int64) offset, whence);
}
#else
#define mylseek(fd, offset, whence)	lseek(fd, (off_t) (offset), whence)
#endif

pqc_file *
pqc_copy(pqc_file *opq)
{
	//return pqc_dup(opq);
	pqc_file *pq = ZNEW(pqc_file);
	if (!pq)
		return pq;
	pq->refcnt = 1;
	pq->filename = opq->filename;
	pq->fd = open(opq->filename, O_RDONLY | O_BINARY);
	pq->pa = create_allocator("pqc_copy", false);
	if (!pq->pa) {
		_DELETE(pq);
		pq = NULL;
	}
	MT_lock_init(&pq->lock, "pqc_file");
	return pq;
}

int64_t
pqc_read( pqc_file *pq, int64_t offset, char *buffer, int sz)
{
	MT_lock_set(&pq->lock);
	if (offset == 0 && sz < PQC_SMALL) {
		if (mylseek(pq->fd, offset, SEEK_SET) != offset)
			offset = -3;
		if (read(pq->fd, buffer, sz) != sz)
			offset = -3;
	} else {
		if (mylseek(pq->fd, offset, SEEK_SET) != offset)
			offset = -3;
		if (read(pq->fd, buffer, sz) != sz)
			offset = -4;
	}
	MT_lock_unset(&pq->lock);
	return offset;
}

int
pqc_open( pqc_file **PQ, char *fn)
{
	char buffer[1024];
	pqc_file *pq = pqc_create();
	if (!pq)
		return -2;
	pq->filename = fn;
	pq->fd = open(fn, O_RDONLY);
	if (pq->fd < 0) {
		pqc_destroy(pq);
		return -1;
	}
	struct stat stb;
    if (MT_stat(fn, &stb) != 0) {
		pqc_destroy(pq);
		return -1;
	}
	pq->sz = stb.st_size;
	pq->buffer = NULL;
	pq->bsz = 0;

	TRC_DEBUG(PARQUET, "fd = %d, file_size = %zu\n", pq->fd, pq->sz);

	ssize_t sz = pq->sz;
	int64_t offset = 0;
	if (sz > 1024) {
		sz = 1024;
		offset = pq->sz - sz;
	}
	if (offset && mylseek(pq->fd, offset, SEEK_SET) != offset) {
		pqc_destroy(pq);
		return -3;
	}
	/* cast to int for Windows; we know it fits, and Linux automatically
	 * casts back */
	if (read(pq->fd, buffer, (int) sz) != sz) {
		pqc_destroy(pq);
		return -3;
	}
	pq->buffer = buffer;
	pq->bsz = sz;
	*PQ = pq;
	int res = pqc_magic(pq);

	uint32_t fmdlen = get_uint32((uint8_t*)pq->buffer+pq->bsz-8);
	if (pq->sz == 8) /* empty */
		return 0;
	if (fmdlen > pq->sz)
		return -1;

	pq->buffer = SA_NEW_ARRAY(pq->pa, char, fmdlen);
	pq->bsz = fmdlen;
	pq->pos = 0;
	TRC_DEBUG(PARQUET, "fmdlen %u\n", fmdlen);

	offset = (pq->sz > pq->bsz) ? pq->sz - (pq->bsz + 8) : 0;
	if (offset && mylseek(pq->fd, offset, SEEK_SET) != offset) {
		pqc_destroy(pq);
		return -3;
	}
	if (read(pq->fd, pq->buffer, fmdlen) != (ssize_t)pq->bsz) {
		pqc_destroy(pq);
		return -3;
	}
	return res;
}

void
pqc_close( pqc_file *pq)
{
	pqc_destroy(pq);
}

static size_t
pqc_struct( pqc_file *pq, size_t pos)
{
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		pos = pqc_struct(pq, pos);
	}
	return pos;
}

static size_t
pqc_timeunit( pqc_file *pq, pqc_schema_element *pse, size_t pos)
{
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case TIME_UNIT_MILLIS:
			pse->precision = 3;
			break;
		case TIME_UNIT_MICROS:
			pse->precision = 6;
			break;
		case TIME_UNIT_NANOS:
			printf("nano\n");
			pse->precision = 9;
			break;
		}
		assert(type == T_STRUCT); /* empty struct */
		pos = pqc_struct(pq, pos);
	}
	return pos;
}

static size_t
pqc_timestamp( pqc_file *pq, pqc_schema_element *pse, size_t pos)
{
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case TIMESTAMP_TYPE_IS_ADJUSTED_TO_UTC:
			/* get bool from type */
			pse->isAdjustedToUTC = type!=2;
			break;
		case TIMESTAMP_TYPE_UNIT:
			assert(type == T_STRUCT);
			pos = pqc_timeunit(pq, pse, pos);
			break;
		}
	}
	return pos;
}

static size_t
pqc_decimal( pqc_file *pq, pqc_schema_element *pse, size_t pos)
{
	int fieldid = 0, type = 0;

	uint32_t scale, precision;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case DECIMAL_TYPE_SCALE:
			pos += pqc_get_zint32(pq->buffer+pos, &scale);
			TRC_DEBUG(PARQUET, "scale %u\n", scale);
			pse->scale = scale;
			break;
		case DECIMAL_TYPE_PRECISION:
			pos += pqc_get_zint32(pq->buffer+pos, &precision);
			TRC_DEBUG(PARQUET, "precision %u\n", precision);
			pse->precision = precision;
			break;
		}
	}
	return pos;
}

static size_t
pqc_integer( pqc_file *pq, pqc_schema_element *pse, size_t pos)
{
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case INT_TYPE_BIT_WIDTH:
			type = pq->buffer[pos++];
			TRC_DEBUG(PARQUET, "bitwidth %d\n", type);
			pse->precision = type;
			break;
		case INT_TYPE_IS_SIGNED:
			pse->isSigned = type != T_BOOLEAN_FALSE;
			TRC_DEBUG(PARQUET, "isSigned %d\n", type != T_BOOLEAN_FALSE);
			break;
		}
	}
	return pos;
}

static size_t
pqc_logicaltype( pqc_file *pq, pqc_schema_element *pse, size_t pos)
{
	int fieldid = 0, type = 0;

	uint32_t precision;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case LOGICAL_TYPE_STRING:
			assert(type == T_STRUCT); /* empty struct */
			pos = pqc_struct(pq, pos);
			pse->type = stringtype;
			break;
		case LOGICAL_TYPE_MAP:
			// TODO Is this correct?
			pos += pqc_get_zint32(pq->buffer+pos, &precision);
			TRC_DEBUG(PARQUET, "precision %u\n", precision);
			pse->precision = precision;
			break;
		case LOGICAL_TYPE_LIST:
			assert(type == T_STRUCT); /* empty struct */
			pos = pqc_struct(pq, pos);
			pse->type = listtype;
			break;
		case LOGICAL_TYPE_ENUM:
			pos = pqc_struct(pq, pos);
			pse->type = enumtype;
			break;
		case LOGICAL_TYPE_DECIMAL: /* decimal */
			pos = pqc_decimal(pq, pse, pos);
			break;
		case LOGICAL_TYPE_DATE: /* date */
			assert(type == T_STRUCT); /* empty struct */
			pos = pqc_struct(pq, pos);
			pse->type = datetype;
			pse->precision = 32;
			break;
		case LOGICAL_TYPE_TIME:
			TRC_ERROR(PARQUET, "ERROR No support for LOGICAL_TYPE_TIME");
			return (size_t) -1;
		case LOGICAL_TYPE_TIMESTAMP: /* timestamp */
			pos = pqc_timestamp(pq, pse, pos);
			break;
		case LOGICAL_TYPE_INTEGER: /* int */
			pos = pqc_integer(pq, pse, pos);
			break;
		case LOGICAL_TYPE_UNKNOWN: /* NULL */
			pos = pqc_struct(pq, pos);
			pse->type = stringtype; /* no idea !! */
			break;
		case LOGICAL_TYPE_JSON:
			TRC_ERROR(PARQUET, "no support for LOGICAL_TYPE_JSON");
			return (size_t) -1;
		case LOGICAL_TYPE_BSON:
			TRC_ERROR(PARQUET, "no support for LOGICAL_TYPE_BSON");
			return (size_t) -1;
		case LOGICAL_TYPE_UUID:
			TRC_ERROR(PARQUET, "no support for LOGICAL_TYPE_UUID");
			return (size_t) -1;
		case LOGICAL_TYPE_FLOAT16:
			TRC_ERROR(PARQUET, "no support for LOGICAL_TYPE_FLOAT16");
			return (size_t) -1;
		default:
			TRC_ERROR(PARQUET, "no support or unknown LOGICAL_TYPE  %d\n", fieldid);
			return (size_t) -1;
		}
	}
	return pos;
}

static int
pqc_oldtype2logicaltype( pqc_schema_element *pse, uint32_t type)
{
	switch (type) {
	case OLD_BOOLEAN:
		pse->type = enumtype; /* special case TRUE/FALSE */
		pse->binary = true;
		pse->precision = 1;
		pse->size = 1;
		break;
	case OLD_INT32:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 32;
		pse->size = 32;
		pse->isSigned = true;
		break;
	case OLD_INT64:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 64;
		pse->size = 64;
		pse->isSigned = true;
		break;
	case OLD_INT96:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 96;
		pse->size = 96;
		pse->isSigned = true;
		break;
	case OLD_FLOAT:
		pse->type = floattype;
		pse->binary = true;
		pse->precision = 32;
		pse->size = 32;
		pse->isSigned = true;
		break;
	case OLD_DOUBLE:
		pse->type = floattype;
		pse->binary = true;
		pse->precision = 64;
		pse->size = 64;
		pse->isSigned = true;
		break;
	case OLD_BYTE_ARRAY:
	case OLD_FIXED_LEN_BYTE_ARRAY:
		//pse->type = stringtype;
		pse->type = blobtype;
		pse->precision = 0;
		break;
	}
	return 0;
}

static int
pqc_convertedtype2logicaltype( pqc_schema_element *pse, uint32_t type)
{
	switch (type) {
	case CT_UTF8:
		pse->type = stringtype;
		break;
	case CT_MAP:
	case CT_MAP_KEY_VALUE:
		pse->type = maptype;
		break;
	case CT_LIST:
		pse->type = listtype;
		break;
	case CT_ENUM:
		//pse->type = enumtype;
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 8;
		pse->size = 32;
		pse->isSigned = false;
		break;
	case CT_DECIMAL:
		pse->type = decimaltype;
		pse->binary = true;
		pse->precision = 18;
		pse->isSigned = true;
		break;
	case CT_DATE:
		pse->type = datetype;
		pse->binary = true;
		break;
	case CT_TIME_MILLIS:
	case CT_TIME_MICROS:
		pse->type = timetype;
		pse->binary = true;
		pse->precision = (type == CT_TIME_MILLIS)?0:1;
		break;
	case CT_TIMESTAMP_MILLIS:
	case CT_TIMESTAMP_MICROS:
		pse->type = timestamptype;
		pse->binary = true;
		pse->precision = (type == CT_TIME_MILLIS)?0:1;
		break;
	case CT_UINT_8:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 8;
		pse->size = 32;
		pse->isSigned = false;
		break;
	case CT_UINT_16:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 16;
		pse->size = 32;
		pse->isSigned = false;
		break;
	case CT_UINT_32:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 32;
		pse->size = 32;
		pse->isSigned = false;
		break;
	case CT_UINT_64:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 64;
		pse->size = 64;
		pse->isSigned = false;
		break;
	case CT_INT_8:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 8;
		pse->size = 32;
		pse->isSigned = true;
		break;
	case CT_INT_16:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 16;
		pse->size = 32;
		pse->isSigned = true;
		break;
	case CT_INT_32:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 32;
		pse->size = 32;
		pse->isSigned = true;
		break;
	case CT_INT_64:
		pse->type = inttype;
		pse->binary = true;
		pse->precision = 64;
		pse->size = 64;
		pse->isSigned = true;
		break;
	case CT_JSON:
		pse->type = jsontype;
		break;
	case CT_BSON:
		pse->type = bsontype;
		break;
	case CT_INTERVAL:
		pse->type = intervaltype;
		pse->precision = 0;
		break;
	}
	return 0;
}

static size_t
pqc_read_schema_element( pqc_file *pq, int nr, size_t pos, int *ccnr, pqc_schema_element *parent)
{
	int fieldid = 0, type = 0;
	pqc_schema_element *pse = pq->fmd->elements + nr;
	*pse = (pqc_schema_element) {
		.physical_type = PT_UNKNOWN,
		.converted_type = CT_UNKNOWN,
		.type = LT_UNKNOWN,
	};

	if (parent) {
		parent->elements[parent->curchild++] = pse;
		pse->parent = parent;
	}
	pse->ccnr = *ccnr;
	uint32_t oldtype, scale, precision, repetition, nchildren, convertedtype;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case SCHEMA_ELEMENT_TYPE:
			pos += pqc_get_zint32(pq->buffer+pos, &oldtype);
			if (pqc_oldtype2logicaltype(pse, oldtype) < 0)
				TRC_ERROR(PARQUET, "type %u not handled\n", oldtype);
			TRC_INFO(PARQUET, "old type %u to type %d\n", oldtype, pse->type);
			pse->physical_type = oldtype;
			break;
		case SCHEMA_ELEMENT_TYPE_LENGTH:
			pos += pqc_get_zint32(pq->buffer+pos, &precision);
			if (pse->type != stringtype) {
				TRC_ERROR(PARQUET, "precision %u used with wrong type %d\n", precision, pse->type);
				return (size_t) -1;
			}
			pse->type_length = precision;
			TRC_INFO(PARQUET, "precision %u\n", precision);
			break;
		case SCHEMA_ELEMENT_REPETITION_TYPE:
			/* TODO make enum */
			assert(type == T_I32);
			pos += pqc_get_zint32(pq->buffer+pos, &repetition);
			assert(repetition <= 2);
			pse->repetition = repetition;
			TRC_INFO(PARQUET, "repetition %s\n", repetition==0?"required":repetition==1?"optional":"repeated");
			break;
		case SCHEMA_ELEMENT_NAME: {
			assert(type == T_BINARY);
			int res = pqc_string(pq, pq->buffer+pos, &pse->name);
			if (res < 0) {
				TRC_ERROR(PARQUET, "failure reading SCHEMA_ELEMENT_NAME");
				return (size_t) -1;
			}
			pos += res;
			TRC_INFO(PARQUET, "name %s\n", pse->name);
		} break;
		case SCHEMA_ELEMENT_NUM_CHILDREN:
			pos += pqc_get_zint32(pq->buffer+pos, &nchildren);
			pse->nchildren = nchildren;
			pse->elements = SA_NEW_ARRAY(pq->pa, pqc_schema_element*, nchildren);
			TRC_INFO(PARQUET, "nchildren %u\n", nchildren);
			break;
		case SCHEMA_ELEMENT_CONVERTED_TYPE:
			pos += pqc_get_zint32(pq->buffer+pos, &convertedtype);
			if (pqc_convertedtype2logicaltype(pse, convertedtype) < 0)
				TRC_ERROR(PARQUET, "converted type %u not handled\n", convertedtype);
			TRC_INFO(PARQUET, "convertedtype %u to type %d\n", convertedtype, pse->type);
			pse->converted_type = convertedtype;
			break;
		case SCHEMA_ELEMENT_SCALE:
			pos += pqc_get_zint32(pq->buffer+pos, &scale);
			if (pse->type != decimaltype) {
				TRC_ERROR(PARQUET, "scale %u used with wrong type %d\n", scale, pse->type);
				return (size_t) -1;
			}
			pse->scale = scale;
			TRC_INFO(PARQUET, "scale %u\n", scale);
			break;
		case SCHEMA_ELEMENT_PRECISION:
			pos += pqc_get_zint32(pq->buffer+pos, &precision);
			if (pse->type != decimaltype) {
				TRC_ERROR(PARQUET, "precision %u used with wrong type %d\n", precision, pse->type);
				return (size_t) -1;
			}
			pse->precision = precision;
			TRC_INFO(PARQUET, "precision %u\n", precision);
			break;
		case SCHEMA_ELEMENT_FIELD_ID:
			assert(type == T_I32);
			uint32_t _field_id;
			pos += pqc_get_zint32(pq->buffer+pos, &_field_id);
			// TODO store _field_id in schema element?
			break;
		case SCHEMA_ELEMENT_LOGICAL_TYPE:
			assert(type == T_STRUCT);
			pos = pqc_logicaltype(pq, pse, pos);
			if (pos == (size_t) -1){
				TRC_ERROR(PARQUET, "failure reading SCHEMA_ELEMENT_LOGICAL_TYPE");
				return pos;
			}
			break;
		default:
			assert(0);
		}
	}
	if (pse->type != LT_UNKNOWN && pse->type != listtype)
		(*ccnr)++;
	return pos;
}

static size_t
pqc_read_keyvalue( pqc_file *pq, pqc_keyvalue *kv, size_t pos )
{
	int fieldid = 0, type = 0;
	*kv = (pqc_keyvalue){};

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case KEY_VALUE_KEY: {
			int res = pqc_string(pq, pq->buffer+pos, &kv->key);
			if (res < 0)
				return -1;
			pos += res;
			TRC_INFO(PARQUET, "key %s\n", kv->key);
		} break;
		case KEY_VALUE_VALUE: {
			int res = pqc_string(pq, pq->buffer+pos, &kv->value_string);
			if (res < 0)
				return -1;
			pos += res;
			TRC_INFO(PARQUET, "value '%s'\n", kv->value_string);
		} break;
		case 3: {
			pos += pqc_get_zint64(pq->buffer+pos, &kv->value);
			TRC_INFO(PARQUET, "value %" PRIu64 "\n", kv->value);
		} break;
		}
	}
	return pos;
}

static size_t
pqc_statistics( pqc_file *pq, pqc_stat *stat, size_t pos )
{
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case STATISTICS_MAX: {
			if (type == T_BINARY) {
				int res = pqc_string(pq, pq->buffer+pos, &stat->max_string);
				if (res < 0)
					return -1;
				pos += res;
				TRC_INFO(PARQUET, "max_string '%s'\n", stat->max_string);
			} else if (type == T_I32) {
				pos += pqc_get_zint32(pq->buffer+pos, &stat->max);
				TRC_INFO(PARQUET, "max %u'\n", stat->max);
			}
		} break;
		case STATISTICS_MIN: {
			if (type == T_BINARY) {
				int res = pqc_string(pq, pq->buffer+pos, &stat->min_string);
				if (res < 0)
					return -1;
				pos += res;
				TRC_INFO(PARQUET, "min_string '%s'\n", stat->min_string);
			} else if (type == T_I32) {
				pos += pqc_get_zint32(pq->buffer+pos, &stat->min);
				TRC_INFO(PARQUET, "min %u'\n", stat->min);
			}
		} break;
		case STATISTICS_NULL_COUNT: {
			pos += pqc_get_zint64(pq->buffer+pos, &stat->null_count);
			TRC_INFO(PARQUET, "null_count %" PRIu64 "\n", stat->null_count);
		}	break;
		case STATISTICS_DISTINCT_COUNT: {
			pos += pqc_get_zint64(pq->buffer+pos, &stat->distinct_count);
			TRC_INFO(PARQUET, "distinct_count %" PRIu64 "\n", stat->distinct_count);
		}	break;
		case STATISTICS_MAX_VALUE: {
			if (type == T_BINARY) {
				int res = pqc_string(pq, pq->buffer+pos, &stat->max_value);
				if (res < 0)
					return -1;
				pos += res;
				TRC_INFO(PARQUET, "max_value '%s'\n", stat->max_value);
			} else if (type == T_STRUCT) {
				pqc_keyvalue kv;
				pos = pqc_read_keyvalue(pq, &kv, pos);
			}
		} break;
		case STATISTICS_MIN_VALUE: {
			if (type == T_BINARY) {
				int res = pqc_string(pq, pq->buffer+pos, &stat->min_value);
				if (res < 0)
					return -1;
				pos += res;
				TRC_INFO(PARQUET, "min_value '%s'\n", stat->min_value);
			} else if (type == T_STRUCT) {
				pqc_keyvalue kv;
				pos = pqc_read_keyvalue(pq, &kv, pos);
			}
		} break;
		case STATISTICS_IS_MAX_VALUE_EXACT:
			stat->max_is_exact = type != 2;
			TRC_INFO(PARQUET, "STATISTICS_IS_MAX_VALUE_EXACT %d", stat->max_is_exact);
			break;
		case STATISTICS_IS_MIN_VALUE_EXACT:
			stat->min_is_exact = type != 2;
			TRC_INFO(PARQUET, "STATISTICS_IS_MIN_VALUE_EXACT %d", stat->min_is_exact);
			break;
		default:
			TRC_ERROR(PARQUET, "UNKNOWN statistic field_id '%d'\n", fieldid);
			return -1;
		}
	}
	return pos;
}

static size_t
pqc_pageencodingsstats( pqc_file *pq, pqc_pageencodings *pe, size_t pos )
{
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case PAGE_ENCODING_STATS_PAGE_TYPE:
			pos += pqc_get_zint32(pq->buffer+pos, &pe->page_type);
			TRC_INFO(PARQUET, "page_type %u\n", pe->page_type);
			break;
		case PAGE_ENCODING_STATS_ENCODING:
			pos += pqc_get_zint32(pq->buffer+pos, &pe->page_encoding);
			TRC_INFO(PARQUET, "page_encoding %u\n", pe->page_encoding);
			break;
		case PAGE_ENCODING_STATS_COUNT:
			pos += pqc_get_zint32(pq->buffer+pos, &pe->page_count);
			TRC_INFO(PARQUET, "page_count %u\n", pe->page_count);
			break;
		}
	}
	return pos;
}

static size_t
pqc_column_metadata( pqc_file *pq, pqc_columnchunk *cc, size_t pos )
{
	int fieldid = 0, type = 0, size = 0;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case COLUMN_META_DATA_TYPE:
			pos += pqc_get_zint32(pq->buffer+pos, &cc->type);
			TRC_INFO(PARQUET, "column type %u\n", cc->type);
			break;
		case COLUMN_META_DATA_ENCODINGS: {
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "encodings list size %d type %d\n", size, type);
			assert(type == T_I32);
			uint32_t encoding;
			cc->encodings = ma_alloc(pq->pa, size * sizeof(uint32_t));
			for(int i = 0; i < size; i++) {
				pos += pqc_get_zint32(pq->buffer+pos, &encoding);
				cc->encodings[i] = encoding;
				TRC_INFO(PARQUET, "\tencoding[%d] %u\n", i, cc->encodings[i]);
			}
			cc->num_encodings = size;
		} break;
		case COLUMN_META_DATA_PATH_IN_SCHEMA:
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "path_in_schema list size %d type %d\n", size, type);
			char *path_in_schema_str = "";
			for(int i = 0; i < size; i++) {
				assert(type == T_BINARY);
				char *cname = NULL;
				int res = pqc_string(pq, pq->buffer+pos, &cname);
				if (res < 0)
					return -1;
				if (cname) {
					path_in_schema_str = ma_strconcat(pq->pa, path_in_schema_str, cname);
					if (i < size - 1)
						path_in_schema_str = ma_strconcat(pq->pa, path_in_schema_str, ", ");
				}
				pos += res;
			}
			if (strlen(path_in_schema_str) > 0)
				cc->path_in_schema = path_in_schema_str;
			TRC_INFO(PARQUET, "path_in_schema %s\n", cc->path_in_schema);
			break;
		case COLUMN_META_DATA_CODEC: {
			uint32_t v;
			pos += pqc_get_zint32(pq->buffer+pos, &v);
			cc->codec = (CompressionCodec) v;
			TRC_INFO(PARQUET, "compression codec %u\n", cc->codec);
			break;
		}
		case COLUMN_META_DATA_NUM_VALUES:
			pos += pqc_get_zint64(pq->buffer+pos, &cc->nrows);
			TRC_INFO(PARQUET, "num values %" PRIu64 "\n", cc->nrows);
			cc->num_values = cc->nrows;
			break;
		case COLUMN_META_DATA_TOTAL_UNCOMPRESSED_SIZE:
			pos += pqc_get_zint64(pq->buffer+pos, &cc->total_uncompressed_size);
			TRC_INFO(PARQUET, "total_uncompressed_size %" PRIu64 "\n", cc->total_uncompressed_size);
			break;
		case COLUMN_META_DATA_TOTAL_COMPRESSED_SIZE:
			pos += pqc_get_zint64(pq->buffer+pos, &cc->total_compressed_size);
			TRC_INFO(PARQUET, "total_compressed_size %" PRIu64 "\n", cc->total_compressed_size);
			break;
		case COLUMN_META_DATA_KEY_VALUE_METADATA:
			assert(type == T_LIST); /* LIST */
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "key value list size %d type %d\n", size, type);
			cc->nkeyvalues = size;
			cc->keyvalues = SA_NEW_ARRAY(pq->pa, pqc_keyvalue, size);
			for(int i = 0; i < size; i++)
				pos = pqc_read_keyvalue(pq, cc->keyvalues+i, pos);
			break;
		case COLUMN_META_DATA_DATA_PAGE_OFFSET:
			pos += pqc_get_zint64(pq->buffer+pos, &cc->data_page_offset);
			TRC_INFO(PARQUET, "data_page_offset %" PRIu64 "\n", cc->data_page_offset);
			break;
		case COLUMN_META_DATA_INDEX_PAGE_OFFSET:
			pos += pqc_get_zint64(pq->buffer+pos, &cc->index_page_offset);
			TRC_INFO(PARQUET, "index_page_offset %" PRIu64 "\n", cc->index_page_offset);
			break;
		case COLUMN_META_DATA_DICTIONARY_PAGE_OFFSET:
			pos += pqc_get_zint64(pq->buffer+pos, &cc->dictionary_page_offset);
			TRC_INFO(PARQUET, "dictionary_page_offset %" PRIu64 "\n", cc->dictionary_page_offset);
			break;
		case COLUMN_META_DATA_STATISTICS:
			pos = pqc_statistics(pq, &cc->stat, pos);
			break;
		case COLUMN_META_DATA_ENCODING_STATS:
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "pageencodingstats list size %d type %d\n", size, type);
			assert(size <= 3);
			for(int i = 0; i < size; i++)
				pos = pqc_pageencodingsstats(pq, cc->pageencodings+i, pos);
			break;
		}
	}
	if (cc->data_page_offset == 2147574643 && cc->dictionary_page_offset == 0  && cc->type != 5) {
		assert(0);
	}
	return pos;
}

static size_t
pqc_read_columnchunk( pqc_file *pq, pqc_columnchunk *cc, size_t pos )
{
	int fieldid = 0, type = 0;
	*cc = (pqc_columnchunk) { };

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case COLUMN_CHUNK_FILE_PATH: {
			assert(type == T_BINARY);
			int res = pqc_string(pq, pq->buffer+pos, &cc->file_path);
			if (res < 0)
				return -1;
			TRC_INFO(PARQUET, "name %s\n", cc->file_path);
			pos += res;
		}	break;
		case COLUMN_CHUNK_FILE_OFFSET: {
			pos += pqc_get_zint64(pq->buffer+pos, &cc->file_offset);
			TRC_INFO(PARQUET, "file_offset %" PRIu64 "\n", cc->file_offset);
		} break;
		case COLUMN_CHUNK_META_DATA:
			assert(type == T_STRUCT);
			pos = pqc_column_metadata(pq, cc, pos);
			break;
		case COLUMN_CHUNK_OFFSET_INDEX_OFFSET:
			pos += pqc_get_zint64(pq->buffer+pos, &cc->offset_index_offset);
			TRC_INFO(PARQUET, "offset_index_offset %" PRIu64 "\n", cc->offset_index_offset);
			break;
		case COLUMN_CHUNK_OFFSET_INDEX_LENGTH:
			pos += pqc_get_zint32(pq->buffer+pos, &cc->offset_index_length);
			TRC_INFO(PARQUET, "offset_index_length %u\n", cc->offset_index_length);
			break;
		case COLUMN_CHUNK_COLUMN_INDEX_OFFSET:
			pos += pqc_get_zint64(pq->buffer+pos, &cc->column_index_offset);
			TRC_INFO(PARQUET, "column_index_offset %" PRIu64 "\n", cc->column_index_offset);
			break;
		case COLUMN_CHUNK_COLUMN_INDEX_LENGTH:
			pos += pqc_get_zint32(pq->buffer+pos, &cc->column_index_length);
			TRC_INFO(PARQUET, "column_index_length %u\n", cc->column_index_length);
			break;
		case COLUMN_CHUNK_CRYPTO_METADATA:
			TRC_ERROR(PARQUET, "No support for field_id COLUMN_CHUNK_CRYPTO_METADATA");
			return -1;
		case COLUMN_CHUNK_ENCRYPTED_COLUMN_METADATA:
			TRC_ERROR(PARQUET, "No support for field_id COLUMN_CHUNK_ENCRYPTED_COLUMN_METADATA");
			return -1;
		default:
			TRC_ERROR(PARQUET, "No support for field_id %d type %d\n", fieldid, type);
			return -1;
		}
	}
	return pos;
}

static size_t
pqc_read_sortingcolumn( pqc_file *pq, pqc_sortingcolumn *sc, size_t pos)
{
	int fieldid = 0, type = 0;
	*sc = (pqc_sortingcolumn){ };

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case SORTING_COLUMN_COLUMN_IDX :
			assert (type == T_I32);
			pos += pqc_get_zint32(pq->buffer+pos, &sc->column_idx);
			TRC_INFO(PARQUET, "column_idx %u\n", sc->column_idx);
			break;
		case SORTING_COLUMN_DESCENDING:
			if (type == T_BOOLEAN_TRUE)
				sc->descending = true;
			else
				sc->descending = false;
			TRC_INFO(PARQUET, "descending %d\n", sc->descending);
			break;
		case SORTING_COLUMN_NULLS_FIRST:
			if (type == T_BOOLEAN_TRUE)
				sc->nulls_first = true;
			else
				sc->nulls_first = false;
			TRC_INFO(PARQUET, "nulls_first %d\n", sc->nulls_first);
			break;
		}
	}
	return pos;
}

static size_t
pqc_rowgroup( pqc_file *pq, pqc_row_group *rg, size_t pos)
{
	int fieldid = 0, type = 0, size = 0;
	*rg = (pqc_row_group){ };

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);
		if (!type)
			break;
		switch (fieldid) {
		case ROW_GROUP_COLUMNS:
			assert (type == T_LIST /* LIST */);
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "columnchunk list size %d type %d\n", size, type);
			rg->ncolumnchunks = size;
			rg->columnchunks = SA_NEW_ARRAY(pq->pa, pqc_columnchunk, size);
			for(int i = 0; i < size; i++)
				pos = pqc_read_columnchunk(pq, rg->columnchunks+i, pos);
			break;
		case ROW_GROUP_TOTAL_BYTE_SIZE: {
			assert (type == T_I64);
			pos += pqc_get_zint64(pq->buffer+pos, &rg->total_byte_size);
			TRC_INFO(PARQUET, "total_byte_size %" PRIu64 "\n", rg->total_byte_size);
		}	break;
		case ROW_GROUP_NUM_ROWS: {
			assert (type == T_I64);
			pos += pqc_get_zint64(pq->buffer+pos, &rg->num_rows);
			TRC_INFO(PARQUET, "num_rows %" PRIu64 "\n", rg->num_rows);
		}	break;
		case ROW_GROUP_SORTING_COLUMNS: {
			assert (type == T_LIST /* LIST */);
			/* sorting columns */
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "sortingcolumns list size %d type %d\n", size, type);
			rg->nsortingcolumns = size;
			rg->sortingcolumns = SA_NEW_ARRAY(pq->pa, pqc_sortingcolumn, size);
			for(int i = 0; i < size; i++)
				pos = pqc_read_sortingcolumn(pq, rg->sortingcolumns+i, pos);
		}	break;
		case ROW_GROUP_FILE_OFFSET: {
			assert (type == T_I64);
			pos += pqc_get_zint64(pq->buffer+pos, &rg->file_offset);
			TRC_INFO(PARQUET, "file_offset %" PRIu64 "\n", rg->file_offset);
		}	break;
		case ROW_GROUP_TOTAL_COMPRESSED_SIZE: {
			assert (type == T_I64);
			pos += pqc_get_zint64(pq->buffer+pos, &rg->total_compressed_size);
			TRC_INFO(PARQUET, "total_compressed_size %" PRIu64 "\n", rg->total_compressed_size);
		}	break;
		case ROW_GROUP_ORDINAL:
			/* i16 (written as int32) */
			pos += pqc_get_zint32(pq->buffer+pos, &rg->ordinal);
			TRC_INFO(PARQUET, "ordinal %u\n", rg->ordinal);
			break;
		}
	}
	return pos;
}

static size_t
pqc_columnorder( pqc_file *pq, size_t pos )
{
	int fieldid = 0, type = 0;

	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);

		if (!type)
			break;
		switch (fieldid) {
		case 1: {
			assert(type == T_STRUCT); /* empty struct */
			pos = pqc_struct(pq, pos);
		} break;
		}
	}
	return pos;
}

static int
pqc_read_file( pqc_file *pq, bool metadata_only)
{
	size_t pos = pq->pos;
	uint32_t version = 0;
	uint64_t nrows = 0;

	int fieldid = 0, type = 0, size = 0;

	pq->fmd = SA_NEW(pq->pa, pqc_filemetadata);
	pqc_filemetadata *fmd = pq->fmd;
	*fmd = (pqc_filemetadata) {0};
	if (!pq->fmd) {
		TRC_ERROR(PARQUET, "PQC: alloc failed\n");
		return -1;
	}
	while(true) {
		pos += pqc_get_field(pq->buffer+pos, &fieldid, &type);
		TRC_DEBUG(PARQUET, "field id %d type %d\n", fieldid, type);

		if (!type)
			break;

		switch(fieldid) {
		case FILE_METADATA_VERSION: /*I32*/
			assert(type == T_I32);
			pos += pqc_get_zint32(pq->buffer+pos, &version);
			TRC_INFO(PARQUET, "version %u\n", version);
			fmd->version = version;
			break;
		case FILE_METADATA_SCHEMA:
			assert(type == T_LIST); /* LIST */
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "schema element list size %d type %d\n", size, type);
			fmd->nelements = size;
			fmd->elements = SA_NEW_ARRAY(pq->pa, pqc_schema_element, size);
			if (!fmd->elements) {
				TRC_ERROR(PARQUET, "PQC: alloc failed\n");
				return -1;
			}
			pqc_schema_element *parent = NULL;
			for(int i = 0, ccnr = 0; i < size; i++) {
				pos = pqc_read_schema_element(pq, i, pos, &ccnr, parent);
				if (pos == (size_t) -1)
					return -1;
				pqc_schema_element *pse = fmd->elements+i;

				if (pse->nchildren > 0) {
					parent = pse;
				} else {
					while (parent && parent->nchildren == parent->curchild) {
						parent = parent->parent;
					}
				}
			}
			break;
		case FILE_METADATA_NUM_ROWS: /* I64 */
			assert(type == T_I64 || type == T_I32 /* seems some files are broken!! */);
			pos += pqc_get_zint64(pq->buffer+pos, &nrows);
			TRC_INFO(PARQUET, "nrows %" PRIu64 "\n", nrows);
			fmd->nrows = nrows;
			pq->read_schema = true;
			if (metadata_only)
				return 0;
			break;
		case FILE_METADATA_ROW_GROUPS:
			assert(type == T_LIST); /* LIST */
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "row groups list size %d type %d\n", size, type);
			fmd->nrowgroups = size;
			fmd->rowgroups = SA_NEW_ARRAY(pq->pa, pqc_row_group, size);
			if (!fmd->rowgroups) {
				TRC_ERROR(PARQUET, "PQC: alloc failed\n");
				return -1;
			}
			for(int i = 0; i < size; i++)
				pos = pqc_rowgroup(pq, pq->fmd->rowgroups+i, pos);
			break;
		case FILE_METADATA_KEY_VALUE_METADATA:
			assert(type == T_LIST); /* LIST */
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			TRC_INFO(PARQUET, "key value list size %d type %d\n", size, type);
			fmd->nkeyvalues = size;
			fmd->keyvalues = SA_NEW_ARRAY(pq->pa, pqc_keyvalue, size);
			for(int i = 0; i < size; i++)
				pos = pqc_read_keyvalue(pq, fmd->keyvalues+i, pos);
			break;
		case FILE_METADATA_CREATED_BY: {
			assert(type == T_BINARY); /*string */
			char *created_by = NULL;
			pos += pqc_get_string(pq->buffer+pos, &created_by, &type);
			TRC_INFO(PARQUET, "created_by %s\n", created_by);
			fmd->created_by = SA_STRDUP(pq->pa, created_by);
		} break;
		case FILE_METADATA_COLUMN_ORDERS:
			assert(type == T_LIST); /* LIST */
			pos += pqc_get_list(pq->buffer+pos, &size, &type);
			assert(size < fmd->nelements);
			TRC_INFO(PARQUET, "columnorder list size %d type %d\n", size, type);
			for(int i = 0; i < size; i++)
				pos = pqc_columnorder(pq, pos);
			break;
		case FILE_METADATA_ENCRYPTION_ALGORITHM:
			TRC_ERROR(PARQUET, "No support for field_id FILE_METADATA_ENCRYPTION_ALGORITHM");
			return -1;
		case FILE_METADATA_FOOTER_SIGNING_KEY_METADATA:
			TRC_ERROR(PARQUET, "No support for field_id FILE_METADATA_FOOTER_SIGNING_KEY_METADATA");
			return -1;
		default:
			TRC_ERROR(PARQUET, "No support for field_id %d type %d\n", fieldid, type);
			return -1;
		}
	}
	return 0;
}

int
pqc_read_schema( pqc_file *pq )
{
	return pqc_read_file(pq, true);
}

/* returns (after call to pqc_read_filemetadata) array with schema_elements and number of elements */
const pqc_schema_element *
pqc_get_schema_elements( pqc_file *pq, int *nr)
{
	if (pq->read_schema) {
		*nr = pq->fmd->nelements;
		return pq->fmd->elements;
	} else {
		return NULL;
	}
}

int
pqc_read_filemetadata( pqc_file *pq )
{
	return pqc_read_file(pq, false);
}

pqc_filemetadata *
pqc_get_filemetadata( pqc_file *pq )
{
	assert(pq);
	return pq->fmd;
}
