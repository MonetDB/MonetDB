/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "mapi.h"
#include "monetdbe_mapi.h"

#define MAPIalloc(sz) malloc(sz)
#define MAPIfree(p)   free(p)

MapiMsg
mapi_error(Mapi mid)
{
	if (mid->msg)
		return MERROR;
	return MOK;
}

MapiHdl
mapi_query(Mapi mid, const char *query)
{
	MapiHdl mh = (MapiHdl)MAPIalloc(sizeof(struct MapiStatement));

	mh->mid = mid;
	mh->query = (char*)query;
	mh->msg = monetdbe_query(mh->mid->mdbe, mh->query, &mh->result, &mh->affected_rows);
	mh->current_row = 0;
	mh->mapi_row = NULL;
	if (mh->result && mh->result->ncols) {
		mh->mapi_row = (char**)MAPIalloc(sizeof(char*)*mh->result->ncols);
		if (!mh->mapi_row) {
			mh->msg = "malloc failure";
			return mh;
		}
		memset(mh->mapi_row, 0, sizeof(char*)*mh->result->ncols);
	}
	return mh;
}

MapiMsg
mapi_close_handle(MapiHdl hdl)
{
	if (hdl) {
		char *msg = NULL;
		if (hdl->result) {
			if (hdl->mapi_row) {
				for (size_t i=0; i<hdl->result->ncols; i++) {
					if (hdl->mapi_row[i])
						MAPIfree(hdl->mapi_row[i]);
				}
				MAPIfree(hdl->mapi_row);
			}
			msg = monetdbe_cleanup_result(hdl->mid->mdbe, hdl->result);
			if (msg)
				hdl->mid->msg = msg;
		}
		MAPIfree(hdl);
	}
	return MOK;
}

int
mapi_fetch_row(MapiHdl hdl)
{
	int n = 0;

	if (hdl->result && hdl->current_row < hdl->result->nrows) {
		n = (int) ++hdl->current_row;
	}
	return n;
}

#define SIMPLE_TYPE_SIZE 128

char *
mapi_fetch_field(MapiHdl hdl, int fnr)
{
	if (fnr < (int)hdl->result->ncols && hdl->current_row > 0 && hdl->current_row <= hdl->result->nrows) {
		monetdbe_column *rcol = NULL;
		if (monetdbe_result_fetch(hdl->result,  &rcol, fnr) == NULL) {
			size_t r = (size_t) hdl->current_row - 1;
			if (rcol->type != monetdbe_str && !hdl->mapi_row[fnr]) {
				hdl->mapi_row[fnr] = MAPIalloc(SIMPLE_TYPE_SIZE);
				if (!hdl->mapi_row[fnr]) {
					hdl->msg = "malloc failure";
					return NULL;
				}
			}
			switch(rcol->type) {
			case monetdbe_bool: {
				monetdbe_column_bool *icol = (monetdbe_column_bool*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%s", icol->data[r]==1?"true":"false");
				return hdl->mapi_row[fnr];
			}
			case monetdbe_int8_t: {
				monetdbe_column_int8_t *icol = (monetdbe_column_int8_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%d", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdbe_int16_t: {
				monetdbe_column_int16_t *icol = (monetdbe_column_int16_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%d", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdbe_int32_t: {
				monetdbe_column_int32_t *icol = (monetdbe_column_int32_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%d", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdbe_int64_t: {
				monetdbe_column_int64_t *icol = (monetdbe_column_int64_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%" PRId64, icol->data[r]);
				return hdl->mapi_row[fnr];
			}
#ifdef HAVE_HGE
			case monetdbe_int128_t: {
				monetdbe_column_int128_t *icol = (monetdbe_column_int128_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%" PRId64, (int64_t)icol->data[r]);
				return hdl->mapi_row[fnr];
			}
#endif
			case monetdbe_float: {
				monetdbe_column_float *icol = (monetdbe_column_float*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%f", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdbe_double: {
				monetdbe_column_double *icol = (monetdbe_column_double*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%f", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdbe_str: {
				monetdbe_column_str *icol = (monetdbe_column_str*)rcol;
				return icol->data[r];
			}
			default:
				return NULL;
			}
		}
	}
	return NULL;
}

char *
mapi_get_type(MapiHdl hdl, int fnr)
{
	if (fnr < (int)hdl->result->ncols) {
		monetdbe_column *rcol = NULL;
		if (monetdbe_result_fetch(hdl->result,  &rcol, fnr) == NULL) {
			switch(rcol->type) {
			case monetdbe_bool:
				return "boolean";
			case monetdbe_int8_t:
				return "tinyint";
			case monetdbe_int16_t:
				return "smallint";
			case monetdbe_int32_t:
				return "integer";
			case monetdbe_int64_t:
				return "bigint";
#ifdef HAVE_HGE
			case monetdbe_int128_t:
				return "hugeint";
#endif
			case monetdbe_float:
				return "float";
			case monetdbe_double:
				return "real";
			case monetdbe_str:
				return "varchar";
			default:
				return "unknown";
			}
		}
	}
	return NULL;
}

MapiMsg
mapi_seek_row(MapiHdl hdl, int64_t rowne, int whence)
{
	if (rowne == 0 && whence == MAPI_SEEK_SET) {
		hdl->current_row = 0;
	}
	return MOK;
}

int64_t
mapi_get_row_count(MapiHdl hdl)
{
	return hdl->result->nrows;
}

int64_t
mapi_rows_affected(MapiHdl hdl)
{
	if (hdl->result)
		return hdl->result->nrows;
	return hdl->affected_rows;
}

int
mapi_get_field_count(MapiHdl hdl)
{
	return (int) hdl->result->ncols;
}

const char *
mapi_result_error(MapiHdl hdl)
{
	if (hdl) {
		return hdl->msg;
	}
	return NULL;
}

int mapi_get_len(MapiHdl hdl, int fnr)
{
	(void)hdl;
	(void)fnr;
	return 0;
}

/* implement these to make dump.c error's more informative */
void mapi_explain(Mapi mid, FILE *fd)
{
	(void)mid;
	(void)fd;
}

void mapi_explain_query(MapiHdl hdl, FILE *fd)
{
	(void)hdl;
	(void)fd;
}

void mapi_explain_result(MapiHdl hdl, FILE *fd)
{
	(void)hdl;
	(void)fd;
}
