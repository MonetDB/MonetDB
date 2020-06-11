
#include "monetdb_config.h"
#include "stream.h"
#include "mapi.h"

#define MAPIalloc(sz) malloc(sz)
#define MAPIfree(p)   free(p)

char *
mapi_error(Mapi mid)
{
	return mid->msg;
}

MapiHdl 
mapi_query(Mapi mid, const char *query)
{
	MapiHdl mh = (MapiHdl)MAPIalloc(sizeof(struct MapiStatement));

	mh->mid = mid;
	mh->query = (char*)query;
	mh->msg = monetdb_query(mh->mid->conn, mh->query, &mh->result, &mh->affected_rows, &mh->prepare_id);
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
		if (hdl->mapi_row) {
			for (size_t i=0; i<hdl->result->ncols; i++) {
				if (hdl->mapi_row[i])
					MAPIfree(hdl->mapi_row[i]);
			}
			MAPIfree(hdl->mapi_row);
		}

		char *msg = monetdb_cleanup_result(hdl->mid->conn, hdl->result);
		MAPIfree(hdl);
		if (msg)
			hdl->mid->msg = msg;
	}
	return MOK;
}

int 
mapi_fetch_row(MapiHdl hdl)
{
	int n = 0;
	if (hdl && hdl->current_row < hdl->result->nrows) {
		n = ++hdl->current_row;
	}
	return n;
}

#define SIMPLE_TYPE_SIZE 128

char *
mapi_fetch_field(MapiHdl hdl, int fnr)
{
	if (hdl && fnr < (int)hdl->result->ncols && hdl->current_row > 0 && hdl->current_row <= hdl->result->nrows) {
		monetdb_column *rcol = NULL;
		if (monetdb_result_fetch(hdl->mid->conn, hdl->result,  &rcol, fnr) == NULL) {
			size_t r = hdl->current_row - 1;
			if (rcol->type != monetdb_str && !hdl->mapi_row[fnr]) {
				hdl->mapi_row[fnr] = MAPIalloc(SIMPLE_TYPE_SIZE);
				if (!hdl->mapi_row[fnr]) {
					hdl->msg = "malloc failure";
					return NULL;
				}
			}
			switch(rcol->type) {
			case monetdb_bool: {
				monetdb_column_bool *icol = (monetdb_column_bool*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%s", icol->data[r]==1?"true":"false");
				return hdl->mapi_row[fnr];
			}
			case monetdb_int8_t: {
				monetdb_column_int8_t *icol = (monetdb_column_int8_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%d", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdb_int16_t: {
				monetdb_column_int16_t *icol = (monetdb_column_int16_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%d", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdb_int32_t: {
				monetdb_column_int32_t *icol = (monetdb_column_int32_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%d", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdb_int64_t: {
				monetdb_column_int64_t *icol = (monetdb_column_int64_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%" PRId64, icol->data[r]);
				return hdl->mapi_row[fnr];
			}
#ifdef HAVE_HGE
			case monetdb_int128_t: {
				monetdb_column_int128_t *icol = (monetdb_column_int128_t*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%" PRId64, (int64_t)icol->data[r]);
				return hdl->mapi_row[fnr];
			}
#endif
			case monetdb_float: {
				monetdb_column_float *icol = (monetdb_column_float*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%f", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdb_double: {
				monetdb_column_double *icol = (monetdb_column_double*)rcol;
				if (icol->data[r] == icol->null_value)
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "NULL");
				else
					snprintf(hdl->mapi_row[fnr], SIMPLE_TYPE_SIZE, "%f", icol->data[r]);
				return hdl->mapi_row[fnr];
			}
			case monetdb_str: {
				monetdb_column_str *icol = (monetdb_column_str*)rcol;
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
	if (hdl && fnr < (int)hdl->result->ncols) {
		monetdb_column *rcol = NULL;
		if (monetdb_result_fetch(hdl->mid->conn, hdl->result,  &rcol, fnr) == NULL) {
			switch(rcol->type) {
			case monetdb_bool:
				return "boolean";
			case monetdb_int8_t:
				return "tinyint";
			case monetdb_int16_t:
				return "smallint";
			case monetdb_int32_t:
				return "integer";
			case monetdb_int64_t:
				return "bigint";
#ifdef HAVE_HGE
			case monetdb_int128_t:
				return "hugeint";
#endif
			case monetdb_float:
				return "float";
			case monetdb_double:
				return "real";
			case monetdb_str:
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
	if (hdl && rowne == 0 && whence == MAPI_SEEK_SET) {
		hdl->current_row = 0;
	}
	return MOK;
}

int64_t 
mapi_get_row_count(MapiHdl hdl)
{
	if (hdl) {
		return hdl->result->nrows;
	}
	return 0;
}

int64_t 
mapi_rows_affected(MapiHdl hdl) 
{
	if (hdl) {
		if (hdl->result)
			return hdl->result->nrows;
		return hdl->affected_rows;
	}
	return 0;
}

int 
mapi_get_field_count(MapiHdl hdl)
{
	if (hdl) {
		return hdl->result->ncols;
	}
	return 0;
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

