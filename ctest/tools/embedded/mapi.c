
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
	return mh;
}

MapiMsg
mapi_close_handle(MapiHdl hdl)
{
	if (hdl) {
		char *msg = monetdb_cleanup_result(hdl->mid->conn, hdl->result);
		Mapi mid = hdl->mid;

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

char *
mapi_fetch_field(MapiHdl hdl, int fnr)
{
	/* should only be used for string columns !!! */
	if (hdl && fnr < hdl->result->ncols && hdl->current_row > 0 && hdl->current_row <= hdl->result->nrows) {
		monetdb_column *rcol = NULL;
		if (monetdb_result_fetch(hdl->mid->conn, hdl->result,  &rcol, fnr) == NULL) {
			monetdb_column_str *icol = (monetdb_column_str*)rcol;
			return icol->data[hdl->current_row-1];
		}
	}
	return NULL;
}

char *
mapi_get_type(MapiHdl hdl, int fnr)
{
	if (hdl && fnr < hdl->result->ncols) {
		monetdb_column *rcol = NULL;
		if (monetdb_result_fetch(hdl->mid->conn, hdl->result,  &rcol, fnr) != NULL) {
			switch(rcol->type) {
			case monetdb_int8_t:
			case monetdb_int16_t:
			case monetdb_int32_t:
			case monetdb_int64_t:
				return "integer";
			case monetdb_float:
			case monetdb_double:
				return "float";
			case monetdb_str:
				return "string";
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

