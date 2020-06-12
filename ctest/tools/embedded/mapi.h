
#ifndef MAPI_H_
#define MAPI_H_

#include "monetdb_config.h"
#include "stream.h"
#include "mstring.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <monetdb_embedded.h>

typedef struct MapiStruct {
	monetdb_connection conn;
	char *msg;
} *Mapi;

typedef struct MapiStatement {
	Mapi mid;
	char *query;
	monetdb_result *result;
	char **mapi_row;	/* keep buffers for string return values */
	monetdb_cnt current_row;
	monetdb_cnt affected_rows;
	char *msg;
} *MapiHdl;

typedef int MapiMsg;

#define MAPI_SEEK_SET	0
#define MAPI_SEEK_CUR	1
#define MAPI_SEEK_END	2

#define MOK		0

extern char *mapi_error(Mapi mid);
extern MapiHdl mapi_query(Mapi mid, const char *query);
extern MapiMsg mapi_close_handle(MapiHdl hdl);
extern int mapi_fetch_row(MapiHdl hdl);
extern char *mapi_fetch_field(MapiHdl hdl, int fnr);
extern char *mapi_get_type(MapiHdl hdl, int fnr);
extern MapiMsg mapi_seek_row(MapiHdl hdl, int64_t rowne, int whence);
extern int64_t mapi_get_row_count(MapiHdl hdl);
extern int64_t mapi_rows_affected(MapiHdl hdl);
extern int mapi_get_field_count(MapiHdl hdl);
extern const char *mapi_result_error(MapiHdl hdl);
extern int mapi_get_len(MapiHdl hdl, int fnr);

extern void mapi_explain(Mapi mid, FILE *fd);
extern void mapi_explain_query(MapiHdl hdl, FILE *fd);
extern void mapi_explain_result(MapiHdl hdl, FILE *fd);

#endif // MAPI_H_
