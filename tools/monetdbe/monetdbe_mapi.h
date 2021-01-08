/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef MONETDBE_MAPI_H_
#define MONETDBE_MAPI_H_

#include "monetdb_config.h"
#include "stream.h"
#include "mstring.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <monetdbe.h>

char* monetdbe_mapi_dump_database(monetdbe_database dbhdl, const char *filename);
char* monetdbe_mapi_dump_table(monetdbe_database dbhdl, const char *sname, const char *tname, const char *filename);

/* TODO: everything below is just one big hack to get dump.c useful in monetdbe and fool it to use the mapi wrapper defined monetdbe_mapi.c for its internals.
 * Basically the dump logic should use a abstract base context class mimicked with void pointers and virtual methods.
 */

struct monetdbe_MapiStruct {
	monetdbe_database mdbe;
	char *msg;
};

typedef struct monetdbe_MapiStruct *monetdbe_Mapi;

struct monetdbe_MapiStatement {
	monetdbe_Mapi mid;
	char *query;
	monetdbe_result *result;
	char **mapi_row;	/* keep buffers for string return values */
	monetdbe_cnt current_row;
	monetdbe_cnt affected_rows;
	char *msg;
};

typedef struct monetdbe_MapiStatement *monetdbe_MapiHdl;

typedef int monetdbe_MapiMsg;

monetdbe_MapiMsg monetdbe_mapi_error(monetdbe_Mapi mid);
monetdbe_MapiHdl monetdbe_mapi_query(monetdbe_Mapi mid, const char *query);
monetdbe_MapiMsg monetdbe_mapi_close_handle(monetdbe_MapiHdl hdl);
int monetdbe_mapi_fetch_row(monetdbe_MapiHdl hdl);
char * monetdbe_mapi_fetch_field(monetdbe_MapiHdl hdl, int fnr);
char * monetdbe_mapi_get_type(monetdbe_MapiHdl hdl, int fnr);
monetdbe_MapiMsg monetdbe_mapi_seek_row(monetdbe_MapiHdl hdl, int64_t rowne, int whence);
int64_t monetdbe_mapi_get_row_count(monetdbe_MapiHdl hdl);
int64_t monetdbe_mapi_rows_affected(monetdbe_MapiHdl hdl);
int monetdbe_mapi_get_field_count(monetdbe_MapiHdl hdl);
const char * monetdbe_mapi_result_error(monetdbe_MapiHdl hdl);
int monetdbe_mapi_get_len(monetdbe_MapiHdl hdl, int fnr);
void monetdbe_mapi_explain(monetdbe_Mapi mid, FILE *fd);
void monetdbe_mapi_explain_query(monetdbe_MapiHdl hdl, FILE *fd);
void monetdbe_mapi_explain_result(monetdbe_MapiHdl hdl, FILE *fd);

#endif // MONETDBE_MAPI_H_
