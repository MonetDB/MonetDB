#ifndef MONETDBE_MAPI_H_
#define MONETDBE_MAPI_H_

#include "monetdb_config.h"
#include "stream.h"
#include "mstring.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <monetdbe.h>

typedef struct MapiStruct {
	monetdbe_database mdbe;
	char *msg;
} *Mapi;

typedef struct MapiStatement {
	Mapi mid;
	char *query;
	monetdbe_result *result;
	char **mapi_row;	/* keep buffers for string return values */
	monetdbe_cnt current_row;
	monetdbe_cnt affected_rows;
	char *msg;
} *MapiHdl;

#endif // MONETDBE_MAPI_H_
