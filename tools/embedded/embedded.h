/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * H. Muehleisen, M. Raasveldt
 * Inverse RAPI
 */
#ifndef _INVERSE_RAPI_LIB_
#define _INVERSE_RAPI_LIB_

#include <Rdefines.h>

typedef struct append_data {
	char* colname;
	ssize_t batid;
} append_data;

char* monetdb_startup(char* installdir, char* dbdir, char silent);
char* monetdb_query(char* query, void** result);
char* monetdb_append(const char* schema, const char* table, append_data *ad, int ncols);
void monetdb_cleanup_result(void* output);
SEXP monetdb_query_R(SEXP querysexp, SEXP notreally);
SEXP monetdb_startup_R(SEXP installdirsexp, SEXP dbdirsexp, SEXP silentsexp);
SEXP monetdb_append_R(SEXP schemaname, SEXP tablename, SEXP tabledata);

#endif
