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
#ifndef _EMBEDDED_R_LIB_
#define _EMBEDDED_R_LIB_

#include <Rdefines.h>
#include "embedded.h"

SEXP monetdb_query_R(SEXP connsexp, SEXP querysexp, SEXP notreally);
SEXP monetdb_startup_R(SEXP dbdirsexp, SEXP silentsexp);
SEXP monetdb_append_R(SEXP connsexp, SEXP schemaname, SEXP tablename, SEXP tabledata);
SEXP monetdb_connect_R(void);
SEXP monetdb_disconnect_R(SEXP connsexp);
SEXP monetdb_shutdown_R(void);

#endif
