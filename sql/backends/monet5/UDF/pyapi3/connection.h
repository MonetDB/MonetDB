/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * M. Raasveldt
 * The connection object is a Python object that can be used
 * to query the database from within UDFs (i.e. loopback queries)
 */

#ifndef _LOOPBACK_QUERY_
#define _LOOPBACK_QUERY_

#include "pytypes.h"
#include "emit.h"

// The QueryStruct is used to send queries between a forked process and the main
// server
typedef struct {
	bool pending_query;
	char query[8192];
	int nr_cols;
	int mmapid;
	size_t memsize;
} QueryStruct;

typedef struct {
	PyObject_HEAD Client cntxt;
	QueryStruct *query_ptr;
	int query_sem;
} Py_ConnectionObject;

extern PyTypeObject Py_ConnectionType;

#define Py_Connection_Check(op) (Py_TYPE(op) == &Py_ConnectionType)
#define Py_Connection_CheckExact(op) (Py_TYPE(op) == &Py_ConnectionType)

PyObject *Py_Connection_Create(Client cntxt, QueryStruct *query_ptr, int query_sem);

str _connection_init(void);
str _connection_query(Client cntxt, const char *query, res_table **result);
str _connection_create_table(Client cntxt, char *sname, char *tname,
							 sql_emit_col *columns, size_t ncols);
str _connection_append_to_table(Client cntxt, char *sname, char *tname,
							 sql_emit_col *columns, size_t ncols);
void _connection_cleanup_result(void *output);

#endif /* _LOOPBACK_QUERY_ */
