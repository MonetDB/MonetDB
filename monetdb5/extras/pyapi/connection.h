/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * 
 */

#ifndef _LOOPBACK_QUERY_
#define _LOOPBACK_QUERY_

#include "pytypes.h"

typedef struct {
    PyObject_HEAD
    Client cntxt;
    bit mapped;
    QueryStruct *query_ptr;
    int query_sem;
} Py_ConnectionObject;

extern PyTypeObject Py_ConnectionType;

#define Py_Connection_Check(op) (Py_TYPE(op) == &Py_ConnectionType)
#define Py_Connection_CheckExact(op) (Py_TYPE(op) == &Py_ConnectionType)

PyObject *Py_Connection_Create(Client cntxt, bit mapped, QueryStruct *query_ptr, int query_sem);

void _connection_init(void);
char* _connection_query(Client cntxt, char* query, res_table** result);
void _connection_cleanup_result(void* output);

#endif /* _LOOPBACK_QUERY_ */
