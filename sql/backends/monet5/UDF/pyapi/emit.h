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

#ifndef _LOADER_EMIT_
#define _LOADER_EMIT_

#include "pytypes.h"

typedef struct {
    PyObject_HEAD
	sql_emit_col *cols;
    size_t ncols;
    BUN nvals;
	size_t maxcols;
	bool create_table;
} PyEmitObject;

extern PyTypeObject PyEmitType;

PyObject *PyEmit_Create(sql_emit_col *cols, size_t ncols);
PyObject *PyEmit_Emit(PyEmitObject *self, PyObject *args);

str _emit_init(void);

#endif /* _LOADER_EMIT_ */
