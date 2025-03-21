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
 * This file contains a number of functions for formatting the
 */

#ifndef _PY_FORMAT_INPUT_LIB_
#define _PY_FORMAT_INPUT_LIB_

#include <stddef.h>

#include "pyheader.h"

extern PyObject *marshal_loads;

char *FormatCode(char *code, char **args, size_t argcount, size_t tabwidth,
				 PyObject **code_object, char **msg, char **additional_args,
				 size_t additional_argcount);

void _formatinput_init(void);

#endif /* _PY_FORMAT_INPUT_LIB_ */
