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
 * This file simply includes standard MonetDB and Python headers
 * Because this needs to be done in a specific order, and this needs to happen
 * in multiple places
 * We simplify our lives by only having to include this header.
 */

#ifndef _PYHEADER_H_
#define _PYHEADER_H_

#include "mal.h"
#include "mal_stack.h"
#include "mal_linker.h"
#include "gdk.h"
#include "sql_catalog.h"
#include "sql_scenario.h"
#include "sql_cast.h"
#include "sql_execute.h"
#include "sql_storage.h"

#include "undef.h"

// Python library
#undef _GNU_SOURCE
#undef _XOPEN_SOURCE
#undef _POSIX_C_SOURCE
#ifdef _DEBUG
#undef _DEBUG
#include <Python.h>
#define _DEBUG
#else
#include <Python.h>
#endif
#include <datetime.h>

// Numpy Library
#ifdef __COVERITY__
#define _NPY_NO_DEPRECATIONS
#endif
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define NPY_INTERNAL_BUILD 0
#ifdef __INTEL_COMPILER
// Intel compiler complains about trailing comma's in numpy source code,
#pragma warning(disable : 271)
#endif
#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>

// DLL Export Flags
#ifdef WIN32
#ifndef LIBPYAPI3
#define pyapi_export extern __declspec(dllimport)
#else
#define pyapi_export extern __declspec(dllexport)
#endif
#else
#define pyapi_export extern
#endif

PyDateTime_CAPI *get_DateTimeAPI(void);
void init_DateTimeAPI(void);

#define USE_DATETIME_API						\
	do {										\
		PyDateTimeAPI = get_DateTimeAPI();		\
	} while(0)



#define utf8string_minlength 256

#endif /* _PYHEADER_H_ */
