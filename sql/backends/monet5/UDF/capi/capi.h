/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * M.Raasveldt
 * JIT UDF Interface
 */

#ifndef _CUDF_LIB_
#define _CUDF_LIB_

#include "monetdb_config.h"
#include "mal.h"
#include "mal_stack.h"
#include "mal_linker.h"
#include "gdk_atoms.h"
#include "gdk_utils.h"
#include "gdk_posix.h"
#include "gdk.h"
#include "sql_catalog.h"
#include "sql_scenario.h"
#include "sql_cast.h"
#include "sql_execute.h"
#include "sql_storage.h"

// DLL Export Flags
#ifdef WIN32
#ifndef LIBCUDF
#define cudf_export extern __declspec(dllimport)
#else
#define cudf_export extern __declspec(dllexport)
#endif
#else
#define cudf_export extern
#endif

cudf_export str CUDFevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							InstrPtr pci);
cudf_export str CUDFevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							 InstrPtr pci);
cudf_export str CUDFprelude(void *ret);

#endif /* _CUDF_LIB_ */
