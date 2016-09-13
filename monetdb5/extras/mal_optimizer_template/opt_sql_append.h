/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _OPT_SQL_APPEND_
#define _OPT_SQL_APPEND_

#ifdef WIN32
#ifndef LIBOPT_SQL_APPEND
#define opt_sql_append_export extern __declspec(dllimport)
#else
#define opt_sql_append_export extern __declspec(dllexport)
#endif
#else
#define opt_sql_append_export extern
#endif

#include "opt_prelude.h"

opt_sql_append_export str OPTsql_append(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#endif /* _OPT_SQL_APPEND_ */
