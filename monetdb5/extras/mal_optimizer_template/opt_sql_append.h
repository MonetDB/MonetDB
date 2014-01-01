/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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

#define DEBUG_OPT_SQL_APPEND 61
#define OPTDEBUGsql_append if (optDebug & ((lng)1 << DEBUG_OPT_SQL_APPEND))

#endif /* _OPT_SQL_APPEND_ */
