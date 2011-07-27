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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"

#ifdef WIN32
#ifndef LIBSQL_CACHE
#define dummy_export extern __declspec(dllimport)
#else
#define dummy_export extern __declspec(dllexport)
#endif
#else
#define dummy_export extern
#endif

/* declare and export a dummy function so that on Windows the .lib
 * file is also produced */
dummy_export void dummy(void);
void
dummy(void)
{
}
