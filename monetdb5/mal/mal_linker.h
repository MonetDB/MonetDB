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

#ifndef _MAL_LINKER_H
#include "mal_module.h"

#define MAL_EXT ".mal"
#define SQL_EXT ".sql"

#ifdef HAVE_DLFCN_H
#include <dlfcn.h>
#else
#define RTLD_LAZY   1
#define RTLD_NOW    2
#define RTLD_GLOBAL 4
#define RTLD_NOW_REPORT_ERROR   8
#endif

/* #define DEBUG_MAL_LINKER */
#define MONET64 1
mal_export MALfcn getAddress(str filename, str modnme, str fcnname,int silent);
mal_export char *MSP_locate_script(const char *mod_name);
mal_export char *MSP_locate_sqlscript(const char *mod_name, bit recurse);
mal_export str loadLibrary(str modulename, int flag);
mal_export void unloadLibraries(void);
mal_export void initLibraries(void);
mal_export int isPreloaded(str nme);
mal_export int isLoaded(str modulename);
#endif
