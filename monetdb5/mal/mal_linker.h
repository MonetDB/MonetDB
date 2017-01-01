/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _MAL_LINKER_H
#define _MAL_LINKER_H

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
mal_export MALfcn getAddress(stream *out, str filename, str fcnname,int silent);
mal_export char *MSP_locate_sqlscript(const char *mod_name, bit recurse);
mal_export str loadLibrary(str modulename, int flag);
mal_export char *locate_file(const char *basename, const char *ext, bit recurse);
#endif /* _MAL_LINKER_H */
