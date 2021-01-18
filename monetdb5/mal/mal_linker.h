/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#endif

mal_export MALfcn getAddress(const char *modname, const char *fcnname);
mal_export char *MSP_locate_sqlscript(const char *mod_name, bit recurse);
mal_export str loadLibrary(const char *modulename, int flag);
mal_export char *locate_file(const char *basename, const char *ext, bit recurse);
mal_export int malLibraryEnabled(const char *name);
mal_export char* malLibraryHowToEnable(const char *name);
#endif /* _MAL_LINKER_H */
