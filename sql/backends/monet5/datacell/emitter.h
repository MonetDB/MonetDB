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

/*
 * @-
 * @+ Implementation
 * The implementation is derived from the emitter module.
 */
#ifndef _EMITTER_
#define _EMITTER_
#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "tablet.h"
#include "mtime.h"
#include "basket.h"

/* #define _DEBUG_EMITTER_ */
#define EMout GDKout

#ifdef WIN32
#ifndef LIBDATACELL
#define adapters_export extern __declspec(dllimport)
#else
#define adapters_export extern __declspec(dllexport)
#endif
#else
#define adapters_export extern
#endif

adapters_export str DCemitterNew(int *ret, str *tbl, str *host, int *port);
adapters_export str DCemitterPause(int *ret, str *nme);
adapters_export str DCemitterResume(int *ret, str *nme);
adapters_export str EMresume(int *ret);
adapters_export str EMstop(int *ret, str *nme);
adapters_export str EMreset(int *ret);
adapters_export str EMdump(void);
adapters_export str EMmode(int *ret, str *nme, str *arg);
adapters_export str EMprotocol(int *ret, str *nme, str *arg);

#endif

