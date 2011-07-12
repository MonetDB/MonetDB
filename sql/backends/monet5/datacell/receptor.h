/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * The implementation is inspired by the tablet module.
 */
#ifndef _RECEPTOR_
#define _RECEPTOR_
#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "tablet.h"
#include "mtime.h"
#include "basket.h"

#ifdef WIN32
#ifndef LIBCONTAINERS
#define datacell_export extern __declspec(dllimport)
#else
#define datacell_export extern __declspec(dllexport)
#endif
#else
#define datacell_export extern
#endif

/* #define _DEBUG_RECEPTOR_*/
#define RCout GDKout
datacell_export str RCdump();

/*
 * @-
 * Multiple protocols for event handling are foreseen.
 * We experiment with the two dominant versions.
 * TCP provides a reliable protocol for event exchange.
 * UDP is not-reliable and its behavior in the context
 * of the DataCell depends on the ability to handle the
 * event stream at the same speed as it arrives.
 */
#define PAUSEDEFAULT 1000

#ifdef WIN32
#ifndef LIBADAPTERS
#define adapters_export extern __declspec(dllimport)
#else
#define adapters_export extern __declspec(dllexport)
#endif
#else
#define adapters_export extern
#endif

adapters_export str DCreceptorNew(int *ret, str *tbl, str *host, int *port);
adapters_export str DCreceptorPause(int *ret, str *nme);
adapters_export str DCreceptorResume(int *ret, str *nme);
adapters_export str RCresume(int *ret);
adapters_export str RCdrop(int *ret, str *nme);
adapters_export str RCreset(int *ret);
adapters_export str RCmode(int *ret, str *nme, str *arg);
adapters_export str RCprotocol(int *ret, str *nme, str *arg);
adapters_export str DCscenario(int *ret, str *nme, str *fnme, int *seq);
adapters_export str DCgenerator(int *ret, str *nme, str *modnme, str *fcnnme);
adapters_export str RCdump() ;
#endif

