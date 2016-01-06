/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _SABAOTH_DEF
#define _SABAOTH_DEF

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define sabaoth_export extern __declspec(dllimport)
#else
#define sabaoth_export extern __declspec(dllexport)
#endif
#else
#define sabaoth_export extern
#endif

sabaoth_export str SABprelude(void *ret);
sabaoth_export str SABepilogue(void *ret);
sabaoth_export str SABmarchScenario(void *ret, str *lang);
sabaoth_export str SABretreatScenario(void *ret, str *lang);
sabaoth_export str SABmarchConnection(void *ret, str *host, int *port) ;
sabaoth_export str SABgetLocalConnectionURI(str *ret);
sabaoth_export str SABgetLocalConnectionHost(str *ret);
sabaoth_export str SABgetLocalConnectionPort(int *ret);

#endif
