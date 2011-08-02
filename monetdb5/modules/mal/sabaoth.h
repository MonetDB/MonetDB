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
 * @- Implementation
 *
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

sabaoth_export str SABprelude(int *ret);
sabaoth_export str SABepilogue(int *ret);
sabaoth_export str SABmarchScenario(int *ret, str *lang);
sabaoth_export str SABretreatScenario(int *ret, str *lang);
sabaoth_export str SABmarchConnection(int *ret, str *host, int *port) ;
sabaoth_export str SABgetLocalConnectionURI(str *ret);
sabaoth_export str SABgetLocalConnectionHost(str *ret);
sabaoth_export str SABgetLocalConnectionPort(int *ret);
sabaoth_export str SABwildRetreat(int *ret);

#endif
