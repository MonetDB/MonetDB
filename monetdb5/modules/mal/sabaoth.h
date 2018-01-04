/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SABAOTH_DEF
#define _SABAOTH_DEF

mal_export str SABprelude(void *ret);
mal_export str SABepilogue(void *ret);
mal_export str SABmarchScenario(void *ret, str *lang);
mal_export str SABretreatScenario(void *ret, str *lang);
mal_export str SABmarchConnection(void *ret, str *host, int *port) ;
mal_export str SABgetLocalConnectionURI(str *ret);
mal_export str SABgetLocalConnectionHost(str *ret);
mal_export str SABgetLocalConnectionPort(int *ret);

#endif
