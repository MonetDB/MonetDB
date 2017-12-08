/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _MAL_SABAOTH_DEF
#define _MAL_SABAOTH_DEF

#include "mal.h"
#include "mal_exception.h"
#include "msabaoth.h"
mal_export str SABAOTHmarchScenario(str *lang);
mal_export str SABAOTHretreatScenario(str *lang);
mal_export str SABAOTHmarchConnection(str *host, int *port);
mal_export str SABAOTHgetLocalConnection(str *ret);
mal_export str SABAOTHgetMyStatus(sabdb** ret);
mal_export str SABAOTHfreeStatus(sabdb** ret);
#endif
