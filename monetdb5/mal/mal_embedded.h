/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAL_EMBEDDED_H
#define _MAL_EMBEDDED_H

#include "monetdb_config.h"

#include "mal_client.h"
#include "mal_import.h"

#define MAXMODULES  128

mal_export str malEmbeddedBoot(int workerlimit, int memorylimit,
							   int querytimeout, int sessionlimit,
							   bool with_mapi_server);
mal_export str malExtraModulesBoot(Client c, str extraMalModules[],
								   char *mal_scripts);
mal_export void malEmbeddedReset(void);
mal_export _Noreturn void malEmbeddedStop(int status);

#endif /*  _MAL_EMBEDDED_H */
