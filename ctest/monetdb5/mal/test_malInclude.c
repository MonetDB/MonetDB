/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_import.h"
#include "mal_parser.h"

int
main(void)
{
    gdk_return gdk_res;

	if (BBPaddfarm(NULL, (1U << PERSISTENT) | (1U << TRANSIENT), false) != GDK_SUCCEED) {
		/* set in memory dbfarm */
		createException(MAL, "embedded.monetdb_startup", "BBPaddfram() failed");
		return 1;
	}

    gdk_res = GDKinit(NULL, 0, true, NULL);
    if (gdk_res != GDK_SUCCEED) {
		createException(MAL, "embedded.monetdb_startup", "GDKinit() failed");
		return 1;
    }
    char *modules[2];
    modules[0] = "sql";
    modules[1] = 0;
    mal_init(modules, true, NULL, NULL);
    return 0;
}
