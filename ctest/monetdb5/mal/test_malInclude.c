/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_import.h"
#include "mal_parser.h"
#include "mal_profiler.h"

int
main(void)
{
    gdk_return gdk_res;

    gdk_res = GDKinit(NULL, 0, true);
    if (gdk_res == GDK_FAIL) {
	createException(MAL, "embedded.monetdb_startup", "GDKinit() failed");
    	return 1;
    }
    char *modules[2];
    modules[0] = "sql";
    modules[1] = 0;
    mal_init(modules, 1);
    return 0;
}
