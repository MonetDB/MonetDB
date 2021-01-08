/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_embedded.h"

int
main(void)
{
    char* msg = MAL_SUCCEED;
    gdk_return gdk_res;

    gdk_res = GDKinit(NULL, 0, true);
    if (gdk_res == GDK_FAIL) {
	msg = createException(MAL, "embedded.monetdb_startup", "GDKinit() failed");
        return 1;
    }
    if ((msg = malEmbeddedBoot(0, 0, 0, 0, 0)) != MAL_SUCCEED)
        return 1;
    return 0;
}
