/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_embedded.h"

int
main(void)
{
    char* msg = MAL_SUCCEED;
    gdk_return gdk_res;

	if (BBPaddfarm(NULL, (1U << PERSISTENT) | (1U << TRANSIENT), false) != GDK_SUCCEED) {
		/* set in memory dbfarm */
		fprintf(stderr, "BBPaddfarm() failed\n");
		return 1;
	}

    gdk_res = GDKinit(NULL, 0, true, NULL);
    if (gdk_res != GDK_SUCCEED) {
		fprintf(stderr, "GDKinit() failed\n");
		return 1;
    }
    if ((msg = malEmbeddedBoot(0, 0, 0, 0, false)) != MAL_SUCCEED) {
		fprintf(stderr, "%s\n", msg);
		return 1;
	}
    return 0;
}
