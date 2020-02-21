/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include <stdio.h>

#include "monetdb_config.h"
#include "merovingian.h"
#include "snapshot.h"

err
snapshot_adhoc(char *dbname, char *dest)
{
	(void)dbname;
	(void)dest;
	sleep_ms(10 * 1000);
	return NO_ERR;
}