/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef RESTRICT_STORAGE_H
#define RESTRICT_STORAGE_H

#include "sql_storage.h"
#include "restrict_logger.h"

typedef struct sql_bat {
	char *name;	/* name of the main bat */
	int bid;
	int ubid;	/* bat with old updated values (su only) */
	size_t cnt;		/* number of tuples (excluding the deletes) */
	BAT *cached;		/* cached copy, used for schema bats only */
} sql_bat;

#define bat_set_access(b,access) b->batRestricted = access
#define bat_clear(b) bat_set_access(b,BAT_WRITE);BATclear(b,TRUE);bat_set_access(b,BAT_READ)

/* initialize bat storage call back functions interface */
extern int su_storage_init( store_functions *sf );
extern int ro_storage_init( store_functions *sf );
extern int suro_storage_init( store_functions *sf );

#endif /*RESTRICT_STORAGE_H */

