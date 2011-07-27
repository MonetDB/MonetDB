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

#define bat_set_access(b,access) b->P->restricted = access
#define bat_clear(b) bat_set_access(b,BAT_WRITE);BATclear(b);bat_set_access(b,BAT_READ)

/* initialize bat storage call back functions interface */
extern int su_storage_init( store_functions *sf );
extern int ro_storage_init( store_functions *sf );
extern int suro_storage_init( store_functions *sf );

#endif /*RESTRICT_STORAGE_H */

