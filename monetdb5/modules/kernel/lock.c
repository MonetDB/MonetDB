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

/*
 * @f lock
 * @a Peter Boncz
 * @v 1.0
 * @+ Lightweight Lock Module
 * This module provides simple SMP lock and thread functionality
 * as already present in the MonetDB system.
 * @+ Locks
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "lock.h"

static int
create_lock(monet_lock *l)
{
	*l = (monet_lock) GDKmalloc(sizeof(MT_Lock));
	if (*l == NULL || *l == ptr_nil) return GDK_FAIL;
	MT_lock_init((MT_Lock*) *l, "M5_create_lock");
	return GDK_SUCCEED;
}

static int
set_lock(monet_lock *l)
{
	if (*l == NULL || *l == ptr_nil) return GDK_FAIL;
	MT_lock_set((MT_Lock*) *l, "set_lock");
	return GDK_SUCCEED;
}

static int
try_lock(int *res, monet_lock *l)
{
	if (*l == NULL || *l == ptr_nil) return GDK_FAIL;
	*res = MT_lock_try((MT_Lock*) *l) ? EBUSY : 0;
	return GDK_SUCCEED;
}

static int
unset_lock(monet_lock *l)
{
	if (*l == NULL || *l == ptr_nil) return GDK_FAIL;
	MT_lock_unset((MT_Lock*) *l, "unset_lock");
	return GDK_SUCCEED;
}

static int
destroy_lock(monet_lock *l)
{
	if (*l == NULL || *l == ptr_nil) return GDK_FAIL;
	MT_lock_destroy((MT_Lock*) *l);
	GDKfree(*l);
	return GDK_SUCCEED;
}


/*
 * @-
 * The old code base is wrapped to ease update propagation.
 */
int
lockToStr(char **dst, int *len, ptr *src)
{
	(void) len;		/* fool compiler */
	if (src == ptr_nil) {
		strcpy(*dst, "nil");
		return 3;
	}
	/* sprintf(*dst,"%o", (ptr)*src); */
	sprintf(*dst, "redo lockToStr");
	return (int) strlen(*dst);
}

str
LCKcreate(monet_lock *l)
{
	create_lock(l);
	return MAL_SUCCEED;
}

str
LCKset(int *res, monet_lock *l)
{
	set_lock(l);
	*res = 1;
	return MAL_SUCCEED;
}

str
LCKtry(int *res, monet_lock *l)
{
	try_lock(res, l);
	return MAL_SUCCEED;
}

str
LCKunset(int *res, monet_lock *l)
{
	unset_lock(l);
	*res = 1;
	return MAL_SUCCEED;
}

str
LCKdestroy(int *res, monet_lock *l)
{
	destroy_lock(l);
	*res = 1;
	return MAL_SUCCEED;
}

