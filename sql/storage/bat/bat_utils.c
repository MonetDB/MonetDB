/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_utils.h"

void
bat_destroy(BAT *b)
{
	if (b)
		BBPunfix(b->batCacheid);
}

BAT *
bat_new(int tt, BUN size, role_t role)
{
	return COLnew(0, tt, size, role);
}

void
bat_clear(BAT *b)
{
	bat_set_access(b,BAT_WRITE);
	BATclear(b,true);
	bat_set_access(b,BAT_READ);
}

BAT *
temp_descriptor(log_bid b)
{
	return BATdescriptor((bat) b);
}

BAT *
quick_descriptor(log_bid b)
{
	return BBPquickdesc((bat) b, false);
}

void
temp_destroy(log_bid b)
{
	if (b)
		BBPrelease(b);
}

log_bid
temp_dup(log_bid b)
{
	if (b)
		BBPretain(b);
	return b;
}

log_bid
temp_create(BAT *b)
{
	temp_dup(b->batCacheid);
	return b->batCacheid;
}

log_bid
temp_copy(log_bid b, int temp)
{
	/* make a copy of b, if temp is set only create a empty bat */
	BAT *o = temp_descriptor(b);
	BAT *c;
	log_bid r;

	if (!o)
		return BID_NIL;
	if (!temp) {
		c = COLcopy(o, o->ttype, true, PERSISTENT);
		if (!c)
			return BID_NIL;
		bat_set_access(c, BAT_READ);
		BATcommit(c, BUN_NONE);
	} else {
		c = bat_new(o->ttype, COLSIZE, PERSISTENT);
		if (!c)
			return BID_NIL;
	}
	r = temp_create(c);
	bat_destroy(c);
	bat_destroy(o);
	return r;
}

BUN
append_inserted(BAT *b, BAT *i )
{
	BUN nr = 0, r;
	BATiter ii = bat_iterator(i);

	for (r = i->batInserted; r < BUNlast(i); r++) {
		if (BUNappend(b, BUNtail(ii,r), true) != GDK_SUCCEED)
			return BUN_NONE;
		nr++;
	}
	return nr;
}

BAT *ebats[MAXATOMS] = { NULL };

log_bid
ebat2real(log_bid b, oid ibase)
{
	/* make a copy of b */
	log_bid r = BID_NIL;
	BAT *o = temp_descriptor(b);
	if(o) {
		BAT *c = COLcopy(o, ATOMtype(o->ttype), true, PERSISTENT);
		if(c) {
			BAThseqbase(c, ibase );
			r = temp_create(c);
			bat_destroy(c);
		}
		bat_destroy(o);
	}
	return r;
}

log_bid
e_bat(int type)
{
	if (ebats[type] == NULL &&
	    (ebats[type] = bat_new(type, 0, TRANSIENT)) == NULL)
		return BID_NIL;
	return temp_create(ebats[type]);
}

BAT *
e_BAT(int type)
{
	if (ebats[type] == NULL &&
	    (ebats[type] = bat_new(type, 0, TRANSIENT)) == NULL)
		return NULL;
	return temp_descriptor(ebats[type]->batCacheid);
}

log_bid
ebat_copy(log_bid b)
{
	/* make a copy of b */
	BAT *o = temp_descriptor(b);
	BAT *c;
	log_bid r;

	if (!o)
		return BID_NIL;
	if (!ebats[o->ttype]) {
		ebats[o->ttype] = bat_new(o->ttype, 0, TRANSIENT);
		if (!ebats[o->ttype]) {
			bat_destroy(o);
			return BID_NIL;
		}
	}

	if (BATcount(o)) {
		c = COLcopy(o, o->ttype, true, PERSISTENT);
		if (!c)
			return BID_NIL;
		BAThseqbase(c, 0);
		BATcommit(c, BUN_NONE);
		bat_set_access(c, BAT_READ);
		r = temp_create(c);
		bat_destroy(c);
	} else {
		c = ebats[o->ttype];
		if (!c)
			return BID_NIL;
		r = temp_create(c);
	}
	bat_destroy(o);
	return r;
}

int
bat_utils_init(void)
{
	int t;
	char name[32];

	for (t=1; t<GDKatomcnt; t++) {
		if (t != TYPE_bat && BATatoms[t].name[0]) {
			ebats[t] = bat_new(t, 0, TRANSIENT);
			if(ebats[t] == NULL) {
				for (t = t - 1; t >= 1; t--)
					bat_destroy(ebats[t]);
				return -1;
			}
			bat_set_access(ebats[t], BAT_READ);
			/* give it a name for debugging purposes */
			snprintf(name, sizeof(name), "sql_empty_%s_bat",
				 ATOMname(t));
			BBPrename(ebats[t]->batCacheid, name);
		}
	}
	return 0;
}
