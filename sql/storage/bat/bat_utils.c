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

#include "monetdb_config.h"
#include "bat_utils.h"

void
leaks(void)
{
	int pbat = 0;
	int pdisk = 0;
	int pheat = 0;
	bat i;
	int tmp = 0, per = 0;
	BAT *b;

	b = BATnew(TYPE_str, TYPE_int, 32);
	if (b == 0)
		return ;

	for (i = 1; i < BBPsize; i++) {
	        if (i != b->batCacheid && BBPvalid(i) && !(BBP_status(i) & BBPUNSTABLE)) {
			pbat++;
			if (BBP_cache(i)) {
				pheat += BBP_lastused(i);
				if (BBP_cache(i)->batPersistence == PERSISTENT)
					per++;
				else
					tmp++;
			} else {
				pdisk++;
			}
		}
	}
	b = BUNins(b, "bats", &pbat, FALSE);
	b = BUNins(b, "tmpbats", &tmp, FALSE);
	b = BUNins(b, "perbats", &per, FALSE);
	b = BUNins(b, "ondisk", &pdisk, FALSE);
	b = BUNins(b, "todisk", &BBPout, FALSE);
	b = BUNins(b, "fromdisk", &BBPin, FALSE);
	BATprint(b);
	bat_destroy(b);
}

void
bat_destroy(BAT *b)
{
	if (b)
		BBPunfix(b->batCacheid);
}

BAT *
bat_new(int ht, int tt, BUN size)
{
	BAT *nb = BATnew(ht, tt, size);

	if (ht == TYPE_void)
		BATseqbase(nb, 0);
	return nb;
}

BAT *
temp_descriptor(log_bid b)
{
	return BATdescriptor((bat) b);
}

BAT *
quick_descriptor(log_bid b)
{
	return BBPquickdesc((bat) b, FALSE);
}

void
temp_destroy(log_bid b)
{
	BBPdecref(b, TRUE);
}

void
temp_dup(log_bid b)
{
	BBPincref(b, TRUE);
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

	if (!temp) {
		c = BATcopy(o, o->htype, o->ttype, TRUE);
		bat_set_access(c, BAT_READ);
		BATcommit(c);
	} else {
		c = bat_new(o->htype, o->ttype, COLSIZE);
	}
	r = temp_create(c);
	bat_destroy(c);
	bat_destroy(o);
	return r;
}

void
update_table_bat(BAT *b, BAT *ub)
{
	if (ub && BATcount(ub)) {
		void_replace_bat(b, ub, TRUE);
		BATclear(ub);
		BATcommit(ub);
	}
}

BUN
append_inserted(BAT *b, BAT *i )
{
	BUN nr = 0;
	BUN r;
       	BATiter ii = bat_iterator(i);

       	for (r = i->batInserted; r < BUNlast(i); r++) {
		BUNappend(b, BUNtail(ii,r), TRUE);
		nr++;
	}
	return nr;
}

BUN
copy_inserted(BAT *b, BAT *i )
{
	BUN nr = 0;
	BUN r;
       	BATiter ii = bat_iterator(i);

	for (r = i->batInserted; r < BUNlast(i); r++) {
		BUNins(b, BUNhead(ii,r), BUNtail(ii,r), TRUE);
       		nr++;
	}
	return nr;
}

BAT *ebats[MAXATOMS] = { NULL };
BAT *eubats[MAXATOMS] = { NULL };

log_bid 
ebat2real(log_bid b, oid ibase)
{
	/* make a copy of b */
	BAT *o = temp_descriptor(b);
	BAT *c = BATcopy(o, TYPE_void, ATOMtype(o->ttype), TRUE);
	log_bid r;

	BATseqbase(c, ibase );
	r = temp_create(c);
	bat_destroy(c);
	bat_destroy(o);
	return r;
}

log_bid 
e_bat(int type)
{
	if (!ebats[type]) 
		ebats[type] = BATnew(TYPE_void, type, 0);
	return temp_create(ebats[type]);
}

BAT * 
e_BAT(int type)
{
	if (!ebats[type]) 
		ebats[type] = BATnew(TYPE_void, type, 0);
	return temp_descriptor(ebats[type]->batCacheid);
}

log_bid 
e_ubat(int type)
{
	if (!eubats[type]) 
		eubats[type] = BATnew(TYPE_oid, type, 0);
	return temp_create(eubats[type]);
}


log_bid 
ebat_copy(log_bid b, oid ibase, int temp)
{
	/* make a copy of b */
	BAT *o = temp_descriptor(b);
	BAT *c;
	log_bid r;

	if (!ebats[o->ttype]) 
		ebats[o->ttype] = BATnew(TYPE_void, o->ttype, 0);

	if (!temp && BATcount(o)) {
		c = BATcopy(o, TYPE_void, o->ttype, TRUE);
		BATseqbase(c, ibase );
		BATcommit(c);
		bat_set_access(c, BAT_READ);
		r = temp_create(c);
		bat_destroy(c);
	} else {
		c = ebats[o->ttype];
		r = temp_create(c);
	}
	bat_destroy(o);
	return r;
}

log_bid 
eubat_copy(log_bid b, int temp)
{
	/* make a copy of b */
	BAT *o = temp_descriptor(b);
	BAT *c;
	log_bid r;

	if (!eubats[o->ttype]) 
		eubats[o->ttype] = BATnew(TYPE_oid, o->ttype, 0);

	if (!temp && BATcount(o)) {
		c = BATcopy(o, TYPE_oid, o->ttype, TRUE);
		BATcommit(c);
		r = temp_create(c);
		bat_set_access(c, BAT_READ);
		bat_destroy(c);
	} else {
		c = eubats[o->ttype];
		r = temp_create(c);
	}
	bat_destroy(o);
	return r;
}

void
bat_utils_init(void)
{
	int t;

	for (t=1; t<GDKatomcnt; t++) {
		if (BATatoms[t].name[0]) {
			eubats[t] = BATnew(TYPE_oid, t, 0);
			ebats[t] = BATnew(TYPE_void, t, 0);
			BATseqbase(ebats[t],0);
			bat_set_access(eubats[t], BAT_READ);
			bat_set_access(ebats[t], BAT_READ);
		}
	}
}
