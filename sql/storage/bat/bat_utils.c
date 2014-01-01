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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
bat_new(int ht, int tt, BUN size)
{
	BAT *nb = BATnew(ht, tt, size);

	if (ht == TYPE_void) {
		BATseqbase(nb, 0);
		nb->H->dense = 1;
	}
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
	if (b)
		BBPdecref(b, TRUE);
}

void
temp_dup(log_bid b)
{
	if (b)
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

BUN
append_inserted(BAT *b, BAT *i )
{
	BUN nr = 0, r;
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
	c->H->dense = 1;
	r = temp_create(c);
	bat_destroy(c);
	bat_destroy(o);
	return r;
}

log_bid 
e_bat(int type)
{
	if (!ebats[type]) 
		ebats[type] = bat_new(TYPE_void, type, 0);
	return temp_create(ebats[type]);
}

BAT * 
e_BAT(int type)
{
	if (!ebats[type]) 
		ebats[type] = bat_new(TYPE_void, type, 0);
	return temp_descriptor(ebats[type]->batCacheid);
}

log_bid 
e_ubat(int type)
{
	if (!eubats[type]) 
		eubats[type] = bat_new(TYPE_oid, type, 0);
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
		ebats[o->ttype] = bat_new(TYPE_void, o->ttype, 0);

	if (!temp && BATcount(o)) {
		c = BATcopy(o, TYPE_void, o->ttype, TRUE);
		BATseqbase(c, ibase );
		c->H->dense = 1;
		BATcommit(o);
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
		eubats[o->ttype] = bat_new(TYPE_oid, o->ttype, 0);

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
		if (t != TYPE_bat && BATatoms[t].name[0]) {
			eubats[t] = bat_new(TYPE_oid, t, 0);
			ebats[t] = bat_new(TYPE_void, t, 0);
			bat_set_access(eubats[t], BAT_READ);
			bat_set_access(ebats[t], BAT_READ);
		}
	}
}

sql_schema *
tr_find_schema( sql_trans *tr, sql_schema *s)
{
	sql_schema *ns = NULL;

	while (!ns && tr) {
	 	ns = find_sql_schema_id(tr, s->base.id);
		tr = tr->parent;
	}
	return ns;
}

sql_table *
tr_find_table( sql_trans *tr, sql_table *t)
{
	sql_table *nt = NULL;

	while ((!nt || !nt->data) && tr) {
		sql_schema *s = tr_find_schema( tr, t->s);

		nt = find_sql_table_id(s, t->base.id);
		tr = tr->parent;
	}
	return nt;
}

sql_column *
tr_find_column( sql_trans *tr, sql_column *c)
{
	sql_column *nc = NULL;

	while ((!nc || !nc->data) && tr) {
		sql_schema *s = tr_find_schema( tr, c->t->s);
		sql_table *t =  find_sql_table_id(s, c->t->base.id);
		node *n = cs_find_id(&t->columns, c->base.id);
		if (n)
			nc = n->data;
		tr = tr->parent;
	}
	return nc;
}

sql_idx *
tr_find_idx( sql_trans *tr, sql_idx *i)
{
	sql_idx *ni = NULL;

	while ((!ni || !ni->data) && tr) {
		sql_schema *s = tr_find_schema( tr, i->t->s);
		sql_table *t =  find_sql_table_id(s, i->t->base.id);
		node *n = cs_find_id(&t->idxs, i->base.id);
		if (n)
			ni = n->data;
		tr = tr->parent;
	}
	return ni;
}

