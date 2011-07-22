/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
#include "bpm_storage.h"
#include <sql_string.h>

static MT_Lock part_lock;
static BAT *part_name = NULL;
static BAT *part_nr = NULL;

static BAT * bpm_part_name(void);

int part = 1; /* part id is saved by the logger */

static int 
PartID() 
{
	int p;

	/* needs locks */
	MT_lock_set(&part_lock, "PartID");
	p = part++;
	MT_lock_unset(&part_lock, "PartID");
	return p;
}

/* make sure there is space for at least one new part */
static void
use_part( sql_bpm *p )
{
	if (p->sz == p->nr) {
		p -> sz = (p->sz)?p->sz<<1:BPM_DEFAULT;
		p -> parts = RENEW_ARRAY(sql_delta, p->parts, p->sz);
	}
	memset(p->parts+p->nr, 0, sizeof(sql_delta));
}

static void
use_dpart( sql_dbpm *p )
{
	if (p->sz == p->nr) {
		p -> sz = (p->sz)?p->sz<<1:BPM_DEFAULT;
		p -> parts = RENEW_ARRAY(sql_dbat, p->parts, p->sz);
	}
	memset(p->parts+p->nr, 0, sizeof(sql_dbat));
}

static sht
extend_part( sql_bpm *p, BAT *b, BAT *i, bat u)
{
	sql_delta *bat;
	sht nr = p->nr;

	use_part(p); 
/*	if (!b) {
		 for now we have fixed block sizes 
		BATseqbase(i, nr * BPM_SPLIT);
	}*/
	bat = p->parts+nr;
	if (!bat->name) 
		bat->name = sql_message("%s_%d", p->name, nr);
	create_delta( bat, b, i, u);
	p -> nr++;
	return nr;
}

static sht
extend_dpart( sql_dbpm *p, BAT *i)
{
	sql_dbat *bat;
	sht nr = p->nr;

	use_dpart(p); 
/*	if (!i) {
		 for now we have fixed block sizes 
		BATseqbase(i, nr * BPM_SPLIT);
	}*/
	bat = p->parts+nr;
	if (!bat->dname) 
		bat->dname = sql_message("%s_%d", p->name, nr);
	bat->dbid = temp_create(i);
	p -> nr++;
	return nr;
}

static sql_bpm *
create_parts(sql_bpm *p, int type, char *nme)
{
	p -> type = type; 
	p -> nr = 0; 
	p -> created = 0; 
	p -> sz = BPM_DEFAULT;
	p -> parts = NEW_ARRAY(sql_delta, p->sz);
	p -> name = nme;
	return p;
}

static sql_dbpm *
create_dparts(sql_dbpm *p)
{
	p -> nr = 0; 
	p -> created = 0; 
	p -> sz = BPM_DEFAULT;
	p -> parts = NEW_ARRAY(sql_dbat, p->sz);
	return p;
}


static void 
bpm_init(void)
{
	int p;

	if (!part_name && (p = logger_find_bat(bat_logger, "part_name"))) {
		#define get_bat(nme) \
			temp_descriptor(logger_find_bat(bat_logger, nme))
		part_name = temp_descriptor(p);
		part_nr = get_bat("part_nr");
	}
	if (!part_name) {
		/* partition bats */
		part_name = bat_new(TYPE_oid, TYPE_str, 0);
		part_nr   = bat_new(TYPE_oid, TYPE_sht, 0);

#define log_P(l, b, n) logger_add_bat(l, b, n); log_bat_persists(l, b, n)
		log_P(bat_logger, part_name, "part_name");
		log_P(bat_logger, part_nr,   "part_nr");
	}
}

static BAT *
bpm_part_name(void) {
	if (!part_name)
		bpm_init();
	return part_name;
}

static void
bpm_new_part(oid id, char *name, sht nr )
{
	BAT *part_name = bpm_part_name();

	BUNins(part_name, (ptr)&id, (ptr)name, FALSE);
	BUNins(part_nr, (ptr)&id, (ptr)&nr, FALSE);

	log_bat(bat_logger, part_name, "part_name");
	log_bat(bat_logger, part_nr, "part_nr");
	BATcommit(part_name);
	BATcommit(part_nr);
}

static BAT *
bind_ucol(sql_trans *tr, sql_column *c, int access)
{
	sql_bpm *p = c->data;

	c->t->s->base.rtime = c->t->base.rtime = c->base.rtime = tr->stime;
	return delta_bind_ubat( &p->parts[0], access);
}

static BAT *
bind_uidx(sql_trans *tr, sql_idx * i, int access)
{
	sql_bpm *p = i->data;

	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_ubat( &p->parts[0], access);
}

static BAT *
bind_col(sql_trans *tr, sql_column *c, int access)
{
	sql_bpm *p = c->data;

	if (access == RD_UPD)
		return bind_ucol(tr, c, access);
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_bat( &p->parts[0], access, isTemp(c));
}

static BAT *
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	sql_bpm *p = i->data;

	if (access == RD_UPD)
		return bind_uidx(tr, i, access);
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_bat( &p->parts[0], access, isTemp(i));
}

static BAT *
bind_del(sql_trans *tr, sql_table *t, int access)
{
	sql_dbpm *p = t->data;

	t->s->base.rtime = t->base.rtime = tr->stime;
	return delta_bind_del( &p->parts[0], access);
}

static void
update_bat( sql_bpm *p, BAT *upd) 
{
	int i;
	oid lv = 0; 
	oid hv = lv + BPM_SPLIT; 

	for (i = 0; i<p->nr; i++) { 
		BAT *b = BATselect_( BATmirror(upd), (ptr)&lv, (ptr)&hv, TRUE, FALSE);

		delta_update_bat( &p->parts[i], BATmirror(b), 0);
		bat_destroy(b);

		lv = hv;
		hv += BPM_SPLIT;
	}
}

static void
update_val( sql_bpm *p, oid rid, void *upd) 
{
	sht nr = (sht) (rid?rid/BPM_SPLIT:0);

	assert(rid != oid_nil);

	if (nr >= p->nr)
		nr = p->nr-1;
	delta_update_val( &p->parts[nr], rid, upd);
}

/* TODO have a update_bpm */
static void
update_col(sql_trans *tr, sql_column *c, void *i, int tpe, oid rid)
{
	sql_bpm *p = c->data;

	c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->stime;
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		update_bat(p, i);
	else 
		update_val(p, rid, i);
}

static void 
update_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	sql_bpm *p = i->data;

	i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->stime;
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		update_bat(p, ib);
	else
		assert(0);
}

#if 0
static void
append_bat( sql_bpm *p, BAT *i ) 
{
	BAT *b = temp_descriptor(p->parts[p->nr-1].bid);

	/* all parts up to p->nr-1 are spread correctly */
	if (BATcount(i) + BATcount(b) > BPM_SPLIT) {

		if (BATcount(i) < BPM_SPLIT) {
			BAT *ni = i;
			if (isVIEW(i) || i->htype != TYPE_void || i->ttype == TYPE_void)
				ni = BATcopy(i, TYPE_void, i->ttype, TRUE);
			extend_part(p, ni, 0);
			if (ni != i)
				bat_destroy(ni);
		} else {
			ssize_t todo = BATcount(i);
			ssize_t l = 0, sz = BPM_SPLIT - BATcount(b);
			BAT *ni, *v = BATslice(i, l, l + sz);
			
			BATappend(b, v, TRUE);
			bat_destroy(v);
			todo -= sz;
			l += sz;
			sz = BPM_SPLIT;
			while(todo > 0) {
				v = BATslice(i, (BUN) l, (BUN) (l + sz));
				ni = BATcopy(v, TYPE_void, v->ttype, TRUE);
				bat_destroy(v);
				extend_part(p, ni, 0);
				todo -= sz;
				l += sz;
			}
		}
	} else if (BATcount(b) == 0 && !isVIEW(i) && i->htype == TYPE_void && i->ttype != TYPE_void){
		temp_destroy(p->parts[p->nr-1].bid);
		p->parts[p->nr-1].bid = temp_create(i);
	} else {
		BATappend(b, i, TRUE);
	}
	bat_destroy(b);
}
#endif

static void
append_bat( sql_bpm *p, BAT *i ) 
{
	/* should depends on the client to which part to add */
	delta_append_bat( &p->parts[p->nr-1], i);
}


static void
append_val( sql_bpm *p, void *i ) 
{
	/* should depends on the client to which part to add */
	delta_append_val( &p->parts[p->nr-1], i);
}

static void 
append_col(sql_trans *tr, sql_column *c, void *i, int tpe)
{
	sql_bpm *p = c->data;

	c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->stime;
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		append_bat(p, i);
	else
		append_val(p, i);
}

static void
append_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	sql_bpm *p = i->data;

	i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->stime;
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		append_bat(p, ib);
	else
		append_val(p, ib);
}

static void
delete_bat( sql_dbpm *p, BAT *del ) 
{
	int i;
	oid lv = 0; 
	oid hv = lv + BPM_SPLIT; 

	for (i = 0; i<p->nr; i++) {
		BAT *b = BATselect_(del, (ptr)&lv, (ptr)&hv, TRUE, FALSE);
		delta_delete_bat( &p->parts[i], b);
		bat_destroy(b);

		lv = hv;
		hv += BPM_SPLIT;
	}
}

static void
delete_val( sql_dbpm *p, oid rid ) 
{
	sht nr = (sht) (rid?rid/BPM_SPLIT:0);

	assert(rid != oid_nil);
	if (nr >= p->nr)
		nr = p->nr-1;
	delta_delete_val( &p->parts[nr], rid);
}

static void
delete_tab(sql_trans *tr, sql_table * t, void *ib, int tpe)
{
	sql_dbpm *p = t->data;

	t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	t->base.rtime = t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		delete_bat(p, ib);
	else
		delete_val(p, *(oid*)ib);
}

static size_t
count_col(sql_column *col)
{
	sql_bpm *b = col->data;
	return b->parts[0].cnt;
}

static size_t
count_idx(sql_idx *idx)
{
	sql_bpm *b = idx->data;
	return b->parts[0].cnt;
}

static int
sorted_col(sql_trans *tr, sql_column *col)
{
	BAT *b = bind_col(tr, col, RDONLY);
	int sorted = BATtordered(b);
	
	bat_destroy(b);
	return sorted;
}

static int 
load_bpm (sql_bpm *p) 
{
	oid pid;
	sht nr, i;
	BAT *part_name = bpm_part_name();
	BUN r;

	r = BUNfnd(BATmirror(part_name), p->name);
	if (r == BUN_NONE)
		return LOG_ERR;
	pid = *(oid*)Hloc(part_name, r);
	if (r == BUN_NONE)
		return LOG_ERR;
	r = BUNfnd(part_nr, &pid);
	nr = *(int*)Tloc(part_nr, r);

	for(i = 0; i<nr; i++) {
		char *name = sql_message("%s_%d", p->name, i);
		int bid = logger_find_bat(bat_logger, name);

		use_part(p);
		p->parts[i].name = name;
		load_delta( &p->parts[i], bid, p->type);
		p -> nr++;
	}
	p->created = p->nr;
	return LOG_OK;
}

static int
new_persistent_bpm(sql_bpm *p, int sz) 
{
	int ok = LOG_OK, i;

	for(i = 0; i<p->nr && ok == LOG_OK; i++) {
		ok = new_persistent_delta( &p->parts[i], sz );
	}
	return ok;
}

static int
create_col(sql_trans *tr, sql_column *c)
{
	int ok = LOG_OK;
	sql_bpm *p = c->data;

	if (!p)
		c->data = p = ZNEW(sql_bpm);
	if (!p->name)
		create_parts(p, c->type.type->localtype, sql_message("%s_%s_%s", c->t->s->base.name, c->t->base.name, c->base.name));

	(void)tr;
	if (c->base.flag == TR_OLD && !isTempTable(c->t)){
		return load_bpm( c->data);
	} else if (p && p->nr && !isTempTable(c->t)) {
		return new_persistent_bpm(c->data, c->t->sz );
	} else if (!p->nr) {
		BAT *i = bat_new(TYPE_void, p->type, c->t->sz);
		if (!i) 
			return LOG_ERR;
		extend_part(p, NULL, i, 0);
		bat_destroy(i);
	}
	return ok;
}

/* will be called for new idx's and when new index columns are create */
static int
create_idx(sql_trans *tr, sql_idx *ni)
{
	int ok = LOG_OK;
	sql_bpm *p = ni->data;
	int type = TYPE_wrd;

	if (ni->type == join_idx)
		type = TYPE_oid;

	if (!p)
		ni->data = p = ZNEW(sql_bpm);
	if (!p->name)
		create_parts(p, type, sql_message("%s_%s_%s", ni->t->s->base.name, ni->t->base.name, ni->base.name));

	(void)tr;
	/* create bats for a loaded idx structure */
	if (ni->base.flag == TR_OLD && !isTempTable(ni->t)){
		return load_bpm( ni->data);
	} else if (p && p->nr && !isTempTable(ni->t)) { /* create bats for a new persistent idx */
		return new_persistent_bpm( ni->data, ni->t->sz );
	} else if (!p->nr) {
		BAT *i = bat_new(TYPE_void, type, ni->t->sz);
		if (!i) 
			return LOG_ERR;
		extend_part(p, NULL, i, 0);
		bat_destroy(i);
	}
	return ok;
}

static int 
load_dbpm (sql_dbpm *p) 
{
	oid pid;
	sht nr, i;
	BAT *part_name = bpm_part_name();
	BUN r;

	r = BUNfnd(BATmirror(part_name), p->name);
	if (r == BUN_NONE)
		return LOG_ERR;
	pid = *(oid*)Hloc(part_name, r);
	r = BUNfnd(part_nr, &pid);
	if (r == BUN_NONE)
		return LOG_ERR;
	nr = *(int*)Tloc(part_nr, r);

	for(i = 0; i<nr; i++) {
		char *dname = sql_message("%s_%d", p->name, i);
		int bid = logger_find_bat(bat_logger, dname);

		use_dpart(p);
		p->parts[i].dname = dname;
		load_dbat( &p->parts[i], bid);
		p->nr++;
	}
	p->created = p->nr;
	return LOG_OK;
}

static int
new_persistent_dbpm(sql_dbpm *p) 
{
	int ok = LOG_OK, i;

	for(i = 0; i<p->nr && ok == LOG_OK; i++) {
		ok = new_persistent_dbat( &p->parts[i]);
	}
	return ok;
}

static int
create_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;
	BAT *b;
	sql_dbpm *p = t->data;

	if (!p)
		p = t->data = ZNEW(sql_bpm);
	p->name = sql_message("D_%s_%s", t->s->base.name, t->base.name);
	create_dparts(p);

	(void)tr;
	if (t->base.flag == TR_OLD && !isTempTable(t)) {
		return load_dbpm(t->data);
	} else if (p && p->nr && !isTempTable(t)) {
		return new_persistent_dbpm(t->data);
	} else if (!p->nr) {
		b = bat_new(TYPE_void, TYPE_oid, t->sz);
		if (!b) 
			return LOG_ERR;
		extend_dpart(p, b);
		bat_destroy(b);
	}
	return ok;
}

static int
log_create_bpm(sql_bpm *p)
{
	int ok = LOG_OK, i;

	if (!p->pid)
		p -> pid = PartID();
	bpm_new_part(p->pid, p->name, p->nr);
	for(i = 0; i<p->nr && ok == LOG_OK; i++) {
		ok = log_create_delta( &p->parts[i] );
	}
	return ok;
}

static int
log_create_col(sql_trans *tr, sql_column *c)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(c->t));
	return log_create_bpm(c->data);
}

static int
log_create_idx(sql_trans *tr, sql_idx *ni)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(ni->t));
	return log_create_bpm(ni->data);
}

static int
log_create_dbpm( sql_dbpm *p )
{
	int ok = LOG_OK, i;

	if (!p->pid)
		p -> pid = PartID();
	bpm_new_part(p->pid, p->name, p->nr);
	for(i = 0; i<p->nr && ok == LOG_OK; i++){
		ok = log_create_dbat( &p->parts[i] );
	}
	return ok;
}

static int
log_create_del(sql_trans *tr, sql_table *t)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(t));
	return log_create_dbpm(t->data);
}

static int 
dup_bpm(sql_trans *tr, sql_table *t, sql_bpm *op, sql_bpm *p, int type, int oc_isnew, int c_isnew)
{
	int ok = LOG_OK, i;

	if (!p->nr) 
		create_parts(p, op->type, _strdup(op->name));

	for (i=0; i<op->nr && ok == LOG_OK; i++) {
		use_part(p);

		ok = dup_delta(tr, &op->parts[i], &p->parts[i], type, oc_isnew, c_isnew, isTempTable(t), t->sz);
		p -> nr++;
	}
	return ok;
}

static int 
dup_col(sql_trans *tr, sql_column *oc, sql_column *c )
{
	if (oc->data) {
		int type = c->type.type->localtype;
		sql_bpm *p = c->data = ZNEW(sql_bpm), *op = oc->data;
		return dup_bpm(tr, c->t, op, p, type, isNew(oc), c->base.flag == TR_NEW);
	}
	return LOG_OK;
}

static int 
dup_idx(sql_trans *tr, sql_idx *oi, sql_idx *i )
{
	if (oi->data) {
		int type = (i->type==join_idx)?TYPE_oid:TYPE_wrd;
		sql_bpm *p = i->data = ZNEW(sql_bpm), *op = oi->data;
		return dup_bpm(tr, i->t, op, p, type, isNew(oi), i->base.flag == TR_NEW);
	}
	return LOG_OK;
}

static int 
dup_dbpm(sql_trans *tr, sql_table *t, sql_dbpm *op, sql_dbpm *p)
{
	int ok = LOG_OK, i;
	
	for (i=0; i<op->nr && ok == LOG_OK; i++) {
		use_dpart(p);

		ok = dup_dbat(tr, &op->parts[i], &p->parts[i], isNew(t), isTempTable(t));
		p -> nr++;
	}
	return ok;
}

static int
dup_del(sql_trans *tr, sql_table *ot, sql_table *t)
{
	if (ot->data) {
		sql_dbpm *p = t->data = ZNEW(sql_dbpm), *op = ot->data;
		return dup_dbpm(tr, t, op, p);
	}
	return LOG_OK;
}

/*
static void
bpm_destroy_part(oid id)
{
	BUNdelHead(part_name, (ptr)&id, FALSE);
	BUNdelHead(part_nr,   (ptr)&id, FALSE);
}
*/

static int
destroy_bpm(sql_bpm *p)
{
	int ok = LOG_OK, i;

	for(i = 0; i < p->nr && ok == LOG_OK; i++) {
		ok = destroy_delta( &p->parts[i] );
	}
	if (p->name)
		_DELETE(p->name);
	_DELETE(p->parts);
	_DELETE(p);
	return ok;
}

static int
log_destroy_bpm(sql_bpm *p)
{
	int ok = LOG_OK, i;

	for(i = 0; i < p->nr && ok == LOG_OK; i++) {
		ok = log_destroy_delta( &p->parts[i] );
	}
	return ok;
}

static int
destroy_dbpm(sql_dbpm *p)
{
	int ok = LOG_OK, i;

	for(i = 0; i < p->nr && ok == LOG_OK; i++) {
		ok = destroy_dbat( &p->parts[i] );
	}
	if (p->name)
		_DELETE(p->name);
	_DELETE(p->parts);
	_DELETE(p);
	return ok;
}

static int
log_destroy_dbpm(sql_dbpm *p)
{
	int ok = LOG_OK, i;

	for(i = 0; i < p->nr && ok == LOG_OK; i++) {
		ok = log_destroy_dbat( &p->parts[i] );
	}
	return ok;
}

static int
destroy_col(sql_trans *tr, sql_column *c)
{
	int ok;

        if ((tr && tr->parent == gtrans) || !c->data)
                return LOG_OK;
 	ok = destroy_bpm(c->data);
	c->data = NULL;
	return ok;
}

static int
destroy_idx(sql_trans *tr, sql_idx *i)
{
	int ok;

        if ((tr && tr->parent == gtrans) || !i->data)
                return LOG_OK;
 	ok = destroy_bpm(i->data);
	i->data = NULL;
	return ok;
}

static int
destroy_del(sql_trans *tr, sql_table *t)
{
	int ok;

        if ((tr && tr->parent == gtrans) || !t->data)
                return LOG_OK;
 	ok = destroy_dbpm(t->data);
	t->data = NULL;
	return ok;
}

static int
log_destroy_col(sql_trans *tr, sql_column *c)
{
	(void)tr;
	return log_destroy_bpm(c->data);
}

static int
log_destroy_idx(sql_trans *tr, sql_idx *i)
{
	(void)tr;
	return log_destroy_bpm(i->data);
}

static int
log_destroy_del(sql_trans *tr, sql_table *t)
{
	(void)tr;
	return log_destroy_dbpm(t->data);
}

static BUN
clear_bpm(sql_trans *tr, sql_bpm *p)
{
	BUN sz = 0;
	int i;

	for (i=0; i<p->nr; i++) {
		sz += clear_delta(tr, &p->parts[i]);
	}
	return sz;
}

static BUN 
clear_col(sql_trans *tr, sql_column *c)
{
	if (c->data)
		return clear_bpm(tr, c->data);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i)
{
	if (i->data)
		return clear_bpm(tr, i->data);
	return 0;
}

static BUN
clear_del(sql_trans *tr, sql_table *t)
{
	sql_dbpm *p = t->data;
	BUN sz = 0;
	int i;

	for (i=0; i<p->nr; i++) {
		sz += clear_dbat(tr, &p->parts[i]);
	}
	return sz;
}

static int 
update_bpm( sql_trans *tr, sql_bpm *op, sql_bpm *cp)
{
	int i, ok = LOG_OK;

	/* todo handle increase of parts !! */
	for (i = 0; i<cp->nr && ok == LOG_OK; i++) {
		tr_update_delta(tr, &op->parts[i], &cp->parts[i], BPM_SPLIT);
	}
	return ok;
}

static int 
update_dbpm( sql_trans *tr, sql_dbpm *op, sql_dbpm *cp, int cleared)
{
	int i, ok = LOG_OK;

	/* todo handle increase of parts !! */
	for (i = 0; i<cp->nr && ok == LOG_OK; i++) {
		tr_update_dbat( tr, &op->parts[i], &cp->parts[i], cleared);	
	}
	return ok;
}

static int
bpm_update_table(sql_trans *tr, sql_table *ft, sql_table *tt)
{
	int ok = LOG_OK;
	node *n, *m;

	if (ft->cleared) {
		(void)store_funcs.clear_del(tr->parent, tt);
		for (n = tt->columns.set->h; n; n = n->next) 
			(void)store_funcs.clear_col(tr->parent, n->data);
		if (tt->idxs.set) 
			for (n = tt->idxs.set->h; n; n = n->next) 
				(void)store_funcs.clear_idx(tr->parent, n->data);
	}

	ok = update_dbpm(tr, tt->data, ft->data, ft->cleared);
	for (n = ft->columns.set->h, m = tt->columns.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
		sql_column *cc = n->data;
		sql_column *oc = m->data;

		if (!cc->base.wtime) 
			continue;
		update_bpm(tr, oc->data, cc->data);

		if (cc->base.rtime)
			oc->base.rtime = tr->stime;
		oc->base.wtime = tr->stime;
		cc->base.rtime = cc->base.wtime = 0;
	}
	if (ok == LOG_OK && tt->idxs.set) {
		for (n = ft->idxs.set->h, m = tt->idxs.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
			sql_idx *ci = n->data;
			sql_idx *oi = m->data;

			/* some indices have no bats */
			if (!oi->data)
				continue;

			if (!ci->base.wtime) 
				continue;
			update_bpm(tr, oi->data, ci->data);

			if (ci->base.rtime)
				oi->base.rtime = tr->stime;
			oi->base.wtime = tr->stime;
			ci->base.rtime = ci->base.wtime = 0;
		}
	}
	return ok;
}

static int 
tr_log_bpm( sql_trans *tr, sql_bpm *cp, int cleared)
{
	int i, ok = LOG_OK;

	/* todo handle increase of parts !! */
	for (i = 0; i<cp->nr && ok == LOG_OK; i++) {
		ok = tr_log_delta(tr, &cp->parts[i], cleared);
	}
	return ok;
}

static int 
tr_log_dbpm( sql_trans *tr, sql_dbpm *cp, int cleared)
{
	int i, ok = LOG_OK;

	/* todo handle increase of parts !! */
	for (i = 0; i<cp->nr && ok == LOG_OK; i++) {
		ok = tr_log_dbat( tr, &cp->parts[i], cleared);	
	}
	return ok;
}

static int
bpm_log_table(sql_trans *tr, sql_table *ft)
{
	int ok = LOG_OK;
	node *n;

	ok = tr_log_dbpm(tr, ft->data, ft->cleared);
	for (n = ft->columns.set->h; ok == LOG_OK && n; n = n->next) {
		sql_column *cc = n->data;

		if (!cc->base.wtime) 
			continue;
		ok = tr_log_bpm(tr, cc->data, ft->cleared);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime)
				continue;
			ok = tr_log_bpm(tr, ci->data, ft->cleared);
		}
	}
	return ok;
}



static int bpm_none(){ return LOG_OK; }

int
bpm_storage_init( store_functions *sf)
{
	sf->bind_col = (bind_col_fptr)&bind_col;
	sf->bind_idx = (bind_idx_fptr)&bind_idx;
	sf->bind_del = (bind_del_fptr)&bind_del;

	sf->append_col = (append_col_fptr)&append_col;
	sf->append_idx = (append_idx_fptr)&append_idx;
	sf->update_col = (update_col_fptr)&update_col;
	sf->update_idx = (update_idx_fptr)&update_idx;
	sf->delete_tab = (delete_tab_fptr)&delete_tab;

	sf->count_col = (count_col_fptr)&count_col;
	sf->count_idx = (count_idx_fptr)&count_idx;
	sf->sorted_col = (sorted_col_fptr)&sorted_col;

	sf->create_col = (create_col_fptr)&create_col;
	sf->create_idx = (create_idx_fptr)&create_idx;
	sf->create_del = (create_del_fptr)&create_del;

	sf->log_create_col = (create_col_fptr)&log_create_col;
	sf->log_create_idx = (create_idx_fptr)&log_create_idx;
	sf->log_create_del = (create_del_fptr)&log_create_del;

	sf->snapshot_create_col = (create_col_fptr)&bpm_none;
	sf->snapshot_create_idx = (create_idx_fptr)&bpm_none;
	sf->snapshot_create_del = (create_del_fptr)&bpm_none;

	sf->dup_col = (dup_col_fptr)&dup_col;
	sf->dup_idx = (dup_idx_fptr)&dup_idx;
	sf->dup_del = (dup_del_fptr)&dup_del;

	sf->destroy_col = (destroy_col_fptr)&destroy_col;
	sf->destroy_idx = (destroy_idx_fptr)&destroy_idx;
	sf->destroy_del = (destroy_del_fptr)&destroy_del;

	sf->log_destroy_col = (destroy_col_fptr)&log_destroy_col;
	sf->log_destroy_idx = (destroy_idx_fptr)&log_destroy_idx;
	sf->log_destroy_del = (destroy_del_fptr)&log_destroy_del;

	sf->clear_col = (clear_col_fptr)&clear_col;
	sf->clear_idx = (clear_idx_fptr)&clear_idx;
	sf->clear_del = (clear_del_fptr)&clear_del;

	sf->update_table = (update_table_fptr)&bpm_update_table;
	sf->log_table = (update_table_fptr)&bpm_log_table;
	sf->snapshot_table = (update_table_fptr)&bpm_none;
	sf->gtrans_update = (gtrans_update_fptr)NULL;

	MT_lock_init(&part_lock, "SQL_part_lock");
	return LOG_OK;
}

