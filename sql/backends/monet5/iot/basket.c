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
 * Copyright August 2008-2016 MonetDB B.V.
 * All Rights Reserved.
 */

/* author: M. Kersten
 * Continuous query processing relies on event baskets
 * passed through a processing pipeline. The baskets
 * are derived from ordinary SQL tables where the delta
 * processing is ignored.
 *
 */

#include "monetdb_config.h"
#include <gdk.h>
#include "iot.h"
#include "basket.h"
#include "receptor.h"
#include "mal_exception.h"
#include "mal_builder.h"
#include "opt_prelude.h"

str statusname[6] = { "<unknown>", "init", "paused", "running", "stop", "error" };

BasketRec *baskets;   /* the global iot catalog */
static int bsktTop = 0, bsktLimit = 0;

// Find an empty slot in the basket catalog
static int BSKTnewEntry(void)
{
	int i;
	if (bsktLimit == 0) {
		bsktLimit = MAXBSK;
		baskets = (BasketRec *) GDKzalloc(bsktLimit * sizeof(BasketRec));
		bsktTop = 1; /* entry 0 is used as non-initialized */
	} else if (bsktTop +1 == bsktLimit) {
		bsktLimit += MAXBSK;
		baskets = (BasketRec *) GDKrealloc(baskets, bsktLimit * sizeof(BasketRec));
	}
	for (i = 1; i < bsktLimit; i++)
		if (baskets[i].table == NULL)
			break;
	bsktTop++;
	return i;
}


// free all malloced space
static void
BSKTclean(int idx)
{
	int i;
	GDKfree(baskets[idx].schema);
	GDKfree(baskets[idx].table);
	baskets[idx].schema = NULL;
	baskets[idx].table = NULL;
	if (baskets[idx].cols) {
		for (i = 0; i < baskets[idx].count; i++)
			GDKfree(baskets[idx].cols[i]);
		GDKfree(baskets[idx].cols);
		baskets[idx].cols = NULL;
	}
	GDKfree(baskets[idx].bats);
	baskets[idx].bats = NULL;
	BBPreclaim(baskets[idx].errors);
	baskets[idx].errors = NULL;
	baskets[idx].count = 0;
	MT_lock_destroy(&baskets[idx].lock);
}

// locate the basket in the catalog
int
BSKTlocate(str sch, str tbl)
{
	int i;

	if( sch == 0 || tbl == 0)
		return 0;
	for (i = 1; i < bsktTop; i++)
		if (baskets[i].schema && strcmp(sch, baskets[i].schema) == 0 &&
			baskets[i].table && strcmp(tbl, baskets[i].table) == 0)
			return i;
	return 0;
}

// Instantiate a basket description for a particular table
str
BSKTnewbasket(sql_schema *s, sql_table *t, sql_trans *tr)
{
	int idx, i;
	node *o;
	BAT *b=  NULL;
	sql_column  *c;
	str msg= MAL_SUCCEED;
	Receptor rc;

	// Don't introduce the same basket twice
	if( BSKTlocate(s->base.name, t->base.name) > 0)
		return msg;
	MT_lock_set(&iotLock);
	idx = BSKTnewEntry();
	MT_lock_init(&baskets[idx].lock,"newbasket");

	baskets[idx].schema = GDKstrdup(s->base.name);
	baskets[idx].table = GDKstrdup(t->base.name);
	baskets[idx].seen = * timestamp_nil;

	baskets[idx].count = 0;
	for (o = t->columns.set->h; o; o = o->next)
		baskets[idx].count++;
	baskets[idx].cols = GDKzalloc((baskets[idx].count + 1) * sizeof(str));
	baskets[idx].bats = GDKzalloc((baskets[idx].count + 1) * sizeof(BAT *));
	baskets[idx].errors = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (baskets[idx].table == NULL ||
	    baskets[idx].cols == NULL ||
	    baskets[idx].bats == NULL ||
	    baskets[idx].errors == NULL) {
		BSKTclean(idx);
		MT_lock_unset(&iotLock);
		throw(MAL,"baskets.register",MAL_MALLOC_FAIL);
	}

	i = 0;
	for (o = t->columns.set->h; o; o = o->next) {
		c = o->data;
		b =store_funcs.bind_col(tr, c, RD_INS);
		if (b == NULL) {
			BSKTclean(idx);
			MT_lock_unset(&iotLock);
			throw(MAL,"baskets.register","Can not locate stream column '%s.%s.%s'",s->base.name, t->base.name, c->base.name);
		}
		baskets[idx].bats[i] = b->batCacheid;
		if ((baskets[idx].cols[i++] = GDKstrdup(c->base.name)) == NULL) {
			BSKTclean(idx);
			MT_lock_unset(&iotLock);
			throw(MAL,"baskets.register",MAL_MALLOC_FAIL);
		}
		BBPkeepref(b->batCacheid);
	}
	MT_lock_unset(&iotLock);
	// start the receptor for this basket
	rc = RCnew(baskets+idx);
	if ( rc && MT_create_thread(&rc->pid, (void (*)(void *))RCreceptor, rc, MT_THR_JOINABLE) != 0)
			throw(SQL, "receptor.start", "Receptor '%s.%s' init failed", baskets[idx].schema,baskets[idx].table);
	return msg;
}

// MAL/SQL interface for registration of a single table
str
BSKTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	sql_schema  *s;
	sql_table   *t;
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_trans *tr;
	str sch, tbl;
	int idx;

	if ( msg != MAL_SUCCEED)
		return msg;
	sch = *getArgReference_str(stk, pci, 1);
	tbl = *getArgReference_str(stk, pci, 2);

	/* check double registration */
	if( BSKTlocate(sch, tbl) > 0)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	tr = m->session->tr;
	s = mvc_bind_schema(m, sch);
	if (s == NULL)
		throw(SQL, "iot.register", "Schema missing");

	t = mvc_bind_table(m, s, tbl);
	if (t == NULL)
		throw(SQL, "iot.register", "Table missing '%s'", tbl);

	msg=  BSKTnewbasket(s, t, tr);
	idx = BSKTlocate(sch,tbl);
	if( msg == MAL_SUCCEED && idx > 0)
		msg= RCstart(idx);
	return msg;
}

str
BSKTbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	str sch = *getArgReference_str(stk,pci,1);
	str tbl = *getArgReference_str(stk,pci,2);
	str col = *getArgReference_str(stk,pci,3);
	int idx,i;
	BAT *b;

	(void) cntxt;
	(void) mb;

	*ret = 0;
	idx= BSKTlocate(sch,tbl);
	if (idx <0)
		throw(SQL,"iot.bind","Stream table not registered");

	for(i=0; i < baskets[idx].count; i++)
		if( strcmp(baskets[idx].cols[i], col)== 0 ){
			b= BATdescriptor(baskets[idx].bats[i]);
			if( b)
				BBPkeepref(*ret =  b->batCacheid);
			return MAL_SUCCEED;
		}
	throw(SQL,"iot.bind","Stream table column '%s.%s.%s' not found",sch,tbl,col);
}

/*
 * The locks are designated towards the baskets.
 * If you can not grab the lock then we have to wait.
 */
str BSKTlock(void *ret, str *sch, str *tbl, int *delay)
{
	int bskt;

	bskt = BSKTlocate(*sch, *tbl);
	if (bskt == 0)
		throw(SQL, "basket.lock", "Could not find the basket %s.%s",*sch,*tbl);
#ifdef _DEBUG_BASKET
	stream_printf(BSKTout, "lock group %s.%s\n", *sch, *tbl);
#endif
	MT_lock_set(&baskets[bskt].lock);
#ifdef _DEBUG_BASKET
	stream_printf(BSKTout, "got  group locked %s.%s\n", *sch, *tbl);
#endif
	(void) delay;  /* control spinlock */
	(void) ret;
	return MAL_SUCCEED;
}


str BSKTlock2(void *ret, str *sch, str *tbl)
{
	int delay = 0;
	return BSKTlock(ret, sch, tbl, &delay);
}

str BSKTunlock(void *ret, str *sch,str *tbl)
{
	int bskt;

	(void) ret;
	bskt = BSKTlocate(*sch,*tbl);
	if (bskt == 0)
		throw(SQL, "basket.lock", "Could not find the basket %s.%s",*sch,*tbl);
	MT_lock_unset(&baskets[bskt].lock);
	return MAL_SUCCEED;
}


str
BSKTdrop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int bskt;
	str sch= *getArgReference_str(stk,pci,1);
	str tbl= *getArgReference_str(stk,pci,2);

	(void) cntxt;
	(void) mb;
	bskt = BSKTlocate(sch,tbl);
	if (bskt == 0)
		throw(SQL, "basket.drop", "Could not find the basket %s.%s",sch,tbl);
	BSKTclean(bskt);
	return MAL_SUCCEED;
}

str
BSKTreset(void *ret)
{
	int i;
	(void) ret;
	for (i = 1; i < bsktLimit; i++)
		if (baskets[i].table)
			BSKTclean(i);
	return MAL_SUCCEED;
}

str
BSKTdump(void *ret)
{
	int bskt;
	BUN cnt;
	BAT *b;

	mnstr_printf(GDKout, "#baskets table\n");
	for (bskt = 1; bskt < bsktLimit; bskt++)
		if (baskets[bskt].table) {
			cnt = 0;
			if( baskets[bskt].bats[0]){
				b = BBPquickdesc(baskets[bskt].bats[0], TRUE);
				if( b)
					cnt = BATcount(b);
			}
			mnstr_printf(GDKout, "#baskets[%2d] %s.%s columns %d threshold %d window=[%d,%d] time window=[" LLFMT "," LLFMT "] beat " LLFMT " milliseconds" BUNFMT"\n",
					bskt,
					baskets[bskt].schema,
					baskets[bskt].table,
					baskets[bskt].count,
					baskets[bskt].threshold,
					baskets[bskt].winsize,
					baskets[bskt].winstride,
					baskets[bskt].timeslice,
					baskets[bskt].timestride,
					baskets[bskt].beat,
					cnt);
		}

	(void) ret;
	return MAL_SUCCEED;
}

str
BSKTupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

InstrPtr
BSKTgrabInstruction(MalBlkPtr mb, str sch, str tbl)
{
	int i, j, bskt;
	InstrPtr p;
	BAT *b;

	bskt = BSKTlocate(sch,tbl);
	if (bskt == 0)
		return 0;
	p = newFcnCall(mb, basketRef, grabRef);
	p->argc = 0;
	for (i = 0; i < baskets[bskt].count; i++) {
		b = BBPquickdesc(baskets[bskt].bats[i], FALSE);
		j = newTmpVariable(mb, newBatType(TYPE_oid, b->ttype));
		setVarUDFtype(mb, j);
		setVarFixed(mb, j);
		p = pushArgument(mb, p, j);
	}
	p->retc = p->argc;
	p = pushStr(mb, p, sch);
	p = pushStr(mb, p, tbl);
	return p;
}

InstrPtr
BSKTupdateInstruction(MalBlkPtr mb, str sch, str tbl)
{
	int i, j, bskt;
	InstrPtr p;
	BAT *b;

	bskt = BSKTlocate(sch,tbl);
	if (bskt == 0)
		return 0;
	p = newInstruction(mb, ASSIGNsymbol);
	getArg(p, 0) = newTmpVariable(mb, TYPE_any);
	getModuleId(p) = basketRef;
	getFunctionId(p) = putName("update", 6);
	p = pushStr(mb, p, sch);
	p = pushStr(mb, p, tbl);
	for (i = 0; i < baskets[bskt].count; i++) {
		b = BBPquickdesc(baskets[bskt].bats[i], FALSE);
		j = newTmpVariable(mb, newBatType(TYPE_oid, b->ttype));
		p = pushArgument(mb, p, j);
	}
	return p;
}

/* provide a tabular view for inspection */
str
BSKTtable(bat *schemaId, bat *nameId, bat *thresholdId, bat * winsizeId, bat *winstrideId, bat *timesliceId, bat *timestrideId, bat *beatId, bat *seenId, bat *eventsId)
{
	BAT *schema= NULL, *name = NULL, *seen = NULL, *events = NULL;
	BAT *threshold = NULL, *winsize = NULL, *winstride = NULL, *beat = NULL;
	BAT *timeslice = NULL, *timestride = NULL;
	int i;

	schema = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (schema == 0)
		goto wrapup;
	BATseqbase(schema, 0);
	name = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (name == 0)
		goto wrapup;
	BATseqbase(name, 0);
	threshold = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (threshold == 0)
		goto wrapup;
	BATseqbase(threshold, 0);
	winsize = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (winsize == 0)
		goto wrapup;
	BATseqbase(winsize, 0);
	winstride = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (winstride == 0)
		goto wrapup;
	BATseqbase(winstride, 0);
	beat = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (beat == 0)
		goto wrapup;
	BATseqbase(beat, 0);
	seen = BATnew(TYPE_void, TYPE_timestamp, BATTINY, TRANSIENT);
	if (seen == 0)
		goto wrapup;
	BATseqbase(seen, 0);
	events = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (events == 0)
		goto wrapup;
	BATseqbase(events, 0);

	timeslice = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (timeslice == 0)
		goto wrapup;
	BATseqbase(timeslice, 0);
	timestride = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (timestride == 0)
		goto wrapup;
	BATseqbase(timestride, 0);

	for (i = 1; i < bsktTop; i++)
		if (baskets[i].table) {
			BUNappend(schema, baskets[i].schema, FALSE);
			BUNappend(name, baskets[i].table, FALSE);
			BUNappend(threshold, &baskets[i].threshold, FALSE);
			BUNappend(winsize, &baskets[i].winsize, FALSE);
			BUNappend(winstride, &baskets[i].winstride, FALSE);
			BUNappend(beat, &baskets[i].beat, FALSE);
			BUNappend(seen, &baskets[i].seen, FALSE);
			baskets[i].events = 0; //(int) BATcount( baskets[i].bats[0]);
			BUNappend(events, &baskets[i].events, FALSE);
			BUNappend(timeslice, &baskets[i].timeslice, FALSE);
			BUNappend(timestride, &baskets[i].timestride, FALSE);
		}

	BBPkeepref(*schemaId = schema->batCacheid);
	BBPkeepref(*nameId = name->batCacheid);
	BBPkeepref(*thresholdId = threshold->batCacheid);
	BBPkeepref(*winsizeId = winsize->batCacheid);
	BBPkeepref(*winstrideId = winstride->batCacheid);
	BBPkeepref(*timesliceId = timeslice->batCacheid);
	BBPkeepref(*timestrideId = timestride->batCacheid);
	BBPkeepref(*beatId = beat->batCacheid);
	BBPkeepref(*seenId = seen->batCacheid);
	BBPkeepref(*eventsId = events->batCacheid);
	return MAL_SUCCEED;
wrapup:
	if (name)
		BBPunfix(name->batCacheid);
	if (threshold)
		BBPunfix(threshold->batCacheid);
	if (winsize)
		BBPunfix(winsize->batCacheid);
	if (winstride)
		BBPunfix(winstride->batCacheid);
	if (timeslice)
		BBPunfix(timeslice->batCacheid);
	if (timestride)
		BBPunfix(timestride->batCacheid);
	if (beat)
		BBPunfix(beat->batCacheid);
	if (seen)
		BBPunfix(seen->batCacheid);
	if (events)
		BBPunfix(events->batCacheid);
	throw(SQL, "iot.baskets", MAL_MALLOC_FAIL);
}

str
BSKTtableerrors(bat *nameId, bat *errorId)
{
	BAT  *name, *error;
	BATiter bi;
	BUN p, q;
	int i;
	name = BATnew(TYPE_void, TYPE_str, BATTINY, PERSISTENT);
	if (name == 0)
		throw(SQL, "baskets.errors", MAL_MALLOC_FAIL);
	error = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (error == 0) {
		BBPunfix(name->batCacheid);
		throw(SQL, "baskets.errors", MAL_MALLOC_FAIL);
	}

	for (i = 1; i < bsktTop; i++)
		if (BATcount(baskets[i].errors) > 0) {
			bi = bat_iterator(baskets[i].errors);
			BATloop(baskets[i].errors, p, q)
			{
				str err = BUNtail(bi, p);
				BUNappend(name, &baskets[i].table, FALSE);
				BUNappend(error, err, FALSE);
			}
		}


	BBPkeepref(*nameId = name->batCacheid);
	BBPkeepref(*errorId = error->batCacheid);
	return MAL_SUCCEED;
}
