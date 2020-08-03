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
 * Copyright August 2008-2020 MonetDB B.V.
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
#include <unistd.h>
#include "gdk.h"
#include "sql_basket.h"
#include "mal_exception.h"
#include "mal_builder.h"
#include "opt_prelude.h"

#define _DEBUG_BASKET_ if(0)

BasketRec *baskets;   /* the global timetrails catalog */
static int bsktTop = 0, bsktLimit = 0;

// Initialise a basket, but leave the caller to deal with the lock
#define BSKTinit(idx)                       \
	baskets[idx].table = NULL;          \
	baskets[idx].cols = NULL;           \
	baskets[idx].bats = NULL;           \
	baskets[idx].ncols = 0;             \
	baskets[idx].count = 0;             \
	baskets[idx].window = 0;            \
	baskets[idx].stride = STRIDE_ALL;   \
	baskets[idx].seen = timestamp_nil;  \
	baskets[idx].events = 0;            \
	baskets[idx].error = NULL;

// locate the basket in the basket catalog
int
BSKTlocate(str sch, str tbl)
{
	int i;

	if( sch == 0 || tbl == 0)
		return 0;
	for (i = 1; i < bsktTop; i++)
		if (baskets[i].table && baskets[i].table->s && strcmp(sch, baskets[i].table->s->base.name) == 0 &&
			strcmp(tbl, baskets[i].table->base.name) == 0)
			return i;
	return 0;
}

// Find an empty slot in the basket catalog
static int 
BSKTnewEntry(void)
{
	int i = bsktTop;
	BasketRec *bnew;

	if( baskets == 0){
		bnew = (BasketRec *) GDKzalloc((INTIAL_BSKT) * sizeof(BasketRec));
		if( bnew == 0)
			return 0;
		bsktLimit = INTIAL_BSKT;
		baskets = bnew;
	} else
	if (bsktTop + 1 == bsktLimit) {
		bnew = (BasketRec *) GDKrealloc(baskets, (bsktLimit+INTIAL_BSKT) * sizeof(BasketRec));
		if( bnew == 0)
			return 0;
		bsktLimit += INTIAL_BSKT;
		baskets = bnew;
		for (i = bsktLimit-INTIAL_BSKT; i < bsktLimit; i++){
			BSKTinit(i);
		}
	}
	for (i = 1; i < bsktLimit; i++) { /* find an available slot */
		if (baskets[i].table == NULL)
			break;
	}
	if(i >= bsktTop) { /* if it's the last one we need to increment bsktTop */
		bsktTop++;
	}
	MT_lock_init(&baskets[i].lock,"bsktlock");
	return i;
}

// free a basket structure
void
BSKTclean(int idx)
{	int i;

	if( idx && baskets[idx].table){
		GDKfree(baskets[idx].error);
		baskets[idx].table = NULL;
		baskets[idx].error = NULL;
		baskets[idx].window = 0;
		baskets[idx].stride = STRIDE_ALL;
		baskets[idx].count = 0;
		baskets[idx].events = 0;
		baskets[idx].seen = timestamp_nil;
		for(i=0; i < baskets[idx].ncols ; i++){
			BBPunfix(baskets[idx].bats[i]->batCacheid);
			baskets[idx].bats[i] =NULL;
		}
		baskets[i].ncols = 0;
		GDKfree(baskets[idx].bats);
		GDKfree(baskets[idx].cols);
		baskets[idx].bats = NULL;
		baskets[idx].cols = NULL;
		MT_lock_destroy(&baskets[idx].lock);
	}
}

// MAL/SQL interface for registration of a single table
// Create the internal basket structure before we're going to use this stream table
str
BSKTregisterInternal(Client cntxt, MalBlkPtr mb, str sch, str tbl, int* res)
{
	sql_schema  *s;
	sql_table   *t;
	mvc *m = NULL;
	int i, idx, colcnt=0;
	BAT *b;
	node *o;
	str msg = getSQLContext(cntxt, mb, &m, NULL);

	if ( msg != MAL_SUCCEED)
		return msg;

	/* check double registration */
	if( (idx = BSKTlocate(sch, tbl)) > 0) {
		*res = idx;
		return MAL_SUCCEED;
	}


	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	if (!(s = mvc_bind_schema(m, sch)))
		throw(SQL, "basket.register",SQLSTATE(3F000) "Schema missing\n");

	if (!(t = mvc_bind_table(m, s, tbl)))
		throw(SQL, "basket.register",SQLSTATE(3F000) "Table missing '%s'\n", tbl);

	if( !isStream(t))
		throw(SQL,"basket.register",SQLSTATE(42000) "Only allowed for stream tables\n");

	if((idx = BSKTnewEntry()) < 1)
		throw(MAL,"basket.register",SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* Since we just created a new basket entry to register this stream
	 * table, when anything goes wrong below, we reset this basket to its initial
	 * state. We destroy its lock here, because it's a new entry and no
	 * lock has been set (basket.register is called before basket.lock). */
	// FIXME: BSKTregisterInternal is also called by functions other than
	//          basket.register, which shouldn't destroy the lock.  But
	//          they should have been detected as double registration.
	baskets[idx].table = t;
	baskets[idx].window = t->stream->window;
	baskets[idx].stride = t->stream->stride;
	baskets[idx].error = MAL_SUCCEED;
	baskets[idx].seen = timestamp_current();

	// Check the column types first
	for (o = t->columns.set->h; o ; o = o->next){
		sql_column *col = o->data;
		int tpe = col->type.type->localtype;

		if ( !(tpe <= TYPE_str || tpe == TYPE_date || tpe == TYPE_daytime || tpe == TYPE_timestamp) ) {
			BSKTinit(idx);
			MT_lock_destroy(&baskets[idx].lock);
			throw(MAL,"basket.register",SQLSTATE(42000) "Unsupported type %d\n",tpe);
		}
		colcnt++;
	}
	baskets[idx].ncols = colcnt;
	baskets[idx].bats = GDKmalloc(colcnt * sizeof(BAT **));
	if(baskets[idx].bats == NULL) {
		BSKTinit(idx);
		MT_lock_destroy(&baskets[idx].lock);
		throw(MAL,"basket.register",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	baskets[idx].cols = GDKmalloc(colcnt * sizeof(sql_column **));
	if(baskets[idx].cols == NULL) {
		BSKTinit(idx);
		MT_lock_destroy(&baskets[idx].lock);
		throw(MAL,"basket.register",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	// collect the column names and the storage
	for ( i=0, o = t->columns.set->h; i <colcnt && o; o = o->next, i++){
		sql_column *col = o->data;
		b = COLnew(0, col->type.type->localtype, 0, TRANSIENT);
		assert(b);
		BBPfix(b->batCacheid);
		baskets[idx].bats[i]= b;
		baskets[idx].cols[i]= col;
	}
	*res = idx;
	return MAL_SUCCEED;
}

str
BSKTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch, tbl;
	str msg= MAL_SUCCEED;
	int i;

	(void) stk;
	(void) pci;
	sch = getVarConstant(mb, getArg(pci,2)).val.sval;
	tbl = getVarConstant(mb, getArg(pci,3)).val.sval;
	msg = BSKTregisterInternal(cntxt,mb,sch,tbl, &i);
	return msg;
}

str
BSKTwindow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,1);
	str tbl = *getArgReference_str(stk,pci,2);
	int window = *getArgReference_int(stk,pci,3);
	int stride = DEFAULT_TABLE_STRIDE, idx = 0;
	str msg;

	(void) cntxt;
	(void) mb;
	if( window < 0)
		throw(MAL,"basket.window",SQLSTATE(42000) "negative window not allowed\n");
	if( pci->argc == 5) {
		stride = *getArgReference_int(stk,pci,4);
		if( stride < STRIDE_ALL)
			throw(MAL,"basket.stride",SQLSTATE(42000) "negative stride not allowed\n");
		if( window < stride)
			throw(MAL,"basket.window",SQLSTATE(42000) "the window size must not be smaller than the stride size\n");
	}
	msg= BSKTregisterInternal(cntxt, mb, sch, tbl, &idx);
	if( msg != MAL_SUCCEED)
		return msg;
	baskets[idx].window = window;
	if( pci->argc == 5) {
		baskets[idx].stride = stride;
	}
	return MAL_SUCCEED;
}

// keep and release should be used as a pair
str
BSKTkeep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,1);
	str tbl = *getArgReference_str(stk,pci,2);
	int idx;
	str msg;

	(void) cntxt;
	(void) mb;
	msg= BSKTregisterInternal(cntxt, mb, sch, tbl, &idx);
	if( msg != MAL_SUCCEED)
		return msg;
	if( baskets[idx].window >= 0)
		baskets[idx].window = - baskets[idx].window -1;
	return MAL_SUCCEED;
}

str
BSKTrelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,1);
	str tbl = *getArgReference_str(stk,pci,2);
	int idx;
	str msg;

	(void) cntxt;
	(void) mb;
	msg= BSKTregisterInternal(cntxt, mb, sch, tbl, &idx);
	if( msg != MAL_SUCCEED)
		return msg;
	if( baskets[idx].window < 0)
		baskets[idx].window = - baskets[idx].window -1;
	return MAL_SUCCEED;
}

// Returns the BAT of given sch.tbl.col
static BAT *
BSKTbindColumn(str sch, str tbl, str col)
{
	int idx =0,i;

	if( (idx = BSKTlocate(sch,tbl)) == 0)
		return NULL;

	for( i=0; i < baskets[idx].ncols; i++)
		if( strcmp(baskets[idx].cols[i]->base.name, col)== 0)
			return baskets[idx].bats[i];
	return NULL;
}

// Just return a void tid BAT with a count
str
BSKTtid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	str sch = *getArgReference_str(stk,pci,2);
	str tbl = *getArgReference_str(stk,pci,3);
	BAT *tids, *b;
	int bskt;
	
	(void) cntxt;
	(void) mb;

	bskt = BSKTlocate(sch,tbl);
	if( bskt == 0)	
		throw(SQL,"basket.tid",SQLSTATE(3F000) "Stream table '%s.%s' not found\n",sch,tbl);
	b = baskets[bskt].bats[0];
	if( b == 0)
		throw(SQL,"basket.tid",SQLSTATE(3F000) "Stream table reference column '%s.%s.tid' not accessible\n",sch,tbl);

	tids = COLnew(0, TYPE_void, 0, TRANSIENT);
	if (tids == NULL)
		throw(SQL, "basket.tid",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	tids->tseqbase = 0;
	BATsetcount(tids, (baskets[bskt].window > 0 && cntxt->iscqscheduleruser) ? (BUN) baskets[bskt].window : BATcount(b));
	BATsettrivprop(tids);

	BBPkeepref( *ret = tids->batCacheid);
	return MAL_SUCCEED;
}

str
BSKTbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	str sch = *getArgReference_str(stk,pci,2);
	str tbl = *getArgReference_str(stk,pci,3);
	str col = *getArgReference_str(stk,pci,4);
	BAT *bn, *b;
	int bskt=0;
	str msg= MAL_SUCCEED;

	// first add the basket to the catalog
	msg = BSKTregisterInternal(cntxt,mb,sch,tbl, &bskt);
	if( msg != MAL_SUCCEED)
		return msg;
	b = BSKTbindColumn(sch,tbl,col);
	*ret = 0;
	if( b){
		if( baskets[bskt].window >0 && cntxt->iscqscheduleruser){
			bn = VIEWcreate(0,b);
			if( bn){
				VIEWbounds(b,bn, 0, baskets[bskt].window);
				BBPkeepref(*ret =  bn->batCacheid);
			} else
				throw(SQL,"basket.bind",SQLSTATE(HY005) "Can not create view %s.%s.%s[%d]\n",sch,tbl,col,baskets[bskt].window );
		} else{
			BBPkeepref( *ret = b->batCacheid);
			BBPfix(b->batCacheid); // don't loose it
		}
		return MAL_SUCCEED;
	}
	throw(SQL,"basket.bind",SQLSTATE(3F000) "Stream table column '%s.%s.%s' not found\n",sch,tbl,col);
}

str
BSKTdrop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int bskt;
	str sch= *getArgReference_str(stk,pci,2);
	str tbl= *getArgReference_str(stk,pci,3);

	(void) cntxt;
	(void) mb;
	bskt = BSKTlocate(sch,tbl);
	if (bskt == 0)
		throw(SQL, "basket.drop",SQLSTATE(3F000) "Could not find the basket %s.%s\n",sch,tbl);
	BSKTclean(bskt);
	return MAL_SUCCEED;
}

/* remove tuples from a basket according to the sliding policy */
#define ColumnShift(B,TPE) { \
	TPE *first= (TPE*) Tloc(B, 0);\
	TPE *n, *last=  (TPE*) Tloc(B, BUNlast(B));\
	n = first + stride;\
	for(cnt=0 ; n < last; cnt++, n++, first++)\
		*first=*n;\
}

static str
BSKTtumbleInternal(Client cntxt, str sch, str tbl, int bskt, int window, int stride)
{
	BAT *b;
	BUN cnt= 0 ;
	int i;
	(void) cntxt;

	if( stride < STRIDE_ALL)
		throw(MAL,"basket.tumble",SQLSTATE(42000) "negative stride not allowed\n");
	_DEBUG_BASKET_ fprintf(stderr,"Tumble %s.%s %d elements\n",sch,tbl,stride);
	if( stride == 0)
		return MAL_SUCCEED;
	if( stride == STRIDE_ALL) /*IMPORTANT set the implementation stride size to the window size */
		stride = window;
	for(i=0; i< baskets[bskt].ncols ; i++){
		b = baskets[bskt].bats[i];
		assert( b );

		switch(ATOMstorage(b->ttype)){
			case TYPE_bit:ColumnShift(b,bit); break;
			case TYPE_bte:ColumnShift(b,bte); break;
			case TYPE_sht:ColumnShift(b,sht); break;
			case TYPE_int:ColumnShift(b,int); break;
			case TYPE_oid:ColumnShift(b,oid); break;
			case TYPE_flt:ColumnShift(b,flt); break;
			case TYPE_dbl:ColumnShift(b,dbl); break;
			case TYPE_lng:ColumnShift(b,lng); break;
			#ifdef HAVE_HGE
				case TYPE_hge:ColumnShift(b,hge); break;
			#endif
			case TYPE_str:
				switch(b->twidth){
					case 1: ColumnShift(b,bte); break;
					case 2: ColumnShift(b,sht); break;
					case 4: ColumnShift(b,int); break;
					case 8: ColumnShift(b,lng); break;
				}
				break;
			default:
				throw(SQL, "basket.tumble",SQLSTATE(3F000) "Could not find the basket column storage %s.%s[%d]\n",
															sch,tbl,i);
		}

		_DEBUG_BASKET_ fprintf(stderr,"#Tumbled %s.%s[%d] "BUNFMT" elements left\n",sch,tbl,i,cnt);
		BATsetcount(b, cnt);
		if (BUNremoveproperties(b) != GDK_SUCCEED)
			throw(SQL,"basket.tumble",SQLSTATE(3F000) "Failed to remove GDK properties on stream table %s.%s\n",sch,tbl);
		baskets[bskt].count = BATcount(b);
		b->tnil = 0;
		if((BUN) stride < BATcount(b)){
			b->tnokey[0] -= stride;
			b->tnokey[1] -= stride;
			b->tnosorted = 0;
			b->tnorevsorted = 0;
		} else {
			b->tnokey[0] = 0;
			b->tnokey[1] = 0;
			b->tnosorted = 0;
			b->tnorevsorted = 0;
		}
		BATsettrivprop(b);
	}
	return MAL_SUCCEED;
}

/* set the tumbling properties */

str
BSKTtumble(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch, tbl, msg;
	int idx, elw, elm = STRIDE_ALL;

	(void) cntxt;
	(void) mb;

	sch = *getArgReference_str(stk,pci,1);
	tbl = *getArgReference_str(stk,pci,2);

	msg = BSKTregisterInternal(cntxt, mb, sch, tbl, &idx);
	if( msg != MAL_SUCCEED)
		return msg;
	// don't tumble when the window constraint has not been set to at least 0 or we are not querying from a CQ
	if( baskets[idx].window < 0 || !cntxt->iscqscheduleruser)
		return MAL_SUCCEED;
	/* also take care of time-based tumbling */
	elw = baskets[idx].window;
	elm = baskets[idx].stride;
	return BSKTtumbleInternal(cntxt, sch, tbl, idx, elw, elm);
}

// A no-op to handle an SQL COMMIT statement involving a stream table.
str
BSKTcommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,2);
	str tbl = *getArgReference_str(stk,pci,3);
	int idx;

	(void) cntxt;
	(void) mb;

	idx = BSKTlocate(sch, tbl);
	if( idx ==0)
		throw(SQL,"basket.commit",SQLSTATE(3F000) "Stream table %s.%s not accessible\n",sch,tbl);
	return MAL_SUCCEED;
}

str
BSKTlock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,2);
	str tbl = *getArgReference_str(stk,pci,3);
	int idx;

	(void) cntxt;
	(void) mb;

	idx = BSKTlocate(sch, tbl);
	if( idx ==0)
		throw(SQL,"basket.lock",SQLSTATE(3F000) "Stream table %s.%s not accessible\n",sch,tbl);
	/* set the basket lock */
	MT_lock_set(&baskets[idx].lock);
	return MAL_SUCCEED;
}

str
BSKTunlock(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,2);
	str tbl = *getArgReference_str(stk,pci,3);
	int idx;
	BAT *b;

	(void) cntxt;
	(void) mb;

	idx = BSKTlocate(sch, tbl);
	if( idx ==0)
		throw(SQL,"basket.lock",SQLSTATE(3F000) "Stream table %s.%s not accessible\n",sch,tbl);

	/* this is also the place to administer the size of the basket,
	 *   i.e. set it to the count of remaining tuples for the next query on
	 *   this stream table.
	 */
	b = BSKTbindColumn(sch,tbl, baskets[idx].cols[0]->base.name);
	if( b)
		baskets[idx].count = BATcount(b);
	/* release the basket lock */
	MT_lock_unset(&baskets[idx].lock);
	return MAL_SUCCEED;
}

str
BSKTdump(void *ret)
{
	int bskt;
	BUN cnt;
	BAT *b;
	str msg = MAL_SUCCEED;

	fprintf(stderr, "#baskets table\n");
	for (bskt = 1; bskt < bsktLimit; bskt++)
		if (baskets[bskt].table) {
			cnt = 0;
			b = baskets[bskt].bats[0];
			if( b)
				cnt = BATcount(b);

			fprintf(stderr, "#baskets[%2d] %s.%s columns "BUNFMT
					" window=%d stride=%d error=%s fill=%zu\n",
					bskt,
					baskets[bskt].table->s->base.name,
					baskets[bskt].table->base.name,
					baskets[bskt].count,
					baskets[bskt].window,
					baskets[bskt].stride,
					baskets[bskt].error,
					cnt);
		}

	(void) ret;
	return msg;
}

str
BSKTappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	str sname = *getArgReference_str(stk, pci, 2);
	str tname = *getArgReference_str(stk, pci, 3);
	str cname = *getArgReference_str(stk, pci, 4);
	ptr value = getArgReference(stk, pci, 5);
	int tpe = getArgType(mb, pci, 5);
	BAT *bn=0, *binsert = 0;
	int bskt;

	(void) cntxt;
	*res = 0;

	if ( isaBatType(tpe) && (binsert = BATdescriptor(*(int *) value)) == NULL)
		throw(SQL, "basket.append",SQLSTATE(HY005) "Cannot access source descriptor\n");
	if ( !isaBatType(tpe) && ATOMextern(getBatType(tpe)))
		value = *(ptr*) value;

	bskt = BSKTlocate(sname,tname);
	if( bskt == 0) {
		if (binsert) 
			BBPunfix(binsert->batCacheid);
		throw(SQL, "basket.append",SQLSTATE(HY005) "Cannot access basket descriptor %s.%s\n",sname,tname);
	}
	bn = BSKTbindColumn(sname,tname,cname);

	if( bn){
		if (binsert){
			if( BATappend(bn, binsert, NULL, TRUE) != GDK_SUCCEED) {
				BBPunfix(binsert->batCacheid);
				throw(MAL,"basket.append",SQLSTATE(HY005) "insertion failed\n");
			}
		} else
			if( BUNappend(bn, value, TRUE) != GDK_SUCCEED)
				throw(MAL,"basket.append",SQLSTATE(HY005) "insertion failed\n");
		BATsettrivprop(bn);
	} else {
		if (binsert)
			BBPunfix(binsert->batCacheid);
		throw(SQL, "basket.append",SQLSTATE(3F000) "Cannot access target column %s.%s.%s\n",sname,tname,cname);
	}
	
	if (binsert )
		BBPunfix(binsert->batCacheid);
	return MAL_SUCCEED;
}

str
BSKTupdate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	str sname = *getArgReference_str(stk, pci, 2);
	str tname = *getArgReference_str(stk, pci, 3);
	str cname = *getArgReference_str(stk, pci, 4);
	bat rows = *getArgReference_bat(stk, pci, 5);
	bat val = *getArgReference_bat(stk, pci, 6);
	BAT *bn=0, *rid=0, *bval = 0;
	int bskt;

	(void) cntxt;
	(void) mb;
	*res = 0;

	rid = BATdescriptor(rows);
	if( rid == NULL)
		throw(SQL, "basket.update",SQLSTATE(HY005) "Cannot access source oid descriptor\n");
	bval = BATdescriptor(val);
	if( bval == NULL){
		BBPunfix(rid->batCacheid);
		throw(SQL, "basket.update",SQLSTATE(HY005) "Cannot access source descriptor\n");
	}

	bskt = BSKTlocate(sname,tname);
	if( bskt == 0) {
		BBPunfix(rid->batCacheid);
		BBPunfix(bval->batCacheid);
		throw(SQL, "basket.update",SQLSTATE(HY005) "Cannot access basket descriptor %s.%s\n",sname,tname);
	}
	bn = BSKTbindColumn(sname,tname,cname);

	if( bn){
		if( BATreplace(bn, rid, bval, TRUE) != GDK_SUCCEED) {
			BBPunfix(rid->batCacheid);
			BBPunfix(bval->batCacheid);
			throw(SQL, "basket.update",SQLSTATE(HY005) "Cannot access basket descriptor %s.%s\n",sname,tname);
		}
		BATsettrivprop(bn);
	} else {
		BBPunfix(rid->batCacheid);
		BBPunfix(bval->batCacheid);
		throw(SQL, "basket.update",SQLSTATE(3F000) "Cannot access target column %s.%s.%s\n",sname,tname,cname);
	}
	
	BBPunfix(rid->batCacheid);
	BBPunfix(bval->batCacheid);
	return MAL_SUCCEED;
}

str
BSKTdelete(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	str sname = *getArgReference_str(stk, pci, 2);
	str tname = *getArgReference_str(stk, pci, 3);
	bat rows = *getArgReference_bat(stk, pci, 4);
	BAT *b=0, *rid=0;
	int i,idx;

	(void) cntxt;
	(void) mb;
	*res = 0;

	rid = BATdescriptor(rows);
	if( rid == NULL)
		throw(SQL, "basket.delete",SQLSTATE(3F000) "Cannot access source oid descriptor\n");

	idx = BSKTlocate(sname,tname);
	if( idx == 0) {
		BBPunfix(rid->batCacheid);
		throw(SQL, "basket.delete",SQLSTATE(3F000) "Cannot access basket descriptor %s.%s\n",sname,tname);
	}
	for( i=0; i < baskets[idx].ncols; i++){
		b = baskets[idx].bats[i];
		if(b){
			 if( BATdel(b, rid) != GDK_SUCCEED){
				BBPunfix(rid->batCacheid);
				throw(SQL, "basket.delete", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			baskets[idx].count = BATcount(b);
			b->tnil = 0;
			b->tnosorted = 0;
			b->tnorevsorted = 0;
			b->tnokey[0] = 0;
			b->tnokey[1] = 0;
			BATsettrivprop(b);
		}
	}
	BBPunfix(rid->batCacheid);
	*res = *getArgReference_int(stk,pci,1);
	return MAL_SUCCEED;
}

// Reset a busket so that it can be used anew
str
BSKTreset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *res = getArgReference_lng(stk, pci, 0);
	str sname = *getArgReference_str(stk, pci, 2);
	str tname = *getArgReference_str(stk, pci, 3);
	int i, idx;
	BAT *b;
	(void) cntxt;
	(void) mb;

	*res = 0;
	idx = BSKTlocate(sname,tname);
	if( idx == 0)
		throw(SQL,"basket.reset",SQLSTATE(3F000) "Stream table %s.%s not registered\n",sname,tname);
	// FIXME: should we really set and unset the backset lock here?
	// do actual work
	MT_lock_set(&baskets[idx].lock);
	for( i=0; i < baskets[idx].ncols; i++){
		b = baskets[idx].bats[i];
		if(b){
			BATsetcount(b,0);
			BATsettrivprop(b);
			if (BUNremoveproperties(b) != GDK_SUCCEED) {
				MT_lock_unset(&baskets[idx].lock);
				throw(SQL,"basket.reset",SQLSTATE(3F000) "Failed to remove GDK properties on stream table %s.%s\n",sname,tname);
			}
		}
	}
	MT_lock_unset(&baskets[idx].lock);
	return MAL_SUCCEED;
}

str
BSKTerror(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sname = *getArgReference_str(stk, pci, 1);
	str tname = *getArgReference_str(stk, pci, 2);
	str error = *getArgReference_str(stk, pci, 3);
	int idx;

	(void) cntxt;
	(void) mb;

	idx = BSKTlocate(sname,tname);
	if( idx == 0)
		throw(SQL,"basket.error",SQLSTATE(3F000) "Stream table %s.%s not registered\n",sname,tname);

	if(error) {
		baskets[idx].error = GDKstrdup(error);
		if(baskets[idx].error == NULL)
			throw(SQL,"basket.error",SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

/* provide a tabular view for inspection */
str
BSKTstatus (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *seenId = getArgReference_bat(stk,pci,0);
	bat *schemaId = getArgReference_bat(stk,pci,1);
	bat *tableId = getArgReference_bat(stk,pci,2);
	bat *windowId = getArgReference_bat(stk,pci,3);
	bat *strideId = getArgReference_bat(stk,pci,4);
	bat *eventsId = getArgReference_bat(stk,pci,5);
	bat *errorId = getArgReference_bat(stk,pci,6);

	BAT *seen = NULL, *schema = NULL, *table = NULL, *window = NULL;
	BAT *stride = NULL, *events = NULL, *errors = NULL;
	int i;
	BAT *bn = NULL;

	(void) mb;
	(void) cntxt;

	seen = COLnew(0, TYPE_timestamp, BATTINY, TRANSIENT);
	if (seen == 0)
		goto wrapup;
	schema = COLnew(0, TYPE_str, BATTINY, TRANSIENT);
	if (schema == 0)
		goto wrapup;
	table = COLnew(0, TYPE_str, BATTINY, TRANSIENT);
	if (table == 0)
		goto wrapup;
	window = COLnew(0, TYPE_int, BATTINY, TRANSIENT);
	if (window == 0)
		goto wrapup;
	stride = COLnew(0, TYPE_int, BATTINY, TRANSIENT);
	if (stride == 0)
		goto wrapup;
	events = COLnew(0, TYPE_int, BATTINY, TRANSIENT);
	if (events == 0)
		goto wrapup;
	errors = COLnew(0, TYPE_str, BATTINY, TRANSIENT);
	if (errors == 0)
		goto wrapup;

	for (i = 1; i < bsktTop; i++)
		if (baskets[i].table) {
			bn = BSKTbindColumn(baskets[i].table->s->base.name, baskets[i].table->base.name, baskets[i].cols[0]->base.name);
			baskets[i].events = bn ? BATcount( bn): 0;
			if( BUNappend(seen, &baskets[i].seen, FALSE) != GDK_SUCCEED ||
				BUNappend(schema, baskets[i].table->s->base.name, FALSE) != GDK_SUCCEED ||
				BUNappend(table, baskets[i].table->base.name, FALSE) != GDK_SUCCEED ||
				BUNappend(window, &baskets[i].window, FALSE) != GDK_SUCCEED ||
				BUNappend(stride, &baskets[i].stride, FALSE) != GDK_SUCCEED ||
				BUNappend(events, &baskets[i].events, FALSE) != GDK_SUCCEED ||
				BUNappend(errors, (baskets[i].error? baskets[i].error:""), FALSE) != GDK_SUCCEED )
				goto wrapup;
		}

	BBPkeepref(*seenId = seen->batCacheid);
	BBPkeepref(*schemaId = schema->batCacheid);
	BBPkeepref(*tableId = table->batCacheid);
	BBPkeepref(*windowId = window->batCacheid);
	BBPkeepref(*strideId = stride->batCacheid);
	BBPkeepref(*eventsId = events->batCacheid);
	BBPkeepref(*errorId = errors->batCacheid);
	return MAL_SUCCEED;
wrapup:
	if (seen)
		BBPunfix(seen->batCacheid);
	if (schema)
		BBPunfix(schema->batCacheid);
	if (table)
		BBPunfix(table->batCacheid);
	if (window)
		BBPunfix(window->batCacheid);
	if (stride)
		BBPunfix(stride->batCacheid);
	if (errors)
		BBPunfix(errors->batCacheid);
	if (events)
		BBPunfix(events->batCacheid);
	throw(SQL, "basket.status", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

void
BSKTshutdown(void)
{
	int i;
	if(baskets) {
		for(i = 1 ; i < bsktTop ; i++) {
			BSKTclean(i);
		}
		GDKfree(baskets);
		baskets = NULL;
	}
	bsktLimit = INTIAL_BSKT;
	bsktTop = 1;
}

str
BSKTprelude(void *ret)
{
	(void) ret;
	baskets = (BasketRec *) GDKzalloc(INTIAL_BSKT * sizeof(BasketRec));
	bsktLimit = INTIAL_BSKT;
	bsktTop = 1;
	if( baskets == NULL)
		throw(MAL, "basket.prelude", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}
