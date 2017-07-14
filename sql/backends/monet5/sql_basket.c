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
#include <unistd.h>
#include "gdk.h"
#include "sql_basket.h"
#include "mal_exception.h"
#include "mal_builder.h"
#include "opt_prelude.h"

#define _DEBUG_BASKET_ if(0)

BasketRec *baskets;   /* the global timetrails catalog */
int bsktTop = 0, bsktLimit = 0;

// locate the basket in the basket catalog
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

// Find an empty slot in the basket catalog
static int BSKTnewEntry(void)
{
	int i = bsktTop;
	BasketRec *bnew;

	if (bsktLimit == 0) {
		bsktLimit = MAXBSKT;
		baskets = (BasketRec *) GDKzalloc(bsktLimit * sizeof(BasketRec));
		if( baskets == 0)	
			return 0;
		bsktTop = 1; /* entry 0 is used as non-initialized */
	} else if (bsktTop + 1 == bsktLimit) {
		bnew = (BasketRec *) GDKrealloc(baskets, (bsktLimit+MAXBSKT) * sizeof(BasketRec));
		if( bnew == 0)
			return 0;
		bsktLimit += MAXBSKT;
		baskets = bnew;
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

	if( idx){
		GDKfree(baskets[idx].schema);
		GDKfree(baskets[idx].table);
		GDKfree(baskets[idx].error);
		baskets[idx].schema = NULL;
		baskets[idx].table = NULL;
		baskets[idx].error = NULL;
		baskets[idx].count = 0;
		baskets[idx].events = 0;
		baskets[idx].cycles = 0;
		baskets[idx].seen =  *timestamp_nil;
		for(i=0; baskets[idx].bats[i]; i++){
			BBPunfix(baskets[idx].bats[i]->batCacheid);
			baskets[idx].bats[i] =0;
		}
	}
}

// Instantiate a basket description for a particular stream table
static str
BSKTnewbasket(mvc *m, sql_schema *s, sql_table *t)
{
	int i, idx, colcnt=0;
	BAT *b;
	node *o;

	// Don't introduce the same basket twice
	if( BSKTlocate(s->base.name, t->base.name) > 0)
		return MAL_SUCCEED;

	if( !isStream(t))
		throw(MAL,"basket.register","Only allowed for stream tables");

	idx = BSKTnewEntry();

	baskets[idx].schema = GDKstrdup(s->base.name);
	baskets[idx].table = GDKstrdup(t->base.name);
	(void) MTIMEcurrent_timestamp(&baskets[idx].seen);

	// Check the column types first
	for (o = t->columns.set->h; o && colcnt <MAXCOLS-1; o = o->next){
        sql_column *col = o->data;
        int tpe = col->type.type->localtype;

        if ( !(tpe <= TYPE_str || tpe == TYPE_date || tpe == TYPE_daytime || tpe == TYPE_timestamp) )
			throw(MAL,"basket.register","Unsupported type %d\n",tpe);
		colcnt++;
	}
	if( colcnt == MAXCOLS-1){
		BSKTclean(idx);
		throw(MAL,"baskets.register","Too many columns\n");
	}

	// collect the column names and the storage
	for ( i=0, o = t->columns.set->h; i <colcnt && o; o = o->next){
        sql_column *col = o->data;
		b = store_funcs.bind_col(m->session->tr,col,RD_INS);
		assert(b);
		BBPfix(b->batCacheid);
		baskets[idx].bats[i]= b;
		baskets[idx].cols[i++]=  GDKstrdup(col->base.name);
	}
	return MAL_SUCCEED;
}

// MAL/SQL interface for registration of a single table
str
BSKTregisterInternal(Client cntxt, MalBlkPtr mb, str sch, str tbl)
{
	sql_schema  *s;
	sql_table   *t;
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);

	if ( msg != MAL_SUCCEED)
		return msg;

	/* check double registration */
	if( BSKTlocate(sch, tbl) > 0)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	s = mvc_bind_schema(m, sch);
	if (s == NULL)
		throw(SQL, "basket.register", "Schema missing\n");

	t = mvc_bind_table(m, s, tbl);
	if (t == NULL)
		throw(SQL, "basket.register", "Table missing '%s'\n", tbl);

	msg=  BSKTnewbasket(m, s, t);
	return msg;
}

str
BSKTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    str sch, tbl;
    str msg= MAL_SUCCEED;

    (void) stk;
    (void) pci;
    sch = getVarConstant(mb, getArg(pci,2)).val.sval;
    tbl = getVarConstant(mb, getArg(pci,3)).val.sval;
    msg = BSKTregisterInternal(cntxt,mb,sch,tbl);
    return msg;
}

str
BSKTwindow(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,1);
	str tbl = *getArgReference_str(stk,pci,2);
	int window = *getArgReference_int(stk,pci,3);
	int stride;
	int idx;
	str msg;

	(void) cntxt;
	(void) mb;
	idx = BSKTlocate(sch, tbl);
	if( idx == 0){
		msg= BSKTregisterInternal(cntxt, mb, sch, tbl);
		if( msg != MAL_SUCCEED)
			return msg;
		idx = BSKTlocate(sch, tbl);
		if( idx ==0)
			throw(SQL,"basket.window","Stream table %s.%s not accessible\n",sch,tbl);
	}
	if( pci->argc == 5)
		stride = *getArgReference_int(stk,pci,4);
	else stride = window;
	baskets[idx].window = window;
	baskets[idx].stride = stride;
	return MAL_SUCCEED;
}

str
BSKTkeep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch = *getArgReference_str(stk,pci,1);
	str tbl = *getArgReference_str(stk,pci,2);
	int idx;
	str msg;

	(void) cntxt;
	(void) mb;
	idx = BSKTlocate(sch, tbl);
	if( idx == 0){
		msg= BSKTregisterInternal(cntxt, mb, sch, tbl);
		if( msg != MAL_SUCCEED)
			return msg;
		idx = BSKTlocate(sch, tbl);
		if( idx ==0)
			throw(SQL,"basket.window","Stream table %s.%s not accessible\n",sch,tbl);
	}
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
	idx = BSKTlocate(sch, tbl);
	if( idx == 0){
		msg= BSKTregisterInternal(cntxt, mb, sch, tbl);
		if( msg != MAL_SUCCEED)
			return msg;
		idx = BSKTlocate(sch, tbl);
		if( idx ==0)
			throw(SQL,"basket.window","Stream table %s.%s not accessible\n",sch,tbl);
	}
	if( baskets[idx].window < 0)
		baskets[idx].window = - baskets[idx].window -1;
	return MAL_SUCCEED;
}

static BAT *
BSKTbindColumn(str sch, str tbl, str col)
{
	int idx =0,i;

	if( (idx = BSKTlocate(sch,tbl)) < 0)
		return NULL;

	for( i=0; i < MAXCOLS && baskets[idx].cols[i]; i++)
		if( strcmp(baskets[idx].cols[i], col)== 0)
			break;
	if(  i < MAXCOLS)
		return baskets[idx].bats[i];
	return NULL;
}

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
		throw(SQL,"basket.bind","Stream table column '%s.%s' not found\n",sch,tbl);
	b = baskets[bskt].bats[0];
	if( b == 0)
		throw(SQL,"basket.bind","Stream table reference column '%s.%s' not accessible\n",sch,tbl);

    tids = COLnew(0, TYPE_void, 0, TRANSIENT);
    if (tids == NULL)
        throw(SQL, "basket.tid", MAL_MALLOC_FAIL);
	tids->tseqbase = 0;
    BATsetcount(tids, BATcount(b));
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
	msg = BSKTregisterInternal(cntxt,mb,sch,tbl);
	if( msg)
		return msg;
	
	bskt = BSKTlocate(sch,tbl);
	b = BSKTbindColumn(sch,tbl,col);
	*ret = 0;
	if( b){
		if( bskt > 0){
			if( baskets[bskt].window >0){
				bn = VIEWcreate(0,b);
				if( bn){
					VIEWbounds(b,bn, 0, baskets[bskt].window);
					BBPkeepref(*ret =  bn->batCacheid);
				} else
					throw(SQL,"basket.bind","Can not create view %s.%s.%s[%d]\n",sch,tbl,col,baskets[bskt].window );
			} else{
				BBPkeepref( *ret = b->batCacheid);
				BBPfix(b->batCacheid); // don't loose it
			}
		}
		return MAL_SUCCEED;
	}
	throw(SQL,"basket.bind","Stream table column '%s.%s.%s' not found\n",sch,tbl,col);
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
		throw(SQL, "basket.drop", "Could not find the basket %s.%s\n",sch,tbl);
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
BSKTtumbleInternal(Client cntxt, str sch, str tbl, int bskt, int stride)
{
	BAT *b;
	BUN cnt= 0 ;
	int i;
	(void) cntxt;

	if( stride < 0)
		throw(MAL,"basket.tumble","negative stride not allowed");
	_DEBUG_BASKET_ fprintf(stderr,"Tumble %s.%s %d elements\n",sch,tbl,stride);
	if( stride == 0)
		return MAL_SUCCEED;
	for(i=0; i< MAXCOLS && baskets[bskt].cols[i]; i++){
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
			throw(SQL, "basket.tumble", "Could not find the basket column storage %s.%s[%d]",sch,tbl,i);
		}

		_DEBUG_BASKET_ fprintf(stderr,"#Tumbled %s.%s[%d] "BUNFMT" elements left\n",sch,tbl,i,cnt);
		BATsetcount(b, cnt);
		baskets[bskt].count = BATcount(b);
		b->tnil = 0;
		if( (BUN) stride < BATcount(b)){ b->tnokey[0] -= stride;
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
	str sch;
	str tbl;
	int elm = -1;
	int idx;
	str msg;

	(void) cntxt;
	(void) mb;

	sch = *getArgReference_str(stk,pci,2);
	tbl = *getArgReference_str(stk,pci,3);

	idx = BSKTlocate(sch, tbl);
	if( idx == 0){
		msg = BSKTregisterInternal(cntxt, mb, sch, tbl);
		if( msg != MAL_SUCCEED)
			return msg;
		idx = BSKTlocate(sch, tbl);
		if( idx ==0)
			throw(SQL,"basket.tumble","Stream table %s.%s not accessible \n",sch,tbl);
	}
	// don't tumble when the window constraint has not been set to at least 0
	if( baskets[idx].window < 0)
		return MAL_SUCCEED;
	/* also take care of time-based tumbling */
	elm =(int) baskets[idx].stride;
	return BSKTtumbleInternal(cntxt, sch, tbl, idx, elm);
}

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
		throw(SQL,"basket.commit","Stream table %s.%s not accessible\n",sch,tbl);
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
		throw(SQL,"basket.lock","Stream table %s.%s not accessible\n",sch,tbl);
	/* release the basket lock */
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
		throw(SQL,"basket.lock","Stream table %s.%s not accessible\n",sch,tbl);
	/* this is also the place to administer the size of the basket */
    b = BSKTbindColumn(sch,tbl, baskets[idx].cols[0]);
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

	mnstr_printf(GDKout, "#baskets table\n");
	for (bskt = 1; bskt < bsktLimit; bskt++)
		if (baskets[bskt].table) {
			cnt = 0;
			b = baskets[bskt].bats[0];
			if( b)
				cnt = BATcount(b);

			fprintf(stderr, "#baskets[%2d] %s.%s columns "BUNFMT
					" window=%d stride=%d error=%s fill="SZFMT"\n",
					bskt,
					baskets[bskt].schema,
					baskets[bskt].table,
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
        throw(SQL, "basket.append", "Cannot access source descriptor");
	if ( !isaBatType(tpe) && ATOMextern(getBatType(tpe)))
		value = *(ptr*) value;

	bskt = BSKTlocate(sname,tname);
	if( bskt == 0)
		throw(SQL, "basket.append", "Cannot access basket descriptor %s.%s",sname,tname);
	bn = BSKTbindColumn(sname,tname,cname);

	if( bn){
		if (binsert){
			if( BATappend(bn, binsert, NULL, TRUE) != GDK_SUCCEED)
				throw(MAL,"basket.append","insertion failed\n");
		} else
			if( BUNappend(bn, value, TRUE) != GDK_SUCCEED)
				throw(MAL,"basket.append","insertion failed\n");
		BATsettrivprop(bn);
	} else throw(SQL, "basket.append", "Cannot access target column %s.%s.%s",sname,tname,cname);
	
	if (binsert )
		BBPunfix(((BAT *) binsert)->batCacheid);
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
        throw(SQL, "basket.update", "Cannot access source oid descriptor");
    bval = BATdescriptor(val);
	if( bval == NULL){
		BBPunfix(rid->batCacheid);
        throw(SQL, "basket.update", "Cannot access source descriptor");
	}

	bskt = BSKTlocate(sname,tname);
	if( bskt == 0)
		throw(SQL, "basket.update", "Cannot access basket descriptor %s.%s",sname,tname);
	bn = BSKTbindColumn(sname,tname,cname);

	if( bn){
		if( void_replace_bat(bn, rid, bval, TRUE) != GDK_SUCCEED)
			throw(SQL, "basket.update", "Cannot access basket descriptor %s.%s",sname,tname);
		
		BATsettrivprop(bn);
	} else throw(SQL, "basket.append", "Cannot access target column %s.%s.%s",sname,tname,cname);
	
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
        throw(SQL, "basket.delete", "Cannot access source oid descriptor");

	idx = BSKTlocate(sname,tname);
	if( idx == 0)
		throw(SQL, "basket.delete", "Cannot access basket descriptor %s.%s",sname,tname);
	for( i=0; baskets[idx].cols[i]; i++){
		b = baskets[idx].bats[i];
		if(b){
			 if( BATdel(b, rid) != GDK_SUCCEED){
				BBPunfix(rid->batCacheid);
				throw(SQL, "basket.delete", MAL_MALLOC_FAIL);
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
	if( idx <= 0)
		throw(SQL,"basket.clear","Stream table %s.%s not registered \n",sname,tname);
	// do actual work
	MT_lock_set(&baskets[idx].lock);
	for( i=0; baskets[idx].cols[i]; i++){
		b = baskets[idx].bats[i];
		if(b){
			BATsetcount(b,0);
			BATsettrivprop(b);
		}
	}
	MT_lock_unset(&baskets[idx].lock);
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
	bat *cyclesId = getArgReference_bat(stk,pci,6);
	bat *errorId = getArgReference_bat(stk,pci,7);

	BAT *seen = NULL, *schema = NULL, *table = NULL, *window = NULL;
	BAT *stride = NULL, *events = NULL, *cycles = NULL, *errors = NULL;
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
	cycles = COLnew(0, TYPE_int, BATTINY, TRANSIENT);
	if (cycles == 0)
		goto wrapup;
	errors = COLnew(0, TYPE_str, BATTINY, TRANSIENT);
	if (errors == 0)
		goto wrapup;

	for (i = 1; i < bsktTop; i++)
		if (baskets[i].table) {
			bn = BSKTbindColumn(baskets[i].schema, baskets[i].table, baskets[i].cols[0]);
			baskets[i].events = bn ? BATcount( bn): 0;
			if( BUNappend(seen, &baskets[i].seen, FALSE) != GDK_SUCCEED ||
				BUNappend(schema, baskets[i].schema, FALSE) != GDK_SUCCEED ||
				BUNappend(table, baskets[i].table, FALSE) != GDK_SUCCEED ||
				BUNappend(window, &baskets[i].window, FALSE) != GDK_SUCCEED ||
				BUNappend(stride, &baskets[i].stride, FALSE) != GDK_SUCCEED ||
				BUNappend(events, &baskets[i].events, FALSE) != GDK_SUCCEED ||
				BUNappend(cycles, &baskets[i].cycles, FALSE) != GDK_SUCCEED  ||
				BUNappend(errors, (baskets[i].error? baskets[i].error:""), FALSE) != GDK_SUCCEED )
				goto wrapup;
		}

	BBPkeepref(*seenId = seen->batCacheid);
	BBPkeepref(*schemaId = schema->batCacheid);
	BBPkeepref(*tableId = table->batCacheid);
	BBPkeepref(*windowId = window->batCacheid);
	BBPkeepref(*strideId = stride->batCacheid);
	BBPkeepref(*cyclesId = cycles->batCacheid);
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
	if (cycles)
		BBPunfix(cycles->batCacheid);
	if (events)
		BBPunfix(events->batCacheid);
	throw(SQL, "basket.status", MAL_MALLOC_FAIL);
}
