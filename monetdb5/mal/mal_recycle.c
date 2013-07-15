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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) M. Ivanova, M. Kersten, N. Nes
 * mal_recycle
 * Query optimization and processing in off-the-shelf database systems is often
 * still focussed on individual queries. The queries are analysed in isolation
 * and ran against a kernel regardless opportunities offered by concurrent or
 * previous invocations.
 *
 * This approach is far from optimal and two directions to improve
 * are explored: materialized views and (partial) result-set reuse.
 * Materialized views are derived from query logs. They represent
 * common sub-queries, whose materialization improves
 * subsequent processing times.
 * Re-use of (partial) results is used in those cases where a
 * zooming-in or navigational application is at stake.
 *
 * The Recycler module extends it with a middle out approach.
 * It exploits the materialize-all-intermediate approach of MonetDB
 * by deciding to keep a hold on them as long as deemed benificial.
 *
 * The approach taken is to mark the instructions in a MAL program
 * using an optimizer call, such that their result is retained
 * in a global recycle cache. A reference into the cache makes
 * is used to access the latest known version quickly.
 *
 * Upon execution, the Recycler first checks for
 * an up to date result to be picked up at no cost,
 * other than matching the arguments.
 * Otherwise, it evaluates the instruction and calls upon
 * policy functions to decide if it is worthwhile to
 * keep.
 *
 * The Recycler comes with a few policy controlling operators
 * to experiment with its effect in concrete settings.
 *
 * Caveats:
 * Updates in general should immediately invalidate the cache lines depending on the updated Bats. These are all instructions directly operating on the updates, as well as all instructions consuming the result of the first.
 * 
 * Unlike the background publication, we keep a single focus for the released functionality
 * ADM_ALL: infinite case, admission of all instructions subject to cache limits
 * REUSE_COVER: exploit potential range overlap
 * RCACHE_PROFIT: use semantic driven LRU§
 */
#include "monetdb_config.h"
#include "mal_recycle.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_runtime.h"

static MT_Lock recycleLock MT_LOCK_INITIALIZER("recycleLock");
MalBlkPtr recycleBlk = NULL;

#define set1(x,i) ( x | ((lng)1 << i) )
#define set0(x,i) ( x & ~((lng)1 << i) )
#define getbit(x,i) ( x & ((lng)1 << i) )
#define neg(x) ( (x)?FALSE:TRUE)


/* ADM_ALL: infinite case, admission of all instructions subject to cache limits*/

lng recycleSearchTime = 0;	/* cache search time in ms*/
int recycleMaxInterest = REC_MAX_INTEREST;

/* REUSE_COVER: exploit potential range overlap */
int reusePolicy = REUSE_COVER;	

/*	evict items with smallest profit= weight * cost / lifetime adds aging to the benefit policy */

int recycleCacheLimit=0; /* No limit by default */


/*
 * Monitoring the Recycler
 */
lng recyclerMemoryUsed = 0;
int monitorRecycler = 0;
	/*	 1: print statistics for RecyclerPool only
		 2: print stat at the end of each query */

static lng recycled=0;

#ifdef _DEBUG_CACHE_
#define recycleSize recycleBlk->stop - cntxt->rcc->recycleRem
#else
#define recycleSize recycleBlk->stop
#endif

/*
 * The profiler record is re-used to store recycler information.
 * The clk is used by the LRU scheme, counter is the number of
 * times this pattern was used, ticks is the clock ticks
 * used to produce the result. rbytes+wbytes depict the storage
 * size of operands and result arguments.
 *
 * The cost function is a weighted balance between cpu and
 * storage cost. Often there is a direct relationship,
 */

#define recycleCost(X) (recycleBlk->profiler[X].wbytes)
#define recycleW(X)  ((recycleBlk->profiler[X].trace && (recycleBlk->profiler[X].calls >1 )) ? \
						(recycleBlk->profiler[X].calls -1) : 0.1 )

#define recycleLife(X) ((GDKusec() - recycleBlk->profiler[X].rbytes)/ 1000.0)
#define recycleProfit(X) (recycleCost(X) * recycleW(X) / recycleLife(X))

static str bindRef = 0, bind_idxRef = 0, sqlRef = 0;
static str subselectRef = 0, thetasubselectRef = 0, likesubselectRef = 0;
static void RECYCLEexitImpl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof);
/*
 * The Recycle catalog is a global structure, which should be
 * protected with locks when updated.
 * The recycle statistics can be kept in the performance table
 * associated with the recycle MAL block without problems, because
 * the block is never executed.
 */
static void RECYCLEspace(void)
{
	if (recycleBlk == NULL) {
		recycleBlk = newMalBlk(MAXVARS, STMT_INCREMENT);
		recycleBlk->profiler = (ProfPtr) GDKzalloc(
			recycleBlk->ssize*sizeof(ProfRecord));
	}
}

void RECYCLEinit(void){
#ifdef NEED_MT_LOCK_INIT
	MT_lock_init(&recycleLock,"recycleLock");
#endif
	sqlRef = putName("sql",3);
	bindRef = putName("bind",4);
	bind_idxRef = putName("bind_idxbat",11);
	subselectRef = putName("subselect",9);
	thetasubselectRef = putName("thetasubselect",14);
	likesubselectRef= putName("likesubselect",14);
	recycleCacheLimit=HARDLIMIT_STMT;
}

/*
 * The cache of recycled instructions can be kept low.
 * Once the cache is filled, we have to drop instructions
 * and associated variables. At the same time we should
 * invalidate the cache version, such that others can
 * re-synchronize.
 *
 * For the recycle cache LRU scheme we mis-use a field in
 * the performance record.
 *
 * Removal of instructions should be done with care.
 * First, there may be references kept around to
 * variables in the recycle cache. They should be
 * invalidated.
 * Second, for each (BAT) variable removed, we
 * should also remove the dependent instructions.
 */
static void
RECYCLEgarbagecollect(MalBlkPtr mb, InstrPtr q, bte *used){
	int j;
	ValPtr v;

	for(j=0; j< q->argc; j++){
		v= &getVarConstant(mb,getArg(q,j));
		if(getArgType(mb,q,j)==TYPE_bat || isaBatType(getArgType(mb, q,j)) ){
			if( v->val.bval ){
				BBPdecref(ABS(v->val.bval), TRUE);
				if (!BBP_lrefs(v->val.bval)){
					v->vtype= TYPE_int;
					v->val.ival= 0;
				}
			}
		}
		if( v->vtype == TYPE_str && used[getArg(q,j)]<=1) {
			if(v->val.sval) {
				GDKfree(v->val.sval);
				v->val.sval = NULL;
			}
			v->len = 0;
		}
	}
}

/* lvs : array of indices of leaf instructions */

static
int chooseVictims(Client cntxt, int *lvs, int ltop, lng wr)
{
	dbl *wben, ben = 0, maxwb, tmp, ci_ben, tot_ben;
	int l, newtop, mpos, tmpl, ci = 0;
	lng sz, totmem = 0, targmem, smem;

	(void) cntxt;

	wben = (dbl *)GDKzalloc(sizeof(dbl)*ltop);
	for (l = 0; l < ltop; l++){
		sz = recycleBlk->profiler[lvs[l]].wbytes;
		ben = recycleProfit(lvs[l]);
		wben[l] = sz? ben / sz : -1;
		totmem += sz;
	}
	if (totmem <= wr) { /* all leaves need to be dropped */
		GDKfree(wben);
		return ltop;
	}

	/* reorder instructions on increasing weighted credit */
	/* knapsack: find a set with biggest wben fitting in totmem-wr.
	 They are most benefitial and can be saved from dropping */

	targmem = totmem - wr; /*sack volume */
	smem = 0;
	tot_ben = 0;
	for(newtop = ltop; newtop>0; newtop--){
		maxwb = 0; mpos = newtop-1;		/* find most benefitial item (weighted ben.)*/
		for(l = 0; l<newtop; l++){
			if ((lng) recycleBlk->profiler[lvs[l]].wbytes > targmem - smem)
				wben[l] = -1;
			if (maxwb < wben[l]){
				maxwb = wben[l];
				mpos = l;
			}
		}
		if ( maxwb ){			   /* add it to the sack (newtop - ltop)*/
			smem += recycleBlk->profiler[lvs[mpos]].wbytes;
			tmpl = lvs[mpos];
			lvs[mpos] = lvs[newtop-1];
			lvs[newtop-1] = tmpl;
			tmp = wben[mpos];
			wben[mpos] = wben[newtop-1];
			wben[newtop-1] = tmp;
			tot_ben += recycleProfit(tmpl);
#ifdef _DEBUG_CACHE_
		        mnstr_printf(cntxt->fdout,"#Don't drop instruction %d, credit %f\n" , tmpl,tmp);
#endif
		}
		else break;
	}
	/* compare benefits of knap-sack content and the critical item */
	ci_ben = 0;             /* find the critical item */
    for (l = 0; l < ltop; l++) {
		ben = recycleProfit(lvs[l]);
		if (recycleBlk->profiler[lvs[l]].wbytes <= targmem &&
			ben > ci_ben){
			ci = l;
			ci_ben = ben;
		}
	}

	if ( ci_ben > tot_ben ) { /* save the critical item instead */
		newtop = ltop - 1;
		tmpl = lvs[ci];
		lvs[ci] = lvs[newtop];
		lvs[newtop] = tmpl;
#ifdef _DEBUG_CACHE_
		mnstr_printf(cntxt->fdout,"#Don't drop critical item : instruction %d, credit %f\n" ,tmpl,ci_ben);
#endif
	}
	GDKfree(wben);
	return newtop;

}


static void RECYCLEcleanCache(Client cntxt, lng wr0){
	int j,i,l,ltop,v,vtop;
	InstrPtr p;
	InstrPtr *old, *newstmt;
	bit  *lmask, *dmask;
	int k, *lvs, *vm;
	int limit, idx;
	size_t mem;
	int cont, reserve;
	lng oldclk, wr;
	dbl minben, ben;
	bte *used;

	if (!recycleBlk)
		return;

newpass:
	cont = 0;
	wr = wr0;
	used = (bte*)GDKzalloc(recycleBlk->vtop);

	/* set all used variables */
	for (i = 0; i < recycleBlk->stop; i++){
		p = recycleBlk->stmt[i];
#ifdef _DEBUG_CACHE_
		if ( p->token != NOOPsymbol )
#endif
		for( j = p->retc ; j< p->argc; j++)
			if (used[getArg(p,j)]<2)  used[getArg(p,j)]++;
	}

	/* find the leaves, ignore the most recent instruction */

	lmask = (bit*)GDKzalloc(recycleBlk->stop);
	ltop = 0; reserve = 0;
	for (i = 0; i < recycleBlk->stop; i++){
		p = recycleBlk->stmt[i];
#ifdef _DEBUG_CACHE_
		if ( p->token == NOOPsymbol ) continue;
#endif
		for( j = 0; j < p->retc ; j++)
			if (used[getArg(p,j)]) goto skip;
		if (i == cntxt->rcc->recent){
			reserve = i;
			continue;
		}
		lmask[i] = 1;
		ltop++;
		skip:;
	}


	if (ltop == 0 ){  /* ensure at least 1 entry to evict */
		if (reserve){
			lmask[reserve] = 1;
			ltop++;
		} else {	
			GDKfree(lmask);
			return;
		}
	}
	lvs = (int *)GDKzalloc(sizeof(int)*ltop);
	l = 0;
	for (i = 0; i < recycleBlk->stop; i++)
		if (lmask[i]) lvs[l++] = i;
	GDKfree(lmask);

	/* find the oldest */
	oldclk = recycleBlk->profiler[lvs[0]].clk;
	idx = 0;
	for (l = 0; l < ltop; l++){
		k = lvs[l];
		if( recycleBlk->profiler[k].clk < oldclk){
			oldclk = recycleBlk->profiler[k].clk;
			idx = l;
		}
	}

	/* protect leaves from current query invocation */

	if ( oldclk < cntxt->rcc->time0) {

#ifdef _DEBUG_CACHE_
			mnstr_printf(cntxt->fdout,"#Fresh-protected "LLFMT"\n", cntxt->rcc->time0);
      			mnstr_printf(cntxt->fdout,"#All leaves:");
			for (l = 0; l < ltop; l++)
                		mnstr_printf(cntxt->fdout,"%3d("LLFMT") \t",
	                		lvs[l],recycleBlk->profiler[lvs[l]].clk);
			mnstr_printf(cntxt->fdout,"\n");
#endif
		l = 0;
		for (j = 0; j < ltop; j++){
			if (recycleBlk->profiler[lvs[j]].clk < cntxt->rcc->time0)
				lvs[l++] = lvs[j];
		}
		ltop = l;
	}


#ifdef _DEBUG_CACHE_
        mnstr_printf(cntxt->fdout,"\n#RECYCLEcleanCache: policy=PROFIT usedmem="LLFMT"\n", recyclerMemoryUsed);
		mnstr_printf(cntxt->fdout,"#Target memory "LLFMT"KB Available "LLFMT"KB\n", wr,monet_memory -recyclerMemoryUsed);
        mnstr_printf(cntxt->fdout,"#Candidates for eviction\n#(# LRU\t\tTicks\tLife\tSZ\tCnt\tWgt\tBen\tProf)\n");
		for (l = 0; l < ltop; l++)
        	mnstr_printf(cntxt->fdout,"%3d "LLFMT"\t"LLFMT"\t %5.2f\t "LLFMT"\t%3d\t%5.1f\n",
                	lvs[l],recycleBlk->profiler[lvs[l]].clk,
	                recycleBlk->profiler[lvs[l]].ticks,
        	        recycleLife(lvs[l]),
                	recycleBlk->profiler[lvs[l]].wbytes,
	                recycleBlk->profiler[lvs[l]].calls,
        	        recycleW(lvs[l]));
#endif

	/* find entries to evict */
	mem = (size_t)(recyclerMemoryUsed + wr) > monet_memory ;
	vm = (int *)GDKzalloc(sizeof(int)*ltop);
	vtop = 0;

	if (!mem){	 /* evict 1 entry */
		minben = recycleProfit(lvs[0]);
		idx = 0;
		for (l = 1; l < ltop; l++){
			ben = recycleProfit(lvs[l]);
			if( ben < minben) {
				minben = ben;
				idx = l;
			}
		}
		vm[vtop++] = lvs[idx];
	} else {	/* evict several to get enough memory */
		k = 0;	/* exclude binds that don't free memory */
		for (l = 0; l < ltop; l++)
			if ( recycleBlk->profiler[lvs[l]].wbytes > 0 )
				lvs[k++] = lvs[l];
/*				mnstr_printf(cntxt->fdout,"ltop %d k %d\n",ltop, k); */
		if ( k > 0 )
			ltop = k;
		vtop = chooseVictims(cntxt,lvs, ltop, recyclerMemoryUsed + wr - monet_memory );
		for (v = 0; v < vtop; v++){
			vm[v] = lvs[v];
			wr -= recycleBlk->profiler[lvs[v]].wbytes;
		}
	}

	/* check if a new pass of cache cleaning is needed */
	if ( (size_t)(recyclerMemoryUsed + wr) > monet_memory )
		cont = 1;

#ifdef _DEBUG_CACHE_
	mnstr_printf(cntxt->fdout,"#Evicted %d instruction(s) \n ",vtop);
	for(v=0; v<vtop;v++){
		mnstr_printf(cntxt->fdout,"%d\t",vm[v]);
		printInstruction(cntxt->fdout,recycleBlk,0,recycleBlk->stmt[vm[v]], LIST_MAL_ALL);
		mnstr_printf(cntxt->fdout,"\n");
	}
#endif

	GDKfree(lvs);
	/* drop victims in one pass */
	dmask = (bit *)GDKzalloc(recycleBlk->stop);
	for (v = 0; v < vtop; v++)
		dmask[vm[v]] = 1;
	GDKfree(vm);
	(void) newstmt;

	old = recycleBlk->stmt;
	limit = recycleBlk->stop;

    newstmt = (InstrPtr *) GDKzalloc(sizeof(InstrPtr) * recycleBlk->ssize);
    if (newstmt == NULL){
        GDKerror("newMalBlk:"MAL_MALLOC_FAIL);
        return;
    }
    recycleBlk->stmt = newstmt;
    recycleBlk->stop = 0;

	k = 0;
	for (i = 0; i < limit ; i++){
		p = old[i];
		if( dmask[i] ) {
			RECYCLEgarbagecollect(recycleBlk,p,used);
			recyclerMemoryUsed -= recycleBlk->profiler[i].wbytes;
			freeInstruction(p);
		}
		else {
			pushInstruction(recycleBlk,p);
			recycleBlk->profiler[k++]= recycleBlk->profiler[i];
		}
	}

	GDKfree(old);
	GDKfree(used);
	/* remove all un-used variables as well */
	trimMalVariables(recycleBlk);

	GDKfree(dmask);
	if (cont) goto newpass;

}

/*
 * To avoid a polution of the recycle cache, we do not store any
 * intruction for which there is not function/command/pattern implementation.
 * Likewise, we avoid all simple constant assigments.
 */
int
RECYCLEinterest(InstrPtr p){
	if (p->recycle <= REC_NO_INTEREST )
		return 0;
	return getFunctionId(p) != NULL;
}


#ifdef _DEBUG_CACHE_
static void
RECYCLEsync(InstrPtr p)
{
	int i, j, k;
	InstrPtr q;
	ValPtr pa, qa;

	for (i=0; i<recycleBlk->stop; i++) {
		q = getInstrPtr(recycleBlk,i);
		if ((getFunctionId(p) != getFunctionId(q)) ||
			(p->argc != q->argc) ||
			(getModuleId(p) != getModuleId(q)))
			continue;
		for (j=p->retc; j<p->argc; j++)
			if( VALcmp( &getVarConstant(recycleBlk,getArg(p,j)),
						&getVarConstant(recycleBlk,getArg(q,j))))
				break;
		if (j == p->argc) {
			for(k=0; k< p->retc; k++){
				pa = &getVarConstant(recycleBlk,getArg(p,k));
				qa = &getVarConstant(recycleBlk,getArg(q,k));
				if (qa->vtype == TYPE_bat)
					BBPdecref( *(const int*)VALptr(qa), TRUE);
				VALcopy(qa,pa);
				if (qa->vtype == TYPE_bat)
					BBPincref( *(const int*)VALptr(qa), TRUE);
			}
		}
	}
}
#endif

static void
RECYCLEkeep(Client cntxt, MalBlkPtr mb, MalStkPtr s, InstrPtr p, RuntimeProfile prof)
{
	int i, j, c;
	ValRecord *v;
	ValRecord cst;
	InstrPtr q;
	int pc= prof->stkpc;
	lng rd= mb->profiler[pc].rbytes;
	lng wr= mb->profiler[pc].wbytes;
	lng clk= mb->profiler[pc].clk;

	if ( recycleSize >= recycleCacheLimit)
		return ; /* no more caching */
	if ( (size_t)(recyclerMemoryUsed + wr) > monet_memory)
		return ; /* no more caching */

	/*
	 * The instruction is copied and the variables are
	 * all assigned to the symbol table. This means the
	 * link with their source disappears. We can later only
	 * compare by value.
	 */
	q = copyInstruction(p);
	for (i = 0; i< p->argc; i++) {
		j= getArg(p,i);
		v = &s->stk[j];
		VALcopy(&cst,v);
		c = fndConstant(recycleBlk, &cst, recycleBlk->vtop);
#ifdef _DEBUG_RECYCLE_
		//printf("#CONSTANT %s %d\n", getVarName(mb,j), c);
#endif
		if (c<0)
			c = defConstant(recycleBlk, v->vtype, &cst);
		if (v->vtype == TYPE_bat)
			BBPincref( *(const int*)VALptr(v), TRUE);
		setVarUsed(recycleBlk,c);
	 	setArg(q,i,c);
	}
#ifdef _DEBUG_RECYCLE_
	mnstr_printf(cntxt->fdout,"#RECYCLE [%3d] ",recycleBlk->stop);
	printInstruction( cntxt->fdout,recycleBlk, 0, q,LIST_MAL_STMT);
#else
	(void) cntxt;
#endif

	pushInstruction(recycleBlk,q);
	i = recycleBlk->stop-1;
	recycleBlk->profiler[i].clk = clk; // used for LRU scheme
	recycleBlk->profiler[i].calls =1;
	recycleBlk->profiler[i].ticks = GDKusec()-clk;
	recycleBlk->profiler[i].rbytes = rd;
	recycleBlk->profiler[i].wbytes = wr;
	recyclerMemoryUsed += wr;
	cntxt->rcc->recent = i;
	cntxt->rcc->RPadded0++;

#ifdef _DEBUG_CACHE_
	RECYCLEsync(q);
#endif
}

/*
 * The generic wrappers for accessing the recycled instructions.
 * Before the interpreter loop is allowed to execute the instruction
 * we check the recycle table for available variables.
 *
 * Searching for useful recycle instructions is the real challenge.
 * There are two major approaches. The first approach is to search
 * for an identical operation as the target under investigation and
 * reuse its result if possible.
 * The second approach uses the semantics of operations and
 * replaces the arguments of the target to make it cheaper to execute.
 * For example, a previous result of a scan may be used instead
 * or it can be small compared to the target.
 *
 * We should avoid adding the same operation twice, which
 * means it should be easy to find them in the first place.
 * Furthermore, we should only search for instructions if
 * we are dealing with a function call.
 */
static int
RECYCLEfind(Client cntxt, MalBlkPtr mb, MalStkPtr s, InstrPtr p)
{
	int i, j;
	InstrPtr q;
	//lng clk = GDKusec();

	(void) mb;
	if( recycleBlk == 0)
		return -1;

	(void) cntxt;
	for (i=0; i<recycleBlk->stop; i++) {
		q = getInstrPtr(recycleBlk,i);
		if ((getFunctionId(p) != getFunctionId(q)) ||
			(p->argc != q->argc-1) ||
			(getModuleId(p) != getModuleId(q)))
				continue;
		for (j=p->retc; j<p->argc; j++)
			if( VALcmp( &s->stk[getArg(p,j)], &getVarConstant(recycleBlk,getArg(q,j))))
				break;
		if (j == p->argc)
#ifdef _DEBUG_CACHE_
			if ( q->token != NOOPsymbol )
#endif
			return i;
	}
	//recycleSearchTime = GDKusec()-clk;
	return -1;
}


#define boundcheck(flag,a) ((flag)?a<=0:a<0)
/* check if instruction p at the stack is a subset selection of the RecyclerPool instruction q */

static bit
selectSubsume(InstrPtr p, InstrPtr q, MalStkPtr s)
{
	int lcomp, rcomp;
	bit li, hi, lip, hip, cover=0;
	int base =2;
	if ( p->argc >7) // candidate list
		base = 3;

	lcomp = VALcmp(&getVar(recycleBlk,getArg(q,base))->value,
			&s->stk[getArg(p,base)]);
	if ( p->argc == base+1)
		rcomp = VALcmp( &s->stk[getArg(p,base)],
			&getVar(recycleBlk,getArg(q,base+1))->value);
	else
		rcomp = VALcmp( &s->stk[getArg(p,base+1)],
			&getVar(recycleBlk,getArg(q,base+1))->value);
	if ( q->argc == base +4)
		cover = ( lcomp <=0 && rcomp <=0 );
	if ( q->argc == base +5){
		li = *(bit*)getVarValue(recycleBlk,getArg(q,base+2));
		hi = *(bit*)getVarValue(recycleBlk,getArg(q,base+3));
		if (p->argc <=4)
			cover = boundcheck(li,lcomp) && boundcheck(hi,rcomp);
		else {
			lip = *(const bit*)VALptr(&s->stk[getArg(p,base+2)]);
			hip = *(const bit*)VALptr(&s->stk[getArg(p,base+3)]);
			cover = ( boundcheck(li || neg(lip),lcomp) && boundcheck(hi || neg(hip),rcomp) );
		}
	}
	return cover;
}


static bit
likeSubsume(InstrPtr p, InstrPtr q, MalStkPtr s)
{
	str ps, qs, pd, qd, ps0, qs0;
	size_t pl, ql;
	bit first = 1, endp = 0, stop = 0, cover = 0;

	ps = GDKstrdup(s->stk[getArg(p,2)].val.sval);
	qs = GDKstrdup(getVar(recycleBlk,getArg(q,2))->value.val.sval);

	ps0 = ps; qs0 = qs;

	while ((qd = strchr(qs,'%')) != NULL && !stop && !endp){
		*qd = 0;
		if ((pd = strchr(ps,'%')) == NULL)
			endp = 1;
		else *pd = 0;
		ql = strlen(qs);
   	    pl = strlen(ps);
	    if (ql > pl ){
			stop = 1;
			break;
	    }
		if (first){
			if (strncmp(ps,qs,ql) != 0) {
				stop = 1;
				break;
			}
			first = 0;
		}
		else {
	 		if (strstr(ps,qs) == NULL) {
				stop = 1;
				break;
			}
		}
		if (!endp) ps = pd+1;
		qs = qd+1;
 	}

	if (stop) cover = 0;
	else  /* compare remainders after the last % in some of the strings*/
		if (strcmp(ps,"")==0 && strcmp(qs,"")==0) { /*printf("Successful subsumption\n");*/
      cover = 1;
   } else {
	    ql = strlen(qs);
        pl = strlen(ps);
   	if (ql > pl ){			/* printf("No subsumption\n"); */
      	cover = 0;
   	}
		else if (strncmp(ps+pl-ql,qs,ql) != 0) {/*printf("No subsumption\n"); */
			cover = 0;
		}
		else cover = 1;
	}
	GDKfree(ps0);
	GDKfree(qs0);
	return cover;
}


/* p,q margins of 2 intervals. Returns true if p is less than q:
- p is nil (incl q and p are nil)
- p and q != nil and p < q
- if eq is TRUE also checks for equality
*/


static bit
lessEq(ValPtr p, bit pi, ValPtr q, bit qi, bit eq)
{

    int (*cmp) (const void *, const void *);
    int tpe, c;
    const void *nilptr, *pp, *pq;

    if( p == 0 || q == 0 ) return  0;
    if( (tpe = p ->vtype) != q->vtype ) return  0;

    cmp = BATatoms[tpe].atomCmp;
    nilptr = ATOMnilptr(tpe);
    pp = VALptr(p);
    pq = VALptr(q);
    if( (*cmp)(pp, nilptr) == 0 ) return 1; /* p is nil */
    if( (*cmp)(pq, nilptr) == 0 )  return 0;
    c = (*cmp)(pp, pq);
	 if ( c < 0 ) return 1;
	 if (c == 0 ) {
			if ( eq ) return (pi || neg(qi)); /* less or eq */
			else return (pi && neg(qi));		/* strict less */
	 }
	 return 0;
}

/* p,q margins of 2 intervals. Returns true if p is greater than q:
- p is nil (incl q and p are nil)
- p and q != nil and p > q
- if eq is TRUE also checks for equality
*/

static bit
greaterEq(ValPtr p, bit pi, ValPtr q, bit qi, bit eq)
{

    int (*cmp) (const void *, const void *);
    int tpe, c;
    const void *nilptr, *pp, *pq;

    if( p == 0 || q == 0 ) return  0;
    if( (tpe = p ->vtype) != q->vtype ) return  0;

    cmp = BATatoms[tpe].atomCmp;
    nilptr = ATOMnilptr(tpe);
    pp = VALptr(p);
    pq = VALptr(q);
    if( (*cmp)(pp, nilptr) == 0 ) return 1; /* p is nil */
    if( (*cmp)(pq, nilptr) == 0 )  return 0;
    c = (*cmp)(pp, pq);
	 if ( c > 0 ) return 1;
	 if (c == 0 ){
			if ( eq ) return (pi || neg(qi)); /* greater or eq */
			else return (pi && neg(qi));		/* strict greater */
	}
	return 0;
}

static bit
thetaselectSubsume(InstrPtr p, InstrPtr q, MalStkPtr s)
{
	ValPtr pval, qval;
	const char *pop, *qop;
	bit pi, qi;

	qval = &getVar(recycleBlk,getArg(q,2))->value;
	qop = (const char *)getVarValue(recycleBlk,getArg(q,3));

	pval = &s->stk[getArg(p,2)];
	pop = (const char *)VALptr(&s->stk[getArg(p,3)]);

	if ( pop[0] != qop[0] ) return 0;
	pi = ( pop[1] == '=' );
	qi = ( qop[1] == '=' );

	if ( qop[0] == '<' ) {
		if ( lessEq(pval,pi,qval,qi,FALSE) )
			return 1;
		else return 0;
	} else
	if ( qop[0] == '>' ) {
		if ( greaterEq(pval,pi,qval,qi,FALSE) )
			return 1;
		else return 0;
	}
	return 0;
}

static int
RECYCLEreuse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile outerprof)
{
    int i, j, pc= -1;
    bat bid= 0, nbid= 0;
    InstrPtr q;

    MT_lock_set(&recycleLock, "recycle");

    for (i = 0; i < recycleBlk->stop; i++){
        q = getInstrPtr(recycleBlk,i);

        if ((getFunctionId(p) != getFunctionId(q)) ||
            (getModuleId(p) != getModuleId(q)))
            continue;
		if (p->argc != q->argc || p->retc != q->retc) 
			continue;

		/* 1: reuse smallest range covering in select operations */
		if (q->argc > 3 &&
			( 	getFunctionId(p) == subselectRef ||
				getFunctionId(p) == likesubselectRef ||
				getFunctionId(p) == thetasubselectRef ) &&
				getVarConstant(recycleBlk, getArg(q,1)).val.bval == stk->stk[getArg(p,1)].val.bval &&
				BATatoms[getArgType(recycleBlk,q,2)].linear )
		{ 	bit	subsmp = 0;
			/* Time to check for the inclusion constraint */
			if ( getFunctionId(p) == subselectRef )
				subsmp = selectSubsume(p,q,stk);
			else if ( getFunctionId(p) == likesubselectRef )
				subsmp = likeSubsume(p,q,stk);
			else if ( getFunctionId(p) == thetasubselectRef )
				subsmp = thetaselectSubsume(p,q,stk);

			/* select the smallest candidate list */
			if (subsmp){
				BAT *b1, *b2;
				nbid = getVarConstant(recycleBlk, getArg(q,0)).val.bval;
				if( bid == 0){
					bid = nbid;
					pc = i;
					b1 = BBPquickdesc(ABS(bid), FALSE);
				} else {
					b1 = BBPquickdesc(ABS(bid), FALSE);
					b2 = BBPquickdesc(ABS(nbid), FALSE);
					if (b1 && b2 && BATcount(b1) > BATcount(b2)){
						bid = nbid;
						pc = i;
					}
				}
			}
		}
		/* 2: exact covering */
		for (j = p->retc; j < p->argc; j++)
			if (VALcmp(&stk->stk[getArg(p,j)], &getVarConstant(recycleBlk,    getArg(q,j))))
				goto notfound;

		/* found an exact match, get the results on the stack */
		for( j=0; j<p->retc; j++){
			VALcopy(&stk->stk[getArg(p,j)], &getVarConstant(recycleBlk,getArg(q,j)) );
			if (stk->stk[getArg(p,j)].vtype == TYPE_bat)
				BBPincref( stk->stk[getArg(p,j)].val.bval , TRUE);
		}
		recycleBlk->profiler[i].calls++;
		recycleBlk->profiler[i].clk = GDKusec();
		if ( !( getModuleId(p) == sqlRef && ( bindRef == getFunctionId(p) || bind_idxRef == getFunctionId(p))))
			recycled++;
		cntxt->rcc->recent = i;
		MT_lock_unset(&recycleLock, "recycle");
		return i;
		notfound:
			continue;
    }
    /*
     * We have a candidate table 
     */
    if (bid ) {
        int k;
		RuntimeProfileRecord prof;

        i= getPC(mb,p);
#ifdef _DEBUG_RECYCLE_
		mnstr_printf(cntxt->fdout,"#RECYCLEreuse subselect using candidate list");
		printInstruction(cntxt->fdout, recycleBlk, 0,getInstrPtr(recycleBlk,pc),    LIST_MAL_STMT);
#endif
		nbid = stk->stk[getArg(p,2)].val.bval;
        stk->stk[getArg(p,2)].val.bval = bid;
        BBPincref(ABS(bid), TRUE);
        /* make sure the garbage collector is not called, it is taken care of by the current call */
        j = stk->keepAlive ;
        stk->keepAlive = TRUE;
        k = p->recycle;
        p->recycle = NO_RECYCLING; /* No recycling for instructions with subsumption */
		runtimeProfileInit(cntxt, mb, stk);
        runtimeProfileBegin(cntxt, mb, stk, i, &prof, 1);
        (void) reenterMAL(cntxt,mb,i,i+1,stk);
		runtimeProfileExit(cntxt, mb, stk, p, &prof);
        p->recycle= k;
        stk->keepAlive= j;
        BBPdecref(bid, TRUE);
		recycled++;
        recycleBlk->profiler[pc].calls++;
        recycleBlk->profiler[pc].clk = GDKusec();
        MT_lock_unset(&recycleLock, "recycle");
        RECYCLEexitImpl(cntxt, mb, stk, p, &prof);
        stk->stk[getArg(p,2)].val.bval = nbid;
        return pc;
    }

    MT_lock_unset(&recycleLock, "recycle");
	if ( pc >= 0 ) 		/* successful multi-subsumption */
		RECYCLEexitImpl(cntxt,mb,stk,p, outerprof);
    return pc;
}

lng
RECYCLEentry(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof)
{
	int i=0;

	cntxt->rcc->statements++;
	if ( p->recycle == NO_RECYCLING )
		return 0;       /* don't count subsumption instructions */
	if ( !RECYCLEinterest(p) )  /* don't scan RecyclerPool for non-monitored instructions */
		return 0;
	if ( recycleBlk == NULL ){
		RECYCLEspace();
		if ( recycleBlk == NULL )
			return 0;
	}
	i = RECYCLEreuse(cntxt,mb,stk,p,prof);
#ifdef _DEBUG_RECYCLE_
	if ( i>=0 ){
		mnstr_printf(cntxt->fdout,"#REUSED  [%3d]   ",i);
		printInstruction(cntxt->fdout,mb,0,p, LIST_MAL_STMT);
	}
#endif
	return i>=0;
}

/*
 * The 'exit' instruction is called after the interpreter loop
 * itself and has to decide on the results obtained.
 * This is the place where we should call recycle optimization routines( admission policies).
 * It can use the timing information gathered from the previous call,
 * which is stored in the stack frame to avoid concurrency problems.
 */
void
RECYCLEexitImpl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof)
{
	size_t wr;

	if ( (wr = (size_t)mb->profiler[prof->stkpc].wbytes) > monet_memory)
		goto finishexit;
	MT_lock_set(&recycleLock, "recycle");
	if (recycleBlk){
		if ( (size_t)(recyclerMemoryUsed +  wr) > monet_memory ||
	    		recycleSize >= recycleCacheLimit )
			RECYCLEcleanCache(cntxt, wr);
	}

	if ( RECYCLEinterest(p)){
		/* ADM_ALL: infinite case, admit all new instructions */
		if (RECYCLEfind(cntxt,mb,stk,p)<0 )
			(void) RECYCLEkeep(cntxt,mb, stk, p, prof);
	}
finishexit:
	MT_lock_unset(&recycleLock, "recycle");
}

void
RECYCLEexit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof)
{
	if ( mb->profiler == 0) /* don't use recycling before initialization by prelude() */
		return;
	RECYCLEexitImpl(cntxt,mb,stk,p,prof);
}

/*
 * At the end of session we should remove all
 * knowledge from the recycle cache.
 */
void
RECYCLEshutdown(Client cntxt){
	MalBlkPtr mb = recycleBlk;
	int i;
	bte *used;
	Client c;

	if( recycleBlk == NULL)
		return ;

#ifdef _DEBUG_RECYCLE_
	mnstr_printf(cntxt->fdout,"#RECYCLE shutdown\n");
	printFunction(cntxt->fdout, recycleBlk,0,0);
	printStack(cntxt->fdout,mb,0);
#else
	(void) cntxt;
#endif

	used = (bte*)GDKzalloc(recycleBlk->vtop);
	MT_lock_set(&recycleLock, "recycle");
	recycleBlk = NULL;
	recycleSearchTime = 0;
	recyclerMemoryUsed = 0;
	for(c = mal_clients; c < mal_clients+MAL_MAXCLIENTS; c++)
		if (c->mode != FREECLIENT) {
			memset((char *)c->rcc, 0, sizeof(RecStat));
    }
	MT_lock_unset(&recycleLock, "recycle");
	for (i=mb->stop-1; i>=0; i--)
		RECYCLEgarbagecollect(mb, getInstrPtr(mb,i),used);
	freeMalBlk(mb);
	GDKfree(used);
}

/*
 * Once we encounter an update we check and clean the recycle cache from
 * instructions dependent on the updated bat.
 */

str 
RECYCLEcolumn(Client cntxt,str sch,str tbl, str col)
{
	int sid= 0,tid= 0,cid =0,i,j;
	char *release;
	InstrPtr *old,p;
	int limit;
	ValRecord *v,vr;
	
#ifdef _DEBUG_RESET_
	mnstr_printf(cntxt->fdout,"#POOL BEFORE CLEANUP\n");
	RECYCLEdump(cntxt->fdout);
#endif
	MT_lock_set(&recycleLock, "recycle");
	release= (char*) GDKzalloc(recycleBlk->vtop);
	vr.vtype = TYPE_str;
	vr.len  = strlen(sch);
	vr.val.sval = sch;
	sid = fndConstant(recycleBlk,&vr, recycleBlk->vtop);
	vr.val.sval = tbl;
	vr.len  = strlen(tbl);
	tid = fndConstant(recycleBlk,&vr, recycleBlk->vtop);
	if ( col){
		vr.val.sval = col;
		vr.len  = strlen(col);
		cid = fndConstant(recycleBlk,&vr,recycleBlk->vtop);
	}
#ifdef _DEBUG_RESET_
		mnstr_printf(cntxt->fdout,"#RECYCLEcolumn %d %d %d\n",sid,tid,cid);
#endif

	limit= recycleBlk->stop;
	old= recycleBlk->stmt;
	// locate the bind instruction to drop
	for(j= i= 0; i < limit; i++){
		p= old[i];
		if ( getModuleId(p)== sqlRef &&
			 (getFunctionId(p) == bindRef || getFunctionId(p) == bind_idxRef) &&
			getArg(p,2) == sid && getArg(p,3)==tid && (col== 0 ||getArg(p,4)==cid))
		j= release[getArg(p,0)]= 1;
	}
	if ( j ==0){ // Nothing found
		MT_lock_unset(&recycleLock, "recycle");
		GDKfree(release);
		return MAL_SUCCEED;
	}

	if( newMalBlkStmt(recycleBlk,recycleBlk->ssize) < 0){
		MT_lock_unset(&recycleLock, "recycle");
		GDKfree(release);
		throw(MAL,"recycler.reset",MAL_MALLOC_FAIL);
	}

	for(i= 0; i < limit; i++){
		p= old[i];
		for(j=0;j<p->argc;j++)
			if( release[getArg(p,j)] ) break;

		if ( j == p->argc) {
			pushInstruction(recycleBlk,p);
			continue;
		}
#ifdef _DEBUG_RESET_
		mnstr_printf(cntxt->fdout,"#Marked for eviction [%d]",i);
		printInstruction(cntxt->fdout,recycleBlk,0,p, LIST_MAL_DEBUG);
#endif
		for(j=0;j<p->argc;j++) {
			release[getArg(p,j)]=1;//propagate the removal request
			if (j < p->retc && isaBatType(getArgType(recycleBlk,p,j)) ){
				v = &getVarConstant(recycleBlk,getArg(p,j));
				BBPdecref(ABS(v->val.bval), TRUE);
			}
		}
		freeInstruction(p);
	}
	MT_lock_unset(&recycleLock, "recycle");
	GDKfree(release);
	GDKfree(old);
#ifdef _DEBUG_RESET_
	mnstr_printf(cntxt->fdout,"#POOL AFTER CLEANUP\n");
	RECYCLEdump(cntxt->fdout);
#endif
	return MAL_SUCCEED;
}

str 
RECYCLEresetBAT(Client cntxt, int bid)
{
	int i,j;
	char *release;
	InstrPtr *old,p;
	int limit;
	ValRecord *v;
	
#ifdef _DEBUG_RESET_
	mnstr_printf(cntxt->fdout,"#POOL BEFORE CLEANUP\n");
	RECYCLEdump(cntxt->fdout);
#endif
	MT_lock_set(&recycleLock, "recycle");
	release= (char*) GDKzalloc(recycleBlk->vtop);
	limit= recycleBlk->stop;
	old= recycleBlk->stmt;
	if( newMalBlkStmt(recycleBlk,recycleBlk->ssize) < 0){
		MT_lock_unset(&recycleLock, "recycle");
		GDKfree(release);
		throw(MAL,"recycler.reset",MAL_MALLOC_FAIL);
	}

	for(i= 0; i < limit; i++){
		p= old[i];
		
		for(j=0;j<p->argc;j++)
			if( isaBatType(getArgType(recycleBlk,p,j)) && 
				getVarConstant(recycleBlk, getArg(p,j)).val.bval == bid)
			release[getArg(p,j)]= 1;
		
		for(j=0;j<p->argc;j++)
			if( release[getArg(p,j)] ) break;

		if ( j == p->argc) {
			pushInstruction(recycleBlk,p);
			continue;
		}
#ifdef _DEBUG_RESET_
		mnstr_printf(cntxt->fdout,"#Marked for eviction [%d]",i);
		printInstruction(cntxt->fdout,recycleBlk,0,p, LIST_MAL_DEBUG);
#endif
		for(j=0;j<p->argc;j++) {
			release[getArg(p,j)]=1;//propagate the removal request
			if (j < p->retc && isaBatType(getArgType(recycleBlk,p,j)) ){
				v = &getVarConstant(recycleBlk,getArg(p,j));
				BBPdecref(ABS(v->val.bval), TRUE);
			}
		}
		freeInstruction(p);
	}
	MT_lock_unset(&recycleLock, "recycle");
	GDKfree(release);
	GDKfree(old);
#ifdef _DEBUG_RESET_
	mnstr_printf(cntxt->fdout,"#POOL AFTER CLEANUP\n");
	RECYCLEdump(cntxt->fdout);
#endif
	return MAL_SUCCEED;
}

str
RECYCLEstart(Client cntxt)
{
    cntxt->rcc->recent = -1;
    cntxt->rcc->time0 = GDKusec();
    cntxt->rcc->RPadded0 = 0;
    cntxt->rcc->RPreset0 = 0;
	return MAL_SUCCEED;
}

str
RECYCLEstop(Client cntxt)
{
	(void)cntxt;
	return MAL_SUCCEED;
}

void
RECYCLEdump(stream *s)
{
    int i, incache;
    str msg= MAL_SUCCEED;
    lng sz=0, persmem=0;
    ValPtr v;
    Client c;
    lng statements=0, recycleMiss=0, recycleRem=0;

    if (!recycleBlk) return;

    mnstr_printf(s,"#RECYCLER  CATALOG admission ADM_ALL\n");
    mnstr_printf(s,"#CACHE= policy PROFIT limit= %d \n", recycleCacheLimit);
    mnstr_printf(s,"#RESOURCES hard stmt = %d hard var = %d hard mem="SZFMT"\n", HARDLIMIT_STMT, HARDLIMIT_VAR, monet_memory);

    for(i=0; i< recycleBlk->stop; i++){
        v = &getVarConstant(recycleBlk,getArg(recycleBlk->stmt[i],0));
        if ((v->vtype == TYPE_bat) &&
             (BBP_status( *(const int*)VALptr(v)) & BBPPERSISTENT)) {
			assert(0);
            //msg = BKCbatsize(&sz, (int*)VALget(v));
            if ( msg == MAL_SUCCEED )
                persmem += sz;
        }
    }
    persmem = (lng) persmem/RU;

    for(c = mal_clients; c < mal_clients+MAL_MAXCLIENTS; c++)
        if (c->mode != FREECLIENT) {
            statements += c->rcc->statements;
            recycleMiss += c->rcc->recycleMiss;
            recycleRem += c->rcc->recycleRem;
        };

    incache = recycleBlk->stop;
    mnstr_printf(s,"#recycled = "LLFMT" incache= %d executed = "LLFMT" memory(KB)= "LLFMT" PersBat memory="LLFMT"\n",
         recycled, incache, statements, recyclerMemoryUsed, persmem);
#ifdef _DEBUG_CACHE_
    mnstr_printf(s,"#RPremoved = "LLFMT" RPactive= "LLFMT" RPmisses = "LLFMT"\n",
                 recycleRem, incache-recycleRem, recycleMiss);
#endif
    mnstr_printf(s,"#Cache search time= "LLFMT"(usec)\n",recycleSearchTime);

    /* and dump the statistics per instruction*/
	mnstr_printf(s,"# CL\t   lru\t\tcnt\t ticks\t rd\t wr\t Instr\n");
    for(i=0; i< recycleBlk->stop; i++){
        if (getInstrPtr(recycleBlk,i)->token == NOOPsymbol)
            mnstr_printf(s,"#NOOP ");
        else mnstr_printf(s,"#     ");
        mnstr_printf(s,"%4d\t"LLFMT"\t%d\t"LLFMT"\t"LLFMT"\t"LLFMT"\t%s\n", i,
            recycleBlk->profiler[i].clk,
            recycleBlk->profiler[i].calls,
            recycleBlk->profiler[i].ticks,
            recycleBlk->profiler[i].rbytes,
            recycleBlk->profiler[i].wbytes,
            instruction2str(recycleBlk,0,getInstrPtr(recycleBlk,i),LIST_MAL_DEBUG));
    }

}
