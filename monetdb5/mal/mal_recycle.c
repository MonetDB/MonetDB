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
#include "mal_resource.h"
#include "mal_listing.h"
#include "mal_runtime.h"

static MT_Lock recycleLock MT_LOCK_INITIALIZER("recycleLock");
MalBlkPtr recycleBlk = NULL;

#define set1(x,i) ( x | ((lng)1 << i) )
#define set0(x,i) ( x & ~((lng)1 << i) )
#define getbit(x,i) ( x & ((lng)1 << i) )
#define neg(x) ( (x)?FALSE:TRUE)

int recycleCacheLimit=0; /* No limit by default */

/*
 * Monitoring the Recycler
 */
static lng recyclerMemoryUsed = 0;
static lng recyclerSavings = 0;
static lng recycled=0;
static lng statements =0;
static lng recycleSearchTime =0;	/* cache search time in ms*/

/*
 * The profiler record is re-used to store recycler information.
 * The clk is used by the LRU scheme, counter is the number of
 * times this pattern was used, ticks is the clock ticks
 * used to produce the result. wbytes is the best approximation
 * of the amount of storage needed for the intermediate.
 * The read cost depends on too many factors.
 *
 * For each intermediate we have to assume that at some point
 * it is written to disk. This is the most expensive cost involved.
 * For it means we also have to read it back in again.
 * This leads to a cost estimate C =2 * writecost(Bytes)
 * If this number is smaller the the inital cpu cost it is
 * worth considering to keep the result. Otherwise we simply
 * recalculate it.
 * Once stored, an intermediate becomes more valuable as it is
 * reused more often.
 *
 * If we don't write the intermediate, then its actual read time
 * is the normalised cost.
 */

#define MB (1024*1024)
#define USECperMB (75/1000000.0) /* 75MB per second, should be determined once */
#define IOcost(B)  (B/(MB) * USECperMB)
//#define recycleProfit2(X)  (recycleBlk->profiler[X].wbytes? IOcost(recycleBlk->profiler[X].wbytes + recycleBlk->profiler[X].rbytes): recycleBlk->profiler[X].ticks) 
#define recycleProfit2(X)  (double)(recycleBlk->profiler[X].ticks)

#define recycleCost(X) (recycleBlk->profiler[X].wbytes)
#define recycleW(X)  ((recycleBlk->profiler[X].calls >1 ) ? recycleBlk->profiler[X].calls : 0.1 )

#define recycleLife(X) ((GDKusec() - recycleBlk->profiler[X].rbytes)/ 1000.0)
#define recycleProfit(X) recycleCost(X) * recycleW(X) 
/*
 * The new cost function is focussed on minimizing the IO overhead
 */

static str bindRef = 0, bind_idxRef = 0, sqlRef = 0;
static str subselectRef = 0, thetasubselectRef = 0, likesubselectRef = 0;
static void RECYCLEexitImpl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof);
static void RECYCLEdumpInternal(stream *s);

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
		if(isaBatType(getArgType(mb, q,j)) ){
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

/* leaves : array of indices of leaf instructions */
/* choose victum based on the predicted benefit from keeping it around */

static
int chooseVictims(Client cntxt, int *leaves, int ltop, lng wr)
{
	dbl *wben, ben = 0, maxwb, tmp, ci_ben, tot_ben;
	int l, newtop, mpos, tmpl, ci = 0;
	lng sz, totmem = 0, targmem, smem;

	(void) cntxt;

	wben = (dbl *)GDKzalloc(sizeof(dbl)*ltop);
	for (l = 0; l < ltop; l++){
		sz = recycleBlk->profiler[leaves[l]].wbytes;
		ben = recycleProfit2(leaves[l]);
		wben[l] = sz? ben / sz : -1;
#ifdef _DEBUG_CACHE_
		mnstr_printf(cntxt->fdout,"#leaf[%d], wr "LLFMT" size "LLFMT" benefit %6.2f (%6.2f) wben %6.2f\n" ,l, wr, sz, (float)ben, recycleProfit2(l), (float)wben[l]);
#endif
		totmem += sz;
	}
	if (totmem <= wr) { /* all leaves need to be dropped */
		GDKfree(wben);
#ifdef _DEBUG_CACHE_
		mnstr_printf(cntxt->fdout,"#drop all leaves\n");
#endif
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
			if ((lng) recycleBlk->profiler[leaves[l]].wbytes > targmem - smem)
				wben[l] = -1;
			if (maxwb < wben[l]){
				maxwb = wben[l];
				mpos = l;
			}
		}
		if ( maxwb ){			   /* add it to the sack (newtop - ltop)*/
			smem += recycleBlk->profiler[leaves[mpos]].wbytes;
			tmpl = leaves[mpos];
			leaves[mpos] = leaves[newtop-1];
			leaves[newtop-1] = tmpl;
			tmp = wben[mpos];
			wben[mpos] = wben[newtop-1];
			wben[newtop-1] = tmp;
			tot_ben += recycleProfit2(tmpl);
		}
		else break;
	}
	/* compare benefits of knap-sack content and the critical item */
	ci_ben = 0;             /* find the critical item */
    for (l = 0; l < ltop; l++) {
		ben = recycleProfit2(leaves[l]);
		if (recycleBlk->profiler[leaves[l]].wbytes <= targmem &&
			ben > ci_ben){
			ci = l;
			ci_ben = ben;
		}
	}

	if ( ci_ben > tot_ben ) { /* save the critical item instead */
		newtop = ltop - 1;
		tmpl = leaves[ci];
		leaves[ci] = leaves[newtop];
		leaves[newtop] = tmpl;
#ifdef _DEBUG_CACHE_
		mnstr_printf(cntxt->fdout,"#Don't drop critical item : instruction %d, credit %f\n" ,tmpl,ci_ben);
#endif
	}
	GDKfree(wben);
	return newtop;

}


static void RECYCLEcleanCache(Client cntxt){
	int j,i,l,ltop,v,vtop;
	InstrPtr p;
	InstrPtr *old, *newstmt;
	bit  *lmask, *dmask;
	int k, *leaves, *vm;
	int limit, idx;
	size_t mem;
	lng wr = recyclerMemoryUsed - (lng) (MEMORY_THRESHOLD * monet_memory);
	dbl minben, ben;
	bte *used;

#ifdef _DEBUG_RESET_
	//mnstr_printf(cntxt->fdout,"#CACHE BEFORE CLEANUP %d\n",recycleCacheLimit);
	//RECYCLEdumpInternal(cntxt->fdout);
#endif
newpass:
	if ( recycleBlk == 0 || recycleBlk->stop == 0)
		return;
	if ( recycleBlk->stop < recycleCacheLimit)
		return;

	used = (bte*)GDKzalloc(recycleBlk->vtop);

	/* set all variables used */
	for (i = 0; i < recycleBlk->stop; i++){
		p = recycleBlk->stmt[i];
		for( j = p->retc ; j< p->argc; j++)
			if (used[getArg(p,j)]<2)  used[getArg(p,j)]++;
	}

	/* find the leafs */
	lmask = (bit*)GDKzalloc(recycleBlk->stop);
	ltop = 0; 
	for (i = 0; i < recycleBlk->stop; i++){
		p = recycleBlk->stmt[i];
		for( j = 0; j < p->retc ; j++)
			if (used[getArg(p,j)]) goto skip;
		lmask[i] = 1;
		ltop++;
		skip:;
	}

	if (ltop == 0 ){  
		GDKfree(lmask);
		return;
	}
	leaves = (int *)GDKzalloc(sizeof(int)*ltop);
	l = 0;
	for (i = 0; i < recycleBlk->stop; i++){
		if (lmask[i]) leaves[l++] = i;
	}
	GDKfree(lmask);

#ifdef _DEBUG_CACHE_
	//mnstr_printf(cntxt->fdout,"#RECYCLEcleanCache: usedmem="LLFMT"\n", recyclerMemoryUsed);
	//mnstr_printf(cntxt->fdout,"#Candidates for eviction\n#LRU\tclk\t\tticks\t\twbytes\tCalls\tProfit\n");
	//for (l = 0; l < ltop; l++)
		//mnstr_printf(cntxt->fdout,"#%3d\t"LLFMT"\t"LLFMT"\t\t "LLFMT"\t%3d\t%5.1f\n",
				//leaves[l],
				//recycleBlk->profiler[leaves[l]].clk,
				//recycleBlk->profiler[leaves[l]].ticks,
				//recycleBlk->profiler[leaves[l]].wbytes,
				//recycleBlk->profiler[leaves[l]].calls,
				//recycleProfit2(leaves[l]));
#endif

	/* find entries to evict */
	mem = recyclerMemoryUsed > (lng) (MEMORY_THRESHOLD * monet_memory) ;
	vm = (int *)GDKzalloc(sizeof(int)*ltop);
	vtop = 0;

	if (!mem){	 /* evict 1 entry */
		minben = recycleProfit2(leaves[0]);
		idx = 0;
		for (l = 1; l < ltop; l++){
			ben = recycleProfit2(leaves[l]);
			if( ben < minben) {
				minben = ben;
				idx = l;
			}
		}
		vm[vtop++] = leaves[idx];
	} else {	/* evict several to get enough memory */
		wr = recyclerMemoryUsed - (lng) (MEMORY_THRESHOLD * monet_memory);
		k = 0;	
		for (l = 0; l < ltop; l++) {
			// also discard leaves that are more expensive to find then compute
			if( recycleBlk->profiler[leaves[l]].ticks < recycleSearchTime)
				continue;
			if ( recycleBlk->profiler[leaves[l]].wbytes > 0 )
				leaves[k++] = leaves[l];
		}
		if ( k > 0 )
			ltop = k;
		vtop = chooseVictims(cntxt,leaves, ltop, wr);
		wr=0;
		for (v = 0; v < vtop; v++){
			vm[v] = leaves[v];
			wr += recycleBlk->profiler[leaves[v]].wbytes;
		}
	}

#ifdef _DEBUG_CACHE_
	mnstr_printf(cntxt->fdout,"#Evicted %d instruction(s) \n",vtop);
	for(v=0; v<vtop;v++){
		mnstr_printf(cntxt->fdout,"#%d\t " LLFMT" ",vm[v],recycleBlk->profiler[vm[v]].ticks);
		printInstruction(cntxt->fdout,recycleBlk,0,recycleBlk->stmt[vm[v]], LIST_MAL_ALL);
	}
#endif

	GDKfree(leaves);
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
	/* check if a new pass of cache cleaning is needed */
	if ( recyclerMemoryUsed > (lng) (MEMORY_THRESHOLD * monet_memory) )
	goto newpass;
}

#ifdef _DEBUG_CACHE_
static void
RECYCLEsync(Client cntxt, InstrPtr p)
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
#ifdef _DEBUG_RECYCLE_
			mnstr_printf(cntxt->fdout,"#RECYCLE sync [%3d] ",i);
#else
			(void) cntxt;
#endif
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
	lng rd, wr;
	lng clk= mb->profiler[pc].clk;

	if ( recycleBlk->stop >= recycleCacheLimit)
		return ; /* no more caching */
	if ( recyclerMemoryUsed + mb->profiler[pc].wbytes > (lng) (MEMORY_THRESHOLD * monet_memory))
		return ; /* no more caching */

	rd = 0;
	for( j=p->retc;  j < p->argc; j++)
		rd += getMemoryClaim(mb,s,p,j,TRUE);
	wr = 0;
	for( j=0;j < p->retc; j++)
		wr += getMemoryClaim(mb,s,p,j,TRUE);
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

		if ( i >= p->retc){
			c = fndConstant(recycleBlk, &cst, recycleBlk->vtop);
			if (c<0)
				c = defConstant(recycleBlk, v->vtype, &cst);
		} else {
			c = newTmpVariable(recycleBlk, v->vtype);
			setVarConstant(recycleBlk, c);
			setVarFixed(recycleBlk, c);
			if (v->vtype >= 0 && ATOMextern(v->vtype))
				setVarCleanup(recycleBlk, c);
			else
				clrVarCleanup(recycleBlk, c);
				v = &getVarConstant(recycleBlk, c);
				*v = cst;
		}
		if (v->vtype == TYPE_bat)
			BBPincref( *(const int*)VALptr(v), TRUE);
		setVarUsed(recycleBlk,c);
	 	setArg(q,i,c);
	}
	pushInstruction(recycleBlk,q);
	i = recycleBlk->stop-1;
	recycleBlk->profiler[i].clk = clk; // used for LRU scheme
	recycleBlk->profiler[i].calls =1;
	recycleBlk->profiler[i].ticks = GDKusec()-clk;
	recycleBlk->profiler[i].rbytes = rd;
	recycleBlk->profiler[i].wbytes = wr;
	recyclerMemoryUsed += wr;
#ifdef _DEBUG_RECYCLE_
	mnstr_printf(cntxt->fdout,"#RECYCLE [%3d] cost "LLFMT" mem "LLFMT" ",recycleBlk->stop-1, recycleBlk->profiler[i].ticks, wr);
	printInstruction( cntxt->fdout,recycleBlk, 0, q, LIST_MAL_DEBUG);
#else
	(void) cntxt;
#endif


#ifdef _DEBUG_CACHE_
	if(0)RECYCLEsync(cntxt,q);
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
	lng clk;

	(void) mb;
	if( recycleBlk == 0)
		return -1;

	clk = GDKusec();
	(void) cntxt;
	for (i=0; i<recycleBlk->stop; i++) {
		q = getInstrPtr(recycleBlk,i);
		if (getFunctionId(p) != getFunctionId(q) ||
			getModuleId(p) != getModuleId(q) ||
			p->argc != q->argc ||
			p->retc != q->retc )
				continue;
		for (j=p->retc; j<p->argc; j++)
			if( VALcmp( &s->stk[getArg(p,j)], &getVarConstant(recycleBlk,getArg(q,j))))
				break;
		if (j == p->argc){
			recycleSearchTime += GDKusec()-clk;
			return i;
		}
	}
	recycleSearchTime += GDKusec()-clk;
	return -1;
}


#define boundcheck(flag,a) ((flag)?a<=0:a<0)
/* check if instruction p at the stack is a subset selection of the RecyclerPool instruction q 
algebra.mal:command subselect(b:bat[:oid,:any_1],                   low:any_1, high:any_1, li:bit, hi:bit, anti:bit) :bat[:oid,:oid]
algebra.mal:command subselect(b:bat[:oid,:any_1], s:bat[:oid,:oid], low:any_1, high:any_1, li:bit, hi:bit, anti:bit) :bat[:oid,:oid]
*/

static bit
selectSubsume(InstrPtr p, InstrPtr q, MalStkPtr s)
{
	int lcomp, rcomp;
	bit liq, hiq, fiq, lip, hip, fip;
	int base;
	if( p->argc >7)
		base = 3;
	else base = 2;

	fiq = *(bit*)getVarValue(recycleBlk,getArg(q,base+4));
	fip = *(const bit*)VALptr(&s->stk[getArg(p,base+4)]);
	if (fiq != fip)
		return FALSE;

	lcomp = VALcmp(&getVar(recycleBlk,getArg(q,base))->value, &s->stk[getArg(p,base)]);
	rcomp = VALcmp( &s->stk[getArg(p,base+1)], &getVar(recycleBlk,getArg(q,base+1))->value);

	liq = *(bit*)getVarValue(recycleBlk,getArg(q,base+2));
	lip = *(const bit*)VALptr(&s->stk[getArg(p,base+2)]);

	hiq = *(bit*)getVarValue(recycleBlk,getArg(q,base+3));
	hip = *(const bit*)VALptr(&s->stk[getArg(p,base+3)]);

	if( lcomp <=0 && rcomp <=0 )
		return TRUE;
	return (liq == lip && lcomp == 0) && (hiq == hip && rcomp == 0);
	//return ( boundcheck(liq || neg(lip), lcomp) && boundcheck(hiq || neg(hip), rcomp) );
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
		pc = i;

		/* found an exact match, get the results on the stack */
		for( j=0; j<p->retc; j++){
			VALcopy(&stk->stk[getArg(p,j)], &getVarConstant(recycleBlk,getArg(q,j)) );
			if (stk->stk[getArg(p,j)].vtype == TYPE_bat)
				BBPincref( stk->stk[getArg(p,j)].val.bval , TRUE);
		}
		recycleBlk->profiler[i].calls++;
		recycleBlk->profiler[i].clk = GDKusec();
		recycled++;
		recyclerSavings += recycleBlk->profiler[i].ticks;
		MT_lock_unset(&recycleLock, "recycle");
		return pc;
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
		//mnstr_printf(cntxt->fdout,"#RECYCLEreuse subselect using candidate list");
		//printInstruction(cntxt->fdout, recycleBlk, 0,getInstrPtr(recycleBlk,pc),    LIST_MAL_STMT);
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
	recycled++;
    return pc;
}

lng
RECYCLEentry(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof)
{
	int i=0;

	statements++;
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
		MT_lock_set(&recycleLock, "recycle");
		mnstr_printf(cntxt->fdout,"#REUSED  [%3d]  "LLFMT" (usec) ",i, recycleBlk->profiler[i].ticks);
		printInstruction(cntxt->fdout,recycleBlk,0,getInstrPtr(recycleBlk,i), LIST_MAL_DEBUG);
		MT_lock_unset(&recycleLock, "recycle");
	}
#endif
	return i>=0;
}

/*
 * The 'exit' instruction is called after the interpreter loop
 * itself and has to decide on the results obtained.
 * We simply look at the memory footprint to ensure it remains low
 */
void
RECYCLEexitImpl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof)
{
	lng thresh;

	if (recycleBlk == NULL || mb->profiler == NULL)
		return;

	if ( !RECYCLEinterest(p))
		return;
	MT_lock_set(&recycleLock, "recycle");
	thresh = (lng) (MEMORY_THRESHOLD * monet_memory);
	if ( (GDKmem_cursize() >  (size_t) thresh  && recyclerMemoryUsed > thresh) || recycleBlk->stop >= recycleCacheLimit)
		RECYCLEcleanCache(cntxt);

	/* infinite case, admit all new instructions */
	if (RECYCLEfind(cntxt,mb,stk,p)<0 )
		(void) RECYCLEkeep(cntxt,mb, stk, p, prof);
	MT_lock_unset(&recycleLock, "recycle");
}

void
RECYCLEexit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, RuntimeProfile prof)
{
	if (!RECYCLEinterest(p))
		return;
	RECYCLEexitImpl(cntxt,mb,stk,p,prof);
}

/*
 * At the end of session we should remove all
 * knowledge from the recycle cache.
 */
void
RECYCLEdrop(Client cntxt){
	MalBlkPtr mb = recycleBlk;
	int i;
	bte *used;

	if( recycleBlk == NULL)
		return ;

	MT_lock_set(&recycleLock, "recycle");
	used = (bte*)GDKzalloc(recycleBlk->vtop);
#ifdef _DEBUG_RECYCLE_
	if( cntxt) {
		mnstr_printf(cntxt->fdout,"#RECYCLE drop\n");
		RECYCLEdumpInternal(cntxt->fdout);
	}
#else
	(void) cntxt;
#endif

	recycleBlk = NULL;
	recycleSearchTime = 0;
	recyclerMemoryUsed = 0;
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
	
	MT_lock_set(&recycleLock, "recycle");
#ifdef _DEBUG_RESET_
	//mnstr_printf(cntxt->fdout,"#POOL BEFORE CLEANUP\n");
	//RECYCLEdumpInternal(cntxt->fdout);
#endif
	release= (char*) GDKzalloc(recycleBlk->vtop);
	vr.vtype = TYPE_str;
	vr.len  = (int) strlen(sch);
	vr.val.sval = sch;
	sid = fndConstant(recycleBlk,&vr, recycleBlk->vtop);
	vr.val.sval = tbl;
	vr.len  = (int) strlen(tbl);
	tid = fndConstant(recycleBlk,&vr, recycleBlk->vtop);
	if ( col){
		vr.val.sval = col;
		vr.len  = (int) strlen(col);
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
#ifdef _DEBUG_RESET_
	//mnstr_printf(cntxt->fdout,"#POOL AFTER CLEANUP\n");
	//RECYCLEdumpInternal(cntxt->fdout);
#endif
	MT_lock_unset(&recycleLock, "recycle");
	GDKfree(release);
	GDKfree(old);
	return MAL_SUCCEED;
}

str 
RECYCLEresetBAT(Client cntxt, int bid)
{
	int i,j, actions =0;
	char *release;
	InstrPtr *old,p;
	int limit;
	ValRecord *v;
	
	MT_lock_set(&recycleLock, "recycle");
#ifdef _DEBUG_RESET_
	//mnstr_printf(cntxt->fdout,"#POOL RESET BAT %d\n",bid);
	//RECYCLEdumpInternal(cntxt->fdout);
#endif
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
		mnstr_printf(cntxt->fdout,"#EVICT [%d]",i);
		printInstruction(cntxt->fdout,recycleBlk,0,p, LIST_MAL_DEBUG);
#endif
		for(j=0;j<p->argc;j++) {
			release[getArg(p,j)]=1;//propagate the removal request
			if (j < p->retc && isaBatType(getArgType(recycleBlk,p,j)) ){
				v = &getVarConstant(recycleBlk,getArg(p,j));
				BBPdecref(ABS(v->val.bval), TRUE);
			}
		}
		actions++;
		freeInstruction(p);
	}
#ifdef _DEBUG_RESET_
	if( actions){
		mnstr_printf(cntxt->fdout,"#POOL AFTER CLEANUP\n");
		RECYCLEdumpInternal(cntxt->fdout);
	}
#else
	(void) actions;
#endif
	MT_lock_unset(&recycleLock, "recycle");
	GDKfree(release);
	GDKfree(old);
	return MAL_SUCCEED;
}

void
RECYCLEdumpInternal(stream *s)
{
    int i;

    if (!recycleBlk) return;

    mnstr_printf(s,"#RECYCLER CATALOG cached %d instructions, ", recycleBlk->stop);
    mnstr_printf(s,"MAL recycled = "LLFMT" total MAL executed = "LLFMT" memory= "LLFMT" total searchtime="LLFMT"(usec) savings="LLFMT"\n",
         recycled- recycleBlk->stop, statements, recyclerMemoryUsed,recycleSearchTime,  recyclerSavings);

#ifdef _DEBUG_CACHE_
    /* and dump the statistics per instruction*/
	mnstr_printf(s,"# CL\t   lru\t\tcnt\t ticks\t rd\t wr\t Instr\n");
    for(i=0; i< recycleBlk->stop; i++){
        mnstr_printf(s,"#%4d\t"LLFMT"\t%d\t"LLFMT"\t"LLFMT"\t"LLFMT"\t%s\n", i,
            recycleBlk->profiler[i].clk,
            recycleBlk->profiler[i].calls,
            recycleBlk->profiler[i].ticks,
            recycleBlk->profiler[i].rbytes,
            recycleBlk->profiler[i].wbytes,
            instruction2str(recycleBlk,0,getInstrPtr(recycleBlk,i),LIST_MAL_DEBUG));
    }
#else
	(void) i;
#endif
}

void 
RECYCLEdump(stream *s)
{
	MT_lock_set(&recycleLock, "recycle");
	RECYCLEdumpInternal(s);
	MT_lock_unset(&recycleLock, "recycle");
}
