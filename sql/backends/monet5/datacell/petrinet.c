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
 * @f petrinet
 * @a Martin Kersten
 * @v 1
 * @+ Petri-net engine
 * This module is a prototype for the implementation of a
 * Petri-net interpreter for the DataCell.
 *
 * The example below accepts events at channel X
 * and the continuous query move them to Y.
 * Both query and emitter attached are scheduled.
 * @example
 * -see test directory
 * @end example
 */
#include "petrinet.h"
#include "mal_builder.h"

#define MAXPN 200			/* it is the minimum, if we need more space GDKrealloc */
#define PNcontrolInfinit 1	/* infinit loop of PNController  */
#define PNcontrolEnd 2		/* when all factories are disable PNController exits */

/*static int controlRounds = PNcontrolInfinit;*/

static MT_Lock petriLock;

typedef struct {
	char *table;
	int bskt;		/* basket used */
	BAT *b;				/* reference BAT for checking content */
	int available;      /* approximate number of events available */
	size_t lastcount;   /* statistics gathering */
	size_t consumed;
} PoolRec;

typedef struct {
	str name;
	str def;	/* query definition */
	MalBlkPtr mb;       /* Factory MAL block */
	MalStkPtr stk;      /* Factory stack */
	InstrPtr pci;       /* Factory re-entry point */
	int pc;
	int status;		/* query status waiting/running/ready */
	int delay;		/* maximum delay between calls */
	timestamp seen;	/* last executed */
	int cycles;     /* number of invocations of the factory */
	int events;		/* number of events consumed */
	str error;		/* last error seen */
	lng time;   	/* total time spent for all invocations */
	int enabled, available;
	int srctop, trgttop;
	PoolRec *source, *target;
} PNnode, *petrinode;

PNnode *pnet;

int pnettop = 0;

int *enabled;     /*array that contains the id's of all queries that are enable to fire*/

#define PNinitialize 3
#define PNrunning 2
#define PNpause 1
#define PNstopped 0

#define PNwaiting 4
#define PNscheduled 5
#define PNexecute 6
static char *statusnames[7]={"stopped","pause","running","initialize","waiting","scheduled","execute"};

static int status = PNinitialize;
static int cycleDelay = 10; /* be careful, it affects response/throughput timings */

str PNstartThread(int *ret);

str PNanalyseWrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module scope;
	Symbol s = 0;
	str nme = *(str *)getArgReference(stk, pci, 1);
	char buf[BUFSIZ], *modnme, *fcnnme;

	BSKTelements( nme, buf, &modnme, &fcnnme);
	BSKTtolower(modnme);
	BSKTtolower(fcnnme);

	(void) mb;
	scope = findModule(cntxt->nspace, putName(modnme, (int)strlen(modnme)));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme, (int)strlen(fcnnme)));
	if (s == NULL)
		throw(MAL, "petrinet.analysis", "Could not find function");

	return PNanalysis(cntxt,s->def);
}

/* A transition is only allowed when all inputs are privately used */
str PNregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module scope;
	Symbol s = 0;
	int *ret = (int *)getArgReference(stk, pci, 0);
	str nme = *(str *)getArgReference(stk, pci, 1);
	int i;
	char buf[BUFSIZ], *modnme, *fcnnme;

	BSKTelements( nme, buf, &modnme, &fcnnme);
	BSKTtolower(modnme);
	BSKTtolower(fcnnme);
	(void)mb;

	scope = findModule(cntxt->nspace, putName(modnme, (int)strlen(modnme)));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme, (int)strlen(fcnnme)));

	if (s == NULL)
		throw(MAL, "petrinet.register", "Could not find function");

	if (pnettop >= MAXPN) {
		if ((pnet = (PNnode*)GDKrealloc(pnet, sizeof(PNnode) * (pnettop + 1))) == NULL)
			throw(MAL, "petrinet.register", MAL_MALLOC_FAIL);
	}else if (pnettop == 0 && (pnet = (PNnode *)GDKzalloc(MAXPN * sizeof(PNnode))) == NULL)
			throw(MAL, "petrinet.register", MAL_MALLOC_FAIL);

	for ( i = 0; i< pnettop; i++)
		if ( strcmp(pnet[i].name, nme) == 0)
			break;
	if ( i != pnettop)
			throw(MAL, "petrinet.register", "Duplicate definition of transition");
	pnet[pnettop].name = GDKstrdup(nme);
	if ( pci->argc == 3)
		pnet[pnettop].def = GDKstrdup(*(str*) getArgReference(stk,pci,2));
	else
		pnet[pnettop].def = GDKstrdup("");

	pnet[pnettop].status = PNwaiting;
	pnet[pnettop].cycles = 0;
	/* all the rest is zero */

	(void)ret;
	pnettop++;
	return PNanalysis(cntxt,s->def);
}

str
PNremove(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str nme = *(str*) getArgReference(stk,pci,1);
    Module scope;
    Symbol s = NULL;
	int ret;
	int i,j;
	char buf[BUFSIZ], *modnme, *fcnnme;

	BSKTelements( nme, buf, &modnme, &fcnnme);
	BSKTtolower(modnme);
	BSKTtolower(fcnnme);

	if ( status != PNpause)
		throw(MAL,"datacell.remove","Scheduler should be paused first");
	(void) mb;
	/* check for a continous query */
	scope = findModule(cntxt->nspace, putName(modnme, (int)strlen(modnme)));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme, (int)strlen(fcnnme)));
	if (s == NULL)
		throw(SQL, "datacell.remove", "Continuous query found");
	PNpauseScheduler(&ret);
	for( i = j = 0; i< pnettop; i++)
		if (strcmp(nme, pnet[i].name) == 0) {
		} else
			pnet[j++] = pnet[i];
	pnettop = j;
	PNresumeScheduler(&ret);
	return MAL_SUCCEED;
}

str PNstopScheduler(int *ret)
{
	int i =0, j = pnettop;
	pnettop = 0;	/* don't look at it anymore */
	mal_set_lock(petriLock,"pncontroller");
	status = PNstopped;
	mal_unset_lock(petriLock,"pncontroller");
	i = 0;
	do {
		MT_sleep_ms(cycleDelay + 1);  /* delay to make it more tractable */
		i++;
	} while (i < 100 && status != PNinitialize  ) ;
	if ( i == 100)
		throw(MAL,"datacell.stop","reset scheduler time out");
	for( j--; j >= 0; j--)
	{
		/* reset transition */
	}
	(void)ret;
	return MAL_SUCCEED;
}

str PNresumeScheduler(int *ret)
{
	if ( status == PNrunning)
		return MAL_SUCCEED;
	if ( status == PNpause)
		status = PNrunning;
	if ( status == PNinitialize)
		return PNstartThread(ret);
	return MAL_SUCCEED;
}

str PNpauseScheduler(int *ret)
{
	if ( status == PNpause)
		return MAL_SUCCEED;
	status = PNpause;
	do
		MT_sleep_ms(cycleDelay);  /* delay to make it more tractable */
	while ( status == PNrunning);
	(void)ret;
	return MAL_SUCCEED;
}

str PNdump(int *ret)
{
	int i, k;
	mnstr_printf(PNout,"#scheduler status %s\n", statusnames[status]);
	for (i = 0; i < pnettop; i++) {
		mnstr_printf(PNout, "#[%d]\t%s %s delay %d cycles %d events %d time " LLFMT " ms\n",
			i, pnet[i].name, statusnames[pnet[i].status], pnet[i].delay, pnet[i].cycles, pnet[i].events, pnet[i].time/1000);
		if ( pnet[i].error)
			mnstr_printf(PNout,"#%s\n", pnet[i].error);
		for (k = 0; k < pnet[i].srctop; k++)
			mnstr_printf(PNout, "#<--\t%s basket[%d] " SZFMT " " SZFMT "\n",
				pnet[i].source[k].table,
				pnet[i].source[k].bskt,
				pnet[i].source[k].lastcount,
				pnet[i].source[k].consumed);
		for (k = 0; k < pnet[i].trgttop; k++)
			mnstr_printf(PNout, "#-->\t%s basket[%d] " SZFMT " " SZFMT "\n",
				pnet[i].target[k].table,
				pnet[i].source[k].bskt,
				pnet[i].target[k].lastcount,
				pnet[i].target[k].consumed);
	}
	(void)ret;
	return MAL_SUCCEED;
}
/*
 * @-
 * Make the basket group accessible to the transition function
 * The code currently relies on a physical adjacent ordering of all member
 * in the group.
 */
str PNsource(int *ret, str *fcn, str *tbl)
{
	int i, k, z;

	z = BSKTlocate(*tbl);
	for (i = 0; i < pnettop; i++) {
		if (strcmp(*fcn, pnet[i].name) == 0) {
			k = pnet[i].srctop;
			if (k >= MAXPN) {
				if ((pnet[i].source = (PoolRec*)GDKrealloc(pnet[i].source, sizeof(PoolRec) * (k + 1))) == NULL)
					throw(MAL, "petrinet.source", MAL_MALLOC_FAIL);
			}
			else if (k == 0 && (pnet[i].source = (PoolRec *)GDKzalloc(MAXPN * sizeof(PoolRec))) == NULL)
					throw(MAL, "petrinet.source", MAL_MALLOC_FAIL);

			pnet[i].source[k].table = GDKstrdup(*tbl);
			pnet[i].source[k].bskt = z;
			pnet[i].srctop++;

			/*assert(pnet[i].srctop< MAXPN);*/
		}
	}
	(void)ret;
	return MAL_SUCCEED;
}

str PNtarget(int *ret, str *fcn, str *tbl)
{
	int i, k, z;

	z = BSKTlocate(*tbl);
	for (i = 0; i < pnettop; i++) {
		if (strcmp(*fcn, pnet[i].name) == 0) {
			k = pnet[i].trgttop;
			if (k >= MAXPN) {
				if ((pnet[i].target = (PoolRec*)GDKrealloc(pnet[i].target, sizeof(PoolRec) * (k + 1))) == NULL)
					throw(MAL, "petrinet.target", MAL_MALLOC_FAIL);
			} else
			if ( k == 0 && (pnet[i].target = (PoolRec *)GDKzalloc(MAXPN * sizeof(PoolRec))) == NULL)
				throw(MAL, "petrinet.target", MAL_MALLOC_FAIL);

			pnet[i].target[k].table = GDKstrdup(*tbl);
			pnet[i].target[k].bskt = z;
			pnet[i].trgttop++;
			/*assert(pnet[i].trgttop< MAXPN);*/
		}
	}
	(void)ret;
	return MAL_SUCCEED;
}

/* check the routine for input/output relationships */
/* Make sure we do not re-use the same source more then once */
str
PNanalysis(Client cntxt, MalBlkPtr mb)
{
	int i, j, k, ret;
	InstrPtr p, sig = getInstrPtr(mb,0);
	str tbl;
	char buf[BUFSIZ], *nme= buf;

	(void) cntxt;
	snprintf(buf,BUFSIZ,"%s.%s", getModuleId(sig), getFunctionId(sig));
	/* first check for errors */
	for( i = 0; i < mb->stop; i++){
		p= getInstrPtr(mb,i);
		if ( getModuleId(p) == basketRef && getFunctionId(p) == grabRef ){
			tbl = getVarConstant(mb,getArg(p,p->argc-1)).val.sval;

			for (j = 0; j < pnettop; j++)
			for ( k = 0; k < pnet[j].srctop; k++)
			if ( strcmp(tbl, pnet[j].source[k].table) == 0 )
				throw(MAL, "datacell.register","Duplicate use of continuous query input");
		}
	}
	for( i = 0; i < mb->stop; i++){
		p= getInstrPtr(mb,i);
		if ( getModuleId(p) == basketRef &&  getFunctionId(p) == grabRef ){
			tbl = getVarConstant(mb,getArg(p,p->argc-1)).val.sval;
			PNsource(&ret, &nme , &tbl);
		}
		if ( getModuleId(p) == basketRef &&  getFunctionId(p) == putName("pass",4) ){
			tbl = getVarConstant(mb,getArg(p,p->retc)).val.sval;
			mnstr_printf(cntxt->fdout,"#output basket %s \n", tbl);
			PNtarget(&ret, &nme, &tbl);
		}
	}
	return MAL_SUCCEED;
}
/*
 * @-
 * The PetriNet controller lives in an separate thread.
 * It cycles through the nodes, hunting for non-empty baskets
 * and transformations that can fire.
 * The current policy is a simple round-robin. Later we will
 * experiment with more advanced schemes, e.g., priority queues.
 *
 * During each step cycle we first enable the transformations.
 * Then an optional scheduler may decide on the priority
 * of the factory activation.
 * All sources
 */
static void
PNcontroller(void *dummy)
{
	int idx, i, j, cnt = 0;
	Symbol s;
	InstrPtr p;
	MalStkPtr glb;
	MalBlkPtr mb;
	Client cntxt;
	int k = -1;
	int m = 0;
	str msg;
	lng t, analysis, now;

	cntxt = mal_clients; /* run as admin */
	SQLinitEnvironment(cntxt);
	/* At this point we know what is the total number of factories.
	 * The most extremely case is when ALL factories are enable to fire
	 * so the maximum space we could ever need is = #factories (=pnettop)*/

	if ((enabled = (int *)GDKzalloc(MAXPN * sizeof(int))) == NULL) {
		mnstr_printf(cntxt->fdout, "#Petrinet Controller is unable to allocate more memory!\n");
		return;
	}

	/* create the lock */
	MT_lock_init(&petriLock,"petrinet");

	/* create a fake procedure to highlight the continuous queries */
	s = newFunction("user", "pnController", FACTORYsymbol);
	p = getSignature(s);
	getArg(p, 0) = newTmpVariable(mb = s->def, TYPE_void);
	/* create an execution environment for all transitions */
	for (i = 0; i < pnettop; i++) {
		char buf[BUFSIZ], *modnme, *fcnnme;

		BSKTelements( pnet[i].name, buf, &modnme, &fcnnme);
		BSKTtolower(modnme);
		BSKTtolower(fcnnme);
		p = newFcnCall(mb, modnme,fcnnme);
		pnet[i].pc = getPC(mb, p);
	}
	pushEndInstruction(mb);
	/*printf("\n1 mb->vtop:%d\n",mb->vtop);*/
	chkProgram(cntxt->nspace, mb);
	if (mb->errors) {
		mnstr_printf(cntxt->fdout, "#Petrinet Controller found errors\n");
		return;
	}
	newStack(glb, mb->vtop);
	memset((char *)glb, 0, stackSize(mb->vtop));
	glb->stktop = mb->vtop;
	glb->blk = mb;
#ifdef _DEBUG_PETRINET_
	printFunction(cntxt->fdout, mb, 0, LIST_MAL_ALL);
#endif
	status = PNrunning;
	do {
		if ( cycleDelay)
			MT_sleep_ms(cycleDelay);  /* delay to make it more tractable */
		while (status == PNpause  ) ;
		mal_set_lock(petriLock,"pncontroller");
			if ( status != PNstopped)
			/* collect latest statistics, note that we don't need a lock here,
			   because the count need not be accurate to the usec. It will simply
			   come back. We also only have to check the sources that are marked
			   empty. */
				status = PNrunning;
		mal_unset_lock(petriLock,"pncontroller");
		now = GDKusec();
		for (k = i = 0; status == PNrunning && i < pnettop; i++) {
			pnet[i].available = 0;
			pnet[i].enabled = 0;
			for (j = 0; j < pnet[i].srctop; j++) {
				idx = pnet[i].source[j].bskt;
				pnet[i].source[j].b = baskets[idx].primary[0];
				if (pnet[i].source[j].b == 0) { /* we lost the BAT */
					pnet[i].enabled = 0;
					break;
				}
				pnet[i].source[j].available = cnt = (int)BATcount(pnet[i].source[j].b);
				if (cnt) {
					timestamp ts, tn;
					/* only look at large enough baskets */
					if ( cnt <  baskets[idx].threshold) {
						pnet[i].enabled = 0;
						break;
					}
					/* check heart beat delays */
					if ( baskets[idx].beat) {
						(void) MTIMEunix_epoch(&ts);
						(void) MTIMEtimestamp_add(&tn, &baskets[idx].seen, &baskets[idx].beat);
						if(  tn.days < ts.days || (tn.days == ts.days && tn.msecs < ts.msecs) ){
							pnet[i].enabled = 0;
							break;
						}
					}
#ifdef _DEBUG_PETRINET_
					mnstr_printf(cntxt->fdout, "#PETRINET:%d tuples for %s, source %d\n", cnt, pnet[i].name, j);
#endif
					pnet[i].available += cnt;
					pnet[i].enabled++;
				} else {
#ifdef _DEBUG_PETRINET_
					mnstr_printf(cntxt->fdout, "#PETRINET:no tuples for %s, source %d\n",  pnet[i].name, j);
#endif
					/*stop checking if the rest input BATs does not contain elements */
					pnet[i].enabled = 0;
					break;
				}
			}
			if (pnet[i].enabled == pnet[i].srctop)
				/*save the ids of all continuous queries that can be executed */
				enabled[k++] = i;
		}
		analysis = GDKusec()-now;

		/* execute each enabled transformation */
		/* We don't need to access again all the factories and check again which are available to execute them
		 * we have already kept the enable ones in the enabled list (created in the previous loop)
		 * and now it is enough to access that list*/
		for (m = 0; m < k; m++) {
			i = enabled[m];
			if (pnet[i].srctop == pnet[i].enabled && pnet[i].available > 0) {
#ifdef _DEBUG_PETRINET_
				mnstr_printf(cntxt->fdout, "#Run transition %s pc %d\n", pnet[i].name, pnet[i].pc);
#endif
#ifdef _BASKET_SIZE_
				mnstr_printf(cntxt->fdout, "\npnet[%d].srctop:%d\n", i, pnet[i].srctop);
				mnstr_printf(cntxt->fdout, "Function: %s basket size %d\n", pnet[i].name, pnet[i].source[0].available);
#endif

				t= GDKusec();
				msg = reenterMAL(cntxt, mb, pnet[i].pc, pnet[i].pc + 1, glb, 0, 0);
				pnet[i].time += GDKusec() - t + analysis;	/* keep around in microseconds */
				if ( msg != MAL_SUCCEED && !strstr(msg,"too early") ){
					char buf[BUFSIZ];
					if ( pnet[i].error == NULL ) {
						snprintf(buf,BUFSIZ-1,"Query %s failed:%s", pnet[i].name, msg);
						pnet[i].error = GDKstrdup(buf);
					} else GDKfree(msg);
					pnet[i].enabled = -1;
				} else {
					pnet[i].cycles++;
					(void) MTIMEcurrent_timestamp(&pnet[i].seen);
					for (j = 0; j < pnet[i].srctop; j++) {
						idx = pnet[i].source[j].bskt;
						(void) MTIMEcurrent_timestamp(&baskets[idx].seen);
						pnet[i].events += pnet[i].source[j].available;
						pnet[i].source[j].available = 0;  /* force recount */
					}
				}
			}
		}
	} while (status != PNstopped);
	status = PNinitialize;
	(void)dummy;
}

str PNstartThread(int *ret)
{
	MT_Id pid;
	int s;
#ifdef _DEBUG_PETRINET_
	PNdump(&s);
#endif

	if (MT_create_thread(&pid, PNcontroller, &s, MT_THR_DETACHED) != 0)
		throw(MAL, "petrinet.startThread", "Process creation failed");

	(void)ret;
	return MAL_SUCCEED;
}

str PNstart(int *ret)
{
	int s;
#ifdef _DEBUG_PETRINET_
	PNdump(&s);
	printf("\npnettop=%d\n", pnettop);
#endif
	mal_set_lock(petriLock,"pncontroller");
	if ( status != PNstopped )
		status = PNrunning;
	mal_unset_lock(petriLock,"pncontroller");
	/*controlRounds = PNcontrolEnd;*/

	PNcontroller(&s);

	(void)ret;
	return MAL_SUCCEED;
}

/* inspection  routines */
str
PNtable(int *ret)
{
	BAT *bn = NULL, *name = NULL, *def = NULL, *status = NULL, *seen = NULL, *cycles = NULL, *events = NULL, *time = NULL, *error = NULL;
	int i;

	bn = BATnew(TYPE_str, TYPE_bat, BATTINY);
	if ( bn == 0)
		throw(MAL,"dictionary.baskets",MAL_MALLOC_FAIL);

	name = BATnew(TYPE_oid,TYPE_str, BATTINY);
	if ( name == 0 ) goto wrapup;
	BATseqbase(name,0);
	def = BATnew(TYPE_oid,TYPE_str, BATTINY);
	if ( def == 0 ) goto wrapup;
	BATseqbase(def,0);
	status = BATnew(TYPE_oid,TYPE_str, BATTINY);
	if ( status == 0 ) goto wrapup;
	BATseqbase(status,0);
	seen = BATnew(TYPE_oid,TYPE_timestamp, BATTINY);
	if ( seen == 0 ) goto wrapup;
	BATseqbase(seen,0);
	cycles = BATnew(TYPE_oid,TYPE_int, BATTINY);
	if ( cycles == 0 ) goto wrapup;
	BATseqbase(cycles,0);
	events = BATnew(TYPE_oid,TYPE_int, BATTINY);
	if ( events == 0 ) goto wrapup;
	BATseqbase(events,0);
	time = BATnew(TYPE_oid,TYPE_lng, BATTINY);
	if ( time == 0 ) goto wrapup;
	BATseqbase(time,0);
	error = BATnew(TYPE_oid,TYPE_str, BATTINY);
	if ( error == 0 ) goto wrapup;
	BATseqbase(error,0);

	for ( i =0; i < pnettop; i++) {
		BUNappend(name, pnet[i].name, FALSE);
		BUNappend(def, pnet[i].def, FALSE);
		BUNappend(status, statusnames[pnet[i].status], FALSE);
		BUNappend(seen, &pnet[i].seen, FALSE);
		BUNappend(cycles, &pnet[i].cycles, FALSE);
		BUNappend(events, &pnet[i].events, FALSE);
		BUNappend(time, &pnet[i].time, FALSE);
		BUNappend(error, (pnet[i].error ? pnet[i].error:""), FALSE);
	}
	BUNins(bn,"nme", & name->batCacheid, FALSE);
	BUNins(bn,"status", & status->batCacheid, FALSE);
	BUNins(bn,"seen", & seen->batCacheid, FALSE);
	BUNins(bn,"cycles", & cycles->batCacheid, FALSE);
	BUNins(bn,"events", & events->batCacheid, FALSE);
	BUNins(bn,"time", & time->batCacheid, FALSE);
	BUNins(bn,"error", & error->batCacheid, FALSE);
	BUNins(bn,"def", & def->batCacheid, FALSE);

	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	BBPreleaseref(name->batCacheid);
	BBPreleaseref(def->batCacheid);
	BBPreleaseref(status->batCacheid);
	BBPreleaseref(seen->batCacheid);
	BBPreleaseref(cycles->batCacheid);
	BBPreleaseref(events->batCacheid);
	BBPreleaseref(time->batCacheid);
	BBPreleaseref(error->batCacheid);
	return MAL_SUCCEED;
wrapup:
	if ( bn) BBPreleaseref(bn->batCacheid);
	if ( name) BBPreleaseref(name->batCacheid);
	if ( def) BBPreleaseref(def->batCacheid);
	if ( status) BBPreleaseref(status->batCacheid);
	if ( seen) BBPreleaseref(seen->batCacheid);
	if ( cycles) BBPreleaseref(cycles->batCacheid);
	if ( events) BBPreleaseref(events->batCacheid);
	if ( time) BBPreleaseref(time->batCacheid);
	if ( error) BBPreleaseref(error->batCacheid);
	throw(MAL,"datacell.queries",MAL_MALLOC_FAIL);
}
