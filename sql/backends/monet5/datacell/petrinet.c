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

/*
 * Martin Kersten
 * @+ Petri-net engine
   The Datacell scheduler is based on the long-standing and mature Petri-net technology. For completeness, we
   recap its salient points taken from Wikipedia. For more detailed information look at the science library.

   A Petri net (also known as a place/transition net or P/T net) is one of several mathematical modeling
   languages for the description of distributed systems. A Petri net is a directed bipartite graph,
   in which the nodes represent transitions (i.e. events that may occur, signified by bars) and
   places (i.e. conditions, signified by circles). The directed arcs describe which places are pre-
   and/or postconditions for which transitions (signified by arrows).
   Some sources state that Petri nets were invented in August 1939 by Carl Adam Petri –
   at the age of 13 – for the purpose of describing chemical processes.

   Like industry standards such as UML activity diagrams, BPMN and EPCs, Petri nets offer a
   graphical notation for stepwise processes that include choice, iteration, and concurrent execution.
   Unlike these standards, Petri nets have an exact mathematical definition of their execution semantics,
   with a well-developed mathematical theory for process analysis.

   A Petri net consists of places, transitions, and directed arcs. Arcs run from a place to a transition or vice versa,
   never between places or between transitions. The places from which an arc runs to a transition are called the input
   places of the transition; the places to which arcs run from a transition are called the output places of the transition.

   Places may contain a natural number of tokens. A distribution of tokens over the places of a net is called a marking.
   A transition of a Petri net may fire whenever there is a token at the start of all input arcs; when it fires,
   it consumes these tokens, and places tokens at the end of all output arcs. A firing is atomic, i.e., a single non-interruptible step.

   Execution of Petri nets is nondeterministic: when multiple transitions are enabled at the same time,
   any one of them may fire. If a transition is enabled, it may fire, but it doesn't have to.

   Since firing is nondeterministic, and multiple tokens may be present anywhere in the net (even in the same place), Petri nets are well suited for modeling the concurrent behavior of distributed systems.

   The Datacell scheduler is a fair implementation of a Petri-net interpreter. It models all continuous queries as transitions,
   and the baskets as the places. The events are equivalent to tokens. Unlike the pure Petri-net model, all tokens in a place
   are taken out on each firing. They may result into placing multiple tokens into receiving baskets.

   The scheduling amongst the transistions is currently deterministic. Upon each round of the scheduler, it determines all
   transitions eligble to fire, i.e. have non-empty baskets, which are then actived one after the other. Future implementations
   may relax this rigid scheme using a parallel implementation of the scheduler, such that each transition by itself can
   decide to fire. However, when resources are limited to handle all complex continuous queries, it may pay of to invest
   into a domain specif scheduler.

   For example, in the EMILI case, we may want to give priority to fire transistions based on the sensor type (is there fire)
   or detection of emergency trends (the heat increases beyong model-based prediction). The software structure where to
   inject this domain specific code is well identified and relatively easy to extend.

   The current implementation is limited to a fixed number of transitions. The scheduler can be stopped and restarted
   at any time. Even selectively for specific baskets. This provides the handle to debug a system before being deployed.
   In general, event processing through multiple layers of continous queries is too fast to trace them one by one.
   Some general statistics about number of events handled per transition is maintained, as well as the processing time
   for each continous query step. This provides the information to re-design the event handling system.
 */

#include "monetdb_config.h"
#include "petrinet.h"
#include "mal_builder.h"
#include "opt_prelude.h"

#define MAXPN 200           /* it is the minimum, if we need more space GDKrealloc */
#define PNcontrolInfinit 1  /* infinit loop of PNController  */
#define PNcontrolEnd 2      /* when all factories are disable PNController exits */

/* #define _DEBUG_PETRINET_ */

/*static int controlRounds = PNcontrolInfinit;*/

typedef struct {
	char *table;
	int bskt;       /* basket used */
	BAT *b;             /* reference BAT for checking content */
	int available;      /* approximate number of events available */
	size_t lastcount;   /* statistics gathering */
	size_t consumed;
} PoolRec;

typedef struct {
	str name;
	str def;    /* query definition */
	MalBlkPtr mb;       /* Factory MAL block */
	MalStkPtr stk;      /* Factory stack */
	InstrPtr pci;       /* Factory re-entry point */
	int pc;
	int status;     /* query status waiting/running/ready */
	int delay;      /* maximum delay between calls */
	timestamp seen; /* last executed */
	int cycles;     /* number of invocations of the factory */
	int events;     /* number of events consumed */
	str error;      /* last error seen */
	lng time;       /* total time spent for all invocations */
	int enabled, available;
	int srctop, trgttop;
	PoolRec *source, *target;
} PNnode, *petrinode;

PNnode *pnet;

int pnettop = 0;

int *enabled;     /*array that contains the id's of all queries that are enable to fire*/

static int status = BSKTINIT;
static int cycleDelay = 1; /* be careful, it affects response/throughput timings */

str PNstartThread(int *ret);

str PNanalyseWrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module scope;
	Symbol s = 0;
	str nme = *(str *) getArgReference(stk, pci, 1);
	char buf[BUFSIZ], *modnme, *fcnnme;

	BSKTelements(nme, buf, &modnme, &fcnnme);
	BSKTtolower(modnme);
	BSKTtolower(fcnnme);

	(void) mb;
	scope = findModule(cntxt->nspace, putName(modnme, (int) strlen(modnme)));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme, (int) strlen(fcnnme)));
	if (s == NULL)
		throw(MAL, "petrinet.analysis", "Could not find function");

	return PNanalysis(cntxt, s->def);
}

/* A transition is only allowed when all inputs are privately used */
str PNregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module scope;
	Symbol s = 0;
	int *ret = (int *) getArgReference(stk, pci, 0);
	str nme = *(str *) getArgReference(stk, pci, 1);
	int i;
	str msg= MAL_SUCCEED;
	char buf[BUFSIZ], *modnme, *fcnnme;

	BSKTelements(nme, buf, &modnme, &fcnnme);
	BSKTtolower(modnme);
	BSKTtolower(fcnnme);
	(void) mb;

	scope = findModule(cntxt->nspace, putName(modnme, (int) strlen(modnme)));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme, (int) strlen(fcnnme)));

	if (s == NULL)
		throw(MAL, "petrinet.register", "Could not find function");

	if (pnettop >= MAXPN) {
		if ((pnet = (PNnode *) GDKrealloc(pnet, sizeof(PNnode) * (pnettop + 1))) == NULL)
			throw(MAL, "petrinet.register", MAL_MALLOC_FAIL);
	} else if (pnettop == 0 && (pnet = (PNnode *) GDKzalloc(MAXPN * sizeof(PNnode))) == NULL)
		throw(MAL, "petrinet.register", MAL_MALLOC_FAIL);

	for (i = 0; i < pnettop; i++)
		if (strcmp(pnet[i].name, nme) == 0)
			break;
	if (i != pnettop)
		throw(MAL, "petrinet.register", "Duplicate definition of transition");
	pnet[pnettop].name = GDKstrdup(nme);
	if (pci->argc == 3)
		pnet[pnettop].def = GDKstrdup(*(str *) getArgReference(stk, pci, 2));
	else
		pnet[pnettop].def = GDKstrdup("see procedure definition");

	pnet[pnettop].status = BSKTPAUSE;
	pnet[pnettop].cycles = 0;
	pnet[pnettop].seen = *timestamp_nil;
	/* all the rest is zero */

	(void) ret;
	pnettop++;
	msg = PNanalysis(cntxt, s->def);
	/* start the scheduler if analysis does not show errors */
	if ( msg == MAL_SUCCEED )
		return PNstartThread(ret);
	return msg;
}

str
PNpauseQuery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	str qry= *(str*) getArgReference(stk,pci,1);
	int i;
	char buf[BUFSIZ];

	(void) cntxt;
	(void) mb;
	for ( i = 0; i < pnettop; i++)
	if ( strcmp(qry, pnet[i].name) == 0){
		/* stop the query first */
		pnet[i].status = BSKTPAUSE;
		return MAL_SUCCEED;
	}
	snprintf(buf,BUFSIZ,"datacell.%s", qry);
	for ( i = 0; i < pnettop; i++)
	if ( strcmp(buf, pnet[i].name) == 0){
		/* stop the query first */
		pnet[i].status = BSKTPAUSE;
		return MAL_SUCCEED;
	}
	if (pnettop)
		throw(SQL,"datacell.pause","Basket or query not found");
	return MAL_SUCCEED;
}

str
PNresumeQuery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	str qry= *(str*) getArgReference(stk,pci,1);
	int i;
	char buf[BUFSIZ];

	(void) cntxt;
	(void) mb;
	for ( i = 0; i < pnettop; i++)
	if ( strcmp(qry, pnet[i].name) == 0){
		/* stop the query first */
		pnet[i].status = BSKTRUNNING;
		return MAL_SUCCEED;
	}
	snprintf(buf,BUFSIZ,"datacell.%s", qry);
	for ( i = 0; i < pnettop; i++)
	if ( strcmp(buf, pnet[i].name) == 0){
		/* stop the query first */
		pnet[i].status = BSKTRUNNING;
		return MAL_SUCCEED;
	}
	throw(SQL,"datacell.pause","Basket or query not found");
}

#if 0
static str
PNremove(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str nme = *(str *) getArgReference(stk, pci, 1);
	Module scope;
	Symbol s = NULL;
	int ret;
	int i, j;
	char buf[BUFSIZ], *modnme, *fcnnme;

	BSKTelements(nme, buf, &modnme, &fcnnme);
	BSKTtolower(modnme);
	BSKTtolower(fcnnme);

	if (status != BSKTPAUSE)
		throw(MAL, "datacell.remove", "Scheduler should be paused first");
	(void) mb;
	/* check for a continous query */
	scope = findModule(cntxt->nspace, putName(modnme, (int) strlen(modnme)));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme, (int) strlen(fcnnme)));
	if (s == NULL)
		throw(SQL, "datacell.remove", "Continuous query found");
	BSKTPAUSEScheduler(&ret);
	for (i = j = 0; i < pnettop; i++)
		if (strcmp(nme, pnet[i].name) == 0) {} else
			pnet[j++] = pnet[i];
	pnettop = j;
	PNresumeScheduler(&ret);
	return MAL_SUCCEED;
}
#endif

str PNstopScheduler(int *ret)
{
	int i = 0, j = pnettop;
	pnettop = 0;    /* don't look at it anymore */
	MT_lock_set(&dcLock, "pncontroller");
	status = BSKTSTOP;
	MT_lock_unset(&dcLock, "pncontroller");
	i = 0;
	do {
		MT_sleep_ms(cycleDelay + 1);  /* delay to make it more tractable */
		i++;
	} while (i < 100 && status != BSKTINIT);
	if (i == 100)
		throw(MAL, "datacell.stop", "reset scheduler time out");
	for (j--; j >= 0; j--) {
		/* reset transition */
	}
	(void) ret;
	return MAL_SUCCEED;
}

str PNresumeScheduler(int *ret)
{
	int i;

	for( i =0; i< pnettop; i++)
		pnet[i].status = BSKTRUNNING;
	MT_lock_set(&dcLock, "pncontroller");
	status = BSKTRUNNING;
	MT_lock_unset(&dcLock, "pncontroller");
	(void) ret;
	return MAL_SUCCEED;
}

str PNpauseScheduler(int *ret)
{
	int i;

	for( i =0; i< pnettop; i++)
		pnet[i].status = BSKTPAUSE;
	MT_lock_set(&dcLock, "pncontroller");
	status = BSKTPAUSE;
	MT_lock_unset(&dcLock, "pncontroller");
	(void) ret;
	return MAL_SUCCEED;
}

str PNdump(int *ret)
{
	int i, k;
	mnstr_printf(PNout, "#scheduler status %s\n", statusname[status]);
	for (i = 0; i < pnettop; i++) {
		mnstr_printf(PNout, "#[%d]\t%s %s delay %d cycles %d events %d time " LLFMT " ms\n",
				i, pnet[i].name, statusname[pnet[i].status], pnet[i].delay, pnet[i].cycles, pnet[i].events, pnet[i].time / 1000);
		if (pnet[i].error)
			mnstr_printf(PNout, "#%s\n", pnet[i].error);
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
	(void) ret;
	return MAL_SUCCEED;
}
/*
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
				if ((pnet[i].source = (PoolRec *) GDKrealloc(pnet[i].source, sizeof(PoolRec) * (k + 1))) == NULL)
					throw(MAL, "petrinet.source", MAL_MALLOC_FAIL);
			} else if (k == 0 && (pnet[i].source = (PoolRec *) GDKzalloc(MAXPN * sizeof(PoolRec))) == NULL)
				throw(MAL, "petrinet.source", MAL_MALLOC_FAIL);

			pnet[i].source[k].table = GDKstrdup(*tbl);
			pnet[i].source[k].bskt = z;
			pnet[i].srctop++;

			/*assert(pnet[i].srctop< MAXPN);*/
		}
	}
	(void) ret;
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
				if ((pnet[i].target = (PoolRec *) GDKrealloc(pnet[i].target, sizeof(PoolRec) * (k + 1))) == NULL)
					throw(MAL, "petrinet.target", MAL_MALLOC_FAIL);
			} else if (k == 0 && (pnet[i].target = (PoolRec *) GDKzalloc(MAXPN * sizeof(PoolRec))) == NULL)
				throw(MAL, "petrinet.target", MAL_MALLOC_FAIL);

			pnet[i].target[k].table = GDKstrdup(*tbl);
			pnet[i].target[k].bskt = z;
			pnet[i].trgttop++;
			/*assert(pnet[i].trgttop< MAXPN);*/
		}
	}
	(void) ret;
	return MAL_SUCCEED;
}

/* check the routine for input/output relationships */
/* Make sure we do not re-use the same source more than once */
str
PNanalysis(Client cntxt, MalBlkPtr mb)
{
	int i, j, k, ret;
	InstrPtr p, sig = getInstrPtr(mb, 0);
	str tbl;
	char buf[BUFSIZ], *nme = buf;

	(void) cntxt;
	snprintf(buf, BUFSIZ, "%s.%s", getModuleId(sig), getFunctionId(sig));
	/* first check for errors */
	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == basketRef && getFunctionId(p) == grabRef) {
			tbl = getVarConstant(mb, getArg(p, p->argc - 1)).val.sval;

			for (j = 0; j < pnettop; j++)
				for (k = 0; k < pnet[j].srctop; k++)
					if (strcmp(tbl, pnet[j].source[k].table) == 0)
						throw(MAL, "datacell.register", "Duplicate use of continuous query input");
		}
	}
	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == basketRef && getFunctionId(p) == grabRef) {
			tbl = getVarConstant(mb, getArg(p, p->argc - 1)).val.sval;
			PNsource(&ret, &nme, &tbl);
		}
		if (getModuleId(p) == basketRef && getFunctionId(p) == putName("pass", 4)) {
			tbl = getVarConstant(mb, getArg(p, p->retc)).val.sval;
			mnstr_printf(cntxt->fdout, "#output basket %s \n", tbl);
			PNtarget(&ret, &nme, &tbl);
		}
	}
	return MAL_SUCCEED;
}
/*
 * The PetriNet controller lives in an separate thread.
 * It cycles through the nodes, hunting for non-empty baskets
 * and non-paused queries that can fire.
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
	int idx = -1, i, j, cnt = 0;
	Symbol s;
	InstrPtr p;
	MalStkPtr glb;
	MalBlkPtr mb;
	Client cntxt;
	int k = -1;
	int m = 0, abortpc=0;
	str msg;
	lng t, analysis, now;
	char buf[BUFSIZ], *modnme, *fcnnme;

	cntxt = mal_clients; /* run as admin */
	if ( !SQLinitialized)
		SQLinitEnvironment(cntxt);
	/* At this point we know what is the total number of factories.
	 * The most extremely case is when ALL factories are enable to fire
	 * so the maximum space we could ever need is = #factories (=pnettop)*/

	if ((enabled = (int *) GDKzalloc(MAXPN * sizeof(int))) == NULL) {
		mnstr_printf(cntxt->fdout, "#Petrinet Controller is unable to allocate more memory!\n");
		return;
	}

	/* create a fake procedure to highlight the continuous queries */
	s = newFunction(userRef, GDKstrdup("pnController"), FUNCTIONsymbol);
	mb= s->def;
	p = getSignature(s);
	getArg(p, 0) = newTmpVariable(mb, TYPE_void);
reinit:
	MT_lock_set(&dcLock, "pncontroller");
	status = BSKTRUNNING;
	mb->stop = 1;
	/* create an execution environment for all transitions */
	for (i = 0; i < pnettop; i++) {
		BSKTelements(pnet[i].name, buf, &modnme, &fcnnme);
		BSKTtolower(modnme);
		BSKTtolower(fcnnme);
		p = newFcnCall(mb, modnme, fcnnme);
		pnet[i].pc = getPC(mb, p);
	}
	p= newFcnCall(mb, sqlRef, abortRef);
	abortpc = getPC(mb,p);
	pushEndInstruction(mb);
	/*printf("\n1 mb->vtop:%d\n",mb->vtop);*/
	chkProgram(cntxt->fdout, cntxt->nspace, mb);
	MT_lock_unset(&dcLock, "pncontroller");
	if (mb->errors) {
		printFunction(cntxt->fdout, mb, 0, LIST_MAL_ALL);
		mnstr_printf(cntxt->fdout, "#Petrinet Controller found errors\n");
		return;
	}
	newStack(glb, mb->vtop);
	memset((char *) glb, 0, stackSize(mb->vtop));
	glb->stktop = mb->vtop;
	glb->blk = mb;
#ifdef _DEBUG_PETRINET_
	printFunction(cntxt->fdout, mb, 0, LIST_MAL_ALL);
#endif
	while( status != BSKTSTOP){
		if (cycleDelay)
			MT_sleep_ms(cycleDelay);  /* delay to make it more tractable */
		while (status == BSKTPAUSE)
			;
		if ( mb->stop  < pnettop + 2)
			goto reinit;	/* new query arrived */
		/* collect latest statistics, note that we don't need a lock here,
		   because the count need not be accurate to the usec. It will simply
		   come back. We also only have to check the sources that are marked
		   empty. */
		now = GDKusec();
		for (k = i = 0; status == BSKTRUNNING && i < pnettop; i++) 
		if ( pnet[i].status != BSKTPAUSE ){
			pnet[i].available = 0;
			pnet[i].enabled = 0;
			for (j = 0; j < pnet[i].srctop; j++) {
				idx = pnet[i].source[j].bskt;
				pnet[i].source[j].b = baskets[idx].primary[0];
				if (pnet[i].source[j].b == 0) { /* we lost the BAT */
					pnet[i].enabled = 0;
					break;
				}
				pnet[i].source[j].available = cnt = (int) BATcount(pnet[i].source[j].b);
				if (cnt) {
					timestamp ts, tn;
					/* only look at large enough baskets */
					if (cnt < baskets[idx].threshold) {
						pnet[i].enabled = 0;
						break;
					}
					/* check heart beat delays */
					if (baskets[idx].beat) {
						(void) MTIMEunix_epoch(&ts);
						(void) MTIMEtimestamp_add(&tn, &baskets[idx].seen, &baskets[idx].beat);
						if (tn.days < ts.days || (tn.days == ts.days && tn.msecs < ts.msecs)) {
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
					/*stop checking if the rest input BATs does not contain elements */
					pnet[i].enabled = 0;
					break;
				}
			}
			if (pnet[i].enabled == pnet[i].srctop)
				/*save the ids of all continuous queries that can be executed */
				enabled[k++] = i;
		}
		analysis = GDKusec() - now;

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

				(void) MTIMEcurrent_timestamp(&baskets[idx].seen);
				t = GDKusec();
				pnet[i].cycles++;
				msg = reenterMAL(cntxt, mb, pnet[i].pc, pnet[i].pc + 1, glb);
				pnet[i].time += GDKusec() - t + analysis;   /* keep around in microseconds */
				if (msg != MAL_SUCCEED && !strstr(msg, "too early")) {
					char buf[BUFSIZ];
					if (pnet[i].error == NULL) {
						snprintf(buf, BUFSIZ - 1, "Query %s failed:%s", pnet[i].name, msg);
						pnet[i].error = GDKstrdup(buf);
					} else
						GDKfree(msg);
					pnet[i].enabled = -1;
					/* abort current transaction  */
					if ( abortpc )
						msg = reenterMAL(cntxt, mb, abortpc, abortpc + 1, glb);
				} else {
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
	}
	MT_lock_set(&dcLock, "pncontroller");
	status = BSKTINIT;
	MT_lock_unset(&dcLock, "pncontroller");
	(void) dummy;
}

str PNstartThread(int *ret)
{
	MT_Id pid;
	int s;
#ifdef _DEBUG_PETRINET_
	PNdump(&s);
#endif

	if (status== BSKTINIT && MT_create_thread(&pid, PNcontroller, &s, MT_THR_DETACHED) != 0)
		throw(MAL, "petrinet.startThread", "Process creation failed");

	(void) ret;
	return MAL_SUCCEED;
}

#if 0
static str PNstart(int *ret)
{
	int s;
#ifdef _DEBUG_PETRINET_
	PNdump(&s);
#endif
	MT_lock_set(&dcLock, "pncontroller");
	if (status != BSKTSTOP)
		status = BSKTRUNNING;
	MT_lock_unset(&dcLock, "pncontroller");
	/*controlRounds = PNcontrolEnd;*/

	PNcontroller(&s);

	(void) ret;
	return MAL_SUCCEED;
}
#endif

/* inspection  routines */
str
PNtable(int *nameId, int *statusId, int *seenId, int *cyclesId, int *eventsId, int *timeId, int * errorId, int *defId)
{
	BAT *name = NULL, *def = NULL, *status = NULL, *seen = NULL, *cycles = NULL, *events = NULL, *time = NULL, *error = NULL;
	int i;

	name = BATnew(TYPE_void, TYPE_str, BATTINY);
	if (name == 0)
		goto wrapup;
	BATseqbase(name, 0);
	def = BATnew(TYPE_void, TYPE_str, BATTINY);
	if (def == 0)
		goto wrapup;
	BATseqbase(def, 0);
	status = BATnew(TYPE_void, TYPE_str, BATTINY);
	if (status == 0)
		goto wrapup;
	BATseqbase(status, 0);
	seen = BATnew(TYPE_void, TYPE_timestamp, BATTINY);
	if (seen == 0)
		goto wrapup;
	BATseqbase(seen, 0);
	cycles = BATnew(TYPE_void, TYPE_int, BATTINY);
	if (cycles == 0)
		goto wrapup;
	BATseqbase(cycles, 0);
	events = BATnew(TYPE_void, TYPE_int, BATTINY);
	if (events == 0)
		goto wrapup;
	BATseqbase(events, 0);
	time = BATnew(TYPE_void, TYPE_lng, BATTINY);
	if (time == 0)
		goto wrapup;
	BATseqbase(time, 0);
	error = BATnew(TYPE_void, TYPE_str, BATTINY);
	if (error == 0)
		goto wrapup;
	BATseqbase(error, 0);

	for (i = 0; i < pnettop; i++) {
		BUNappend(name, pnet[i].name, FALSE);
		BUNappend(def, pnet[i].def, FALSE);
		BUNappend(status, statusname[pnet[i].status], FALSE);
		BUNappend(seen, &pnet[i].seen, FALSE);
		BUNappend(cycles, &pnet[i].cycles, FALSE);
		BUNappend(events, &pnet[i].events, FALSE);
		BUNappend(time, &pnet[i].time, FALSE);
		BUNappend(error, (pnet[i].error ? pnet[i].error : ""), FALSE);
	}
	BBPkeepref(*nameId = name->batCacheid);
	BBPkeepref(*defId = def->batCacheid);
	BBPkeepref(*statusId = status->batCacheid);
	BBPkeepref(*seenId = seen->batCacheid);
	BBPkeepref(*cyclesId = cycles->batCacheid);
	BBPkeepref(*eventsId = events->batCacheid);
	BBPkeepref(*timeId = time->batCacheid);
	BBPkeepref(*errorId = error->batCacheid);
	return MAL_SUCCEED;
wrapup:
	if (name)
		BBPreleaseref(name->batCacheid);
	if (def)
		BBPreleaseref(def->batCacheid);
	if (status)
		BBPreleaseref(status->batCacheid);
	if (seen)
		BBPreleaseref(seen->batCacheid);
	if (cycles)
		BBPreleaseref(cycles->batCacheid);
	if (events)
		BBPreleaseref(events->batCacheid);
	if (time)
		BBPreleaseref(time->batCacheid);
	if (error)
		BBPreleaseref(error->batCacheid);
	throw(MAL, "datacell.queries", MAL_MALLOC_FAIL);
}
