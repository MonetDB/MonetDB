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

/*
 * Martin Kersten
 * Petri-net query scheduler
   The Iothings scheduler is based on the long-standing and mature Petri-net technology. For completeness, we
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

   The Iothings scheduler is a fair implementation of a Petri-net interpreter. It models all continuous queries as transitions,
   and the stream tables as the places. The events are equivalent to tokens. Unlike the pure Petri-net model, all tokens in a place
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
#include "iot.h"
#include "petrinet.h"
#include "mal_builder.h"
#include "opt_prelude.h"

#define MAXPN 200           /* it is the minimum, if we need more space GDKrealloc */
#define PNcontrolInfinit 1  /* infinit loop of PNController  */
#define PNcontrolEnd 2      /* when all factories are disable PNController exits */

#define _DEBUG_PETRINET_ 

static str statusname[6] = { "<unknown>", "init", "paused", "running", "stop", "error" };

/*static int controlRounds = PNcontrolInfinit;*/
static void
PNstartScheduler(void);

typedef struct {
	char *table;
	int bskt;       /* basket used */
	BAT *b;             /* reference BAT for checking content */
	int available;      /* approximate number of events available */
	size_t lastcount;   /* statistics gathering */
	size_t consumed;
} PoolRec;

typedef struct {
	str modname;	/* the MAL query block */
	str fcnname;
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

str PNanalyseWrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module scope;
	Symbol s = 0;
	str modnme = *getArgReference_str(stk, pci, 1);
	str fcnnme = *getArgReference_str(stk, pci, 2);

	(void) mb;
	scope = findModule(cntxt->nspace, putName(modnme, (int) strlen(modnme)));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme, (int) strlen(fcnnme)));
	if (s == NULL)
		throw(MAL, "petrinet.analysis", "Could not find function");

	return PNanalysis(cntxt, s->def);
}


str PNregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module scope;
	Symbol s = 0;
	str modnme = *getArgReference_str(stk, pci, 1);
	str fcnnme = *getArgReference_str(stk, pci, 2);

	(void) mb;
	scope = findModule(cntxt->nspace, putName(modnme, (int) strlen(modnme)));
	if (scope)
		s = findSymbolInModule(scope, putName(fcnnme, (int) strlen(fcnnme)));

	if (s == NULL)
		throw(MAL, "petrinet.register", "Could not find function");

	return PNregisterInternal(cntxt,s->def);
}

static int
PNlocate(str modname, str fcnname)
{
	int i;
	for (i = 0; i < pnettop; i++)
		if (strcmp(pnet[i].modname, modname) == 0 && strcmp(pnet[i].fcnname, fcnname) == 0)
			break;
	return i;
}
/* A transition is only allowed when all inputs are privately used */
str
PNregisterInternal(Client cntxt, MalBlkPtr mb)
{
	int i;
	InstrPtr sig;
	str msg = MAL_SUCCEED;

	if (pnettop == 0 && (pnet = (PNnode *) GDKzalloc(MAXPN * sizeof(PNnode))) == NULL)
		throw(MAL, "petrinet.register", MAL_MALLOC_FAIL);
	else if (pnettop >= MAXPN) {
		if ((pnet = (PNnode *) GDKrealloc(pnet, sizeof(PNnode) * (pnettop + 1))) == NULL)
			throw(MAL, "petrinet.register", MAL_MALLOC_FAIL);
	} 

	sig= getInstrPtr(mb,0);
	i = PNlocate(getModuleId(sig), getFunctionId(sig));
	if (i != pnettop)
		throw(MAL, "petrinet.register", "Duplicate definition of transition");

	memset((char *) (pnet+pnettop), 0, sizeof(PNnode));
	pnet[pnettop].modname = GDKstrdup(getModuleId(sig));
	pnet[pnettop].fcnname = GDKstrdup(getFunctionId(sig));

	pnet[pnettop].status = BSKTPAUSE;
	pnet[pnettop].cycles = 0;
	pnet[pnettop].seen = *timestamp_nil;
	/* all the rest is zero */

	pnettop++;
	msg = PNanalysis(cntxt, mb);
	/* start the scheduler if analysis does not show errors */
	if( msg == MAL_SUCCEED)
		PNstartScheduler();
	return msg;
}

static str
PNstatus( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int newstatus){
	str modname;
	str fcnname;
	int i;

	(void) cntxt;
	(void) mb;
	MT_lock_set(&iotLock);
	if ( pci->argc == 3){
		modname= *getArgReference_str(stk,pci,1);
		fcnname= *getArgReference_str(stk,pci,2);
		i = PNlocate(modname,fcnname);
		if ( i == pnettop)
			throw(SQL,"iot.pause","Continuous query not found");
		pnet[i].status = newstatus;
		return MAL_SUCCEED;
	}
	for ( i = 0; i < pnettop; i++)
		pnet[i].status = newstatus;
	MT_lock_unset(&iotLock);
	return MAL_SUCCEED;
}
str
PNpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	return PNstatus(cntxt, mb, stk, pci, BSKTPAUSE);
}

str
PNresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	return PNstatus(cntxt, mb, stk, pci, BSKTRUNNING);
}

str
PNstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	return PNstatus(cntxt, mb, stk, pci, BSKTSTOP);
}

// remove a transition
str
PNdrop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	return PNstatus(cntxt, mb, stk, pci, BSKTSTOP);
}

str PNdump(void *ret)
{
	int i, k;
	mnstr_printf(PNout, "#scheduler status %s\n", statusname[status]);
	for (i = 0; i < pnettop; i++) {
		mnstr_printf(PNout, "#[%d]\t%s.%s %s delay %d cycles %d events %d time " LLFMT " ms\n",
				i, pnet[i].modname, pnet[i].fcnname, statusname[pnet[i].status], pnet[i].delay, pnet[i].cycles, pnet[i].events, pnet[i].time / 1000);
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
str
PNsource(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	throw(MAL,"petrinet.source","Should not be called directly");
}
str
PNtarget(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	throw(MAL,"petrinet.source","Should not be called directly");
}

/* check the routine for input/output relationships */
/* Make sure we do not re-use the same source more than once */
str
PNanalysis(Client cntxt, MalBlkPtr mb)
{
	int i, j, k;
	InstrPtr p;
	str tbl;

	(void) cntxt;
	/* first check for errors */
	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == basketRef && getFunctionId(p) == grabRef) {
			tbl = getVarConstant(mb, getArg(p, p->argc - 1)).val.sval;

			for (j = 0; j < pnettop; j++)
				for (k = 0; k < pnet[j].srctop; k++)
					if (strcmp(tbl, pnet[j].source[k].table) == 0)
						throw(MAL, "iot.register", "Duplicate use of continuous query input");
		}
	}
	for (i = 0; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == basketRef && getFunctionId(p) == grabRef) {
			tbl = getVarConstant(mb, getArg(p, p->argc - 1)).val.sval;
			//PNsource(&ret, &nme, &tbl);
		}
		if (getModuleId(p) == basketRef && getFunctionId(p) == putName("pass", 4)) {
			tbl = getVarConstant(mb, getArg(p, p->retc)).val.sval;
			mnstr_printf(cntxt->fdout, "#output basket %s \n", tbl);
			//PNtarget(&ret, &nme, &tbl);
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

	cntxt = mal_clients; /* run as admin in SQL mode*/
	 if( strcmp(cntxt->scenario, "sql") )
		 SQLinitEnvironment(cntxt, NULL, NULL, NULL);
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
	MT_lock_set(&iotLock);
	status = BSKTRUNNING;
	mb->stop = 1;
	/* create an execution environment for all transitions */
	for (i = 0; i < pnettop; i++) {
		p = newFcnCall(mb, pnet[i].modname, pnet[i].fcnname);
		pnet[i].pc = getPC(mb, p);
	}
	p= newFcnCall(mb, sqlRef, abortRef);
	abortpc = getPC(mb,p);
	pushEndInstruction(mb);
	/*printf("\n1 mb->vtop:%d\n",mb->vtop);*/
	chkProgram(cntxt->fdout, cntxt->nspace, mb);
	MT_lock_unset(&iotLock);

#ifdef _DEBUG_PETRINET_
		printFunction(cntxt->fdout, mb, 0, LIST_MAL_ALL);
#endif
	if (mb->errors) {
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
					mnstr_printf(cntxt->fdout, "#PETRINET:%d tuples for %s, source %d\n", cnt, pnet[i].fcnname, j);
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
				mnstr_printf(cntxt->fdout, "#Run transition %s pc %d\n", pnet[i].fcnname, pnet[i].pc);
#endif
#ifdef _BASKET_SIZE_
				mnstr_printf(cntxt->fdout, "\npnet[%d].srctop:%d\n", i, pnet[i].srctop);
				mnstr_printf(cntxt->fdout, "Function: %s basket size %d\n", pnet[i].fcnname, pnet[i].source[0].available);
#endif

				(void) MTIMEcurrent_timestamp(&baskets[idx].seen);
				t = GDKusec();
				pnet[i].cycles++;
				msg = reenterMAL(cntxt, mb, pnet[i].pc, pnet[i].pc + 1, glb);
				pnet[i].time += GDKusec() - t + analysis;   /* keep around in microseconds */
				if (msg != MAL_SUCCEED && !strstr(msg, "too early")) {
					char buf[BUFSIZ];
					if (pnet[i].error == NULL) {
						snprintf(buf, BUFSIZ - 1, "Query %s failed:%s", pnet[i].fcnname, msg);
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
	MT_lock_set(&iotLock);
	status = BSKTINIT;
	MT_lock_unset(&iotLock);
	(void) dummy;
}

void
PNstartScheduler(void)
{
	MT_Id pid;
	int s;
#ifdef _DEBUG_PETRINET_
	PNdump(&s);
#else
	(void) s;
#endif

	if (status== BSKTINIT && MT_create_thread(&pid, PNcontroller, &s, MT_THR_JOINABLE) != 0){
		GDKerror( "petrinet creation failed");
	}
	(void) pid;
}

/* inspection  routines */
str
PNtable(bat *modnameId, bat *fcnnameId, bat *statusId, bat *seenId, bat *cyclesId, bat *eventsId, bat *timeId, bat * errorId)
{
	BAT *modname = NULL, *fcnname = NULL, *status = NULL, *seen = NULL, *cycles = NULL, *events = NULL, *time = NULL, *error = NULL;
	int i;

	modname = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (modname == 0)
		goto wrapup;
	BATseqbase(modname, 0);
	fcnname = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (fcnname == 0)
		goto wrapup;
	BATseqbase(fcnname, 0);
	status = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (status == 0)
		goto wrapup;
	BATseqbase(status, 0);
	seen = BATnew(TYPE_void, TYPE_timestamp, BATTINY, TRANSIENT);
	if (seen == 0)
		goto wrapup;
	BATseqbase(seen, 0);
	cycles = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (cycles == 0)
		goto wrapup;
	BATseqbase(cycles, 0);
	events = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (events == 0)
		goto wrapup;
	BATseqbase(events, 0);
	time = BATnew(TYPE_void, TYPE_lng, BATTINY, TRANSIENT);
	if (time == 0)
		goto wrapup;
	BATseqbase(time, 0);
	error = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (error == 0)
		goto wrapup;
	BATseqbase(error, 0);

	for (i = 0; i < pnettop; i++) {
		BUNappend(modname, pnet[i].modname, FALSE);
		BUNappend(fcnname, pnet[i].fcnname, FALSE);
		BUNappend(status, statusname[pnet[i].status], FALSE);
		BUNappend(seen, &pnet[i].seen, FALSE);
		BUNappend(cycles, &pnet[i].cycles, FALSE);
		BUNappend(events, &pnet[i].events, FALSE);
		BUNappend(time, &pnet[i].time, FALSE);
		BUNappend(error, (pnet[i].error ? pnet[i].error : ""), FALSE);
	}
	BBPkeepref(*modnameId = modname->batCacheid);
	BBPkeepref(*fcnnameId = fcnname->batCacheid);
	BBPkeepref(*statusId = status->batCacheid);
	BBPkeepref(*seenId = seen->batCacheid);
	BBPkeepref(*cyclesId = cycles->batCacheid);
	BBPkeepref(*eventsId = events->batCacheid);
	BBPkeepref(*timeId = time->batCacheid);
	BBPkeepref(*errorId = error->batCacheid);
	return MAL_SUCCEED;
wrapup:
	if (modname)
		BBPunfix(modname->batCacheid);
	if (fcnname)
		BBPunfix(fcnname->batCacheid);
	if (status)
		BBPunfix(status->batCacheid);
	if (seen)
		BBPunfix(seen->batCacheid);
	if (cycles)
		BBPunfix(cycles->batCacheid);
	if (events)
		BBPunfix(events->batCacheid);
	if (time)
		BBPunfix(time->batCacheid);
	if (error)
		BBPunfix(error->batCacheid);
	throw(MAL, "iot.queries", MAL_MALLOC_FAIL);
}
