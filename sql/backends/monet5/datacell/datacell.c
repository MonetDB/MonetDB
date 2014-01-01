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
 * The interface from SQL passes through here.
 */

#include "monetdb_config.h"
#include "datacell.h"
#include "receptor.h"
#include "emitter.h"
#include "opt_datacell.h"
#include "sql_optimizer.h"
#include "sql_gencode.h"

#ifdef WIN32
#include "winsock2.h"
#endif

MT_Lock dcLock MT_LOCK_INITIALIZER("dcLock");
/*
 * The scheduler works against a converted datacell schema.
 * It should be stopped before additions to the scheme will take effect
 */

#define DCNONINITIALIZED 1
#define DCINITIALIZED 2
#define DCPAUSED 3

/*static int DCprepared;*/

/*
 * grab all tables in the datacell schema and turn them into baskets.
 * The same for all procedures, turn them into continuous queries.
 */
static str
DCprocedureStmt(Client cntxt, MalBlkPtr mb, str schema, str nme)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_schema  *s;
	backend *be;
	node *o;
	sql_func *f;
	/*sql_trans *tr;*/

	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;
	s = mvc_bind_schema(m, schema);
	if (s == NULL)
		throw(SQL, "datacell.query", "Schema missing");
	/*tr = m->session->tr;*/
	for (o = s->funcs.set->h; o; o = o->next) {
		f = o->data;
		if (strcmp(f->base.name, nme) == 0) {
			be = (void *) backend_create(m, cntxt);
			if ( be->mvc->sa == NULL)
				be->mvc->sa = sa_create();
			backend_create_func(be, f);
			return MAL_SUCCEED;
		}
	}
	throw(SQL, "datacell.query", "Procedure missing");
}

str
DCprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
#ifdef NEED_MT_LOCK_INIT
	MT_lock_init( &dcLock, "datacellLock");
#endif
	addPipeDefinition(cntxt, "datacell_pipe",
		"optimizer.inline();optimizer.remap();optimizer.datacell();optimizer.garbageCollector();"
		"optimizer.evaluate();optimizer.costModel();optimizer.coercions();optimizer.emptySet();"
		"optimizer.aliases();optimizer.mitosis();optimizer.mergetable();optimizer.deadcode();"
		"optimizer.commonTerms();optimizer.groups();optimizer.joinPath();optimizer.reorder();"
		"optimizer.deadcode();optimizer.reduce();optimizer.dataflow();optimizer.history();"
		"optimizer.multiplex();optimizer.accumulators();optimizer.garbageCollector();");
	return MAL_SUCCEED;
}

str
DCinitialize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getSQLContext(cntxt, mb, &m, NULL);
	sql_schema  *s;
	node *o;
	sql_table *t;
	sql_func *f;
	sql_trans *tr;

	if ((msg = checkSQLContext(cntxt)) != MAL_SUCCEED)
		return msg;

	assert(m != NULL);

	s = mvc_bind_schema(m, schema_default);
	if (s == NULL)
		throw(SQL, "datacell.prelude", "Schema missing");
	tr = m->session->tr;
	for (o = s->tables.set->h; msg == MAL_SUCCEED && o; o = o->next) {
		t = o->data;
		if (BSKTlocate(t->base.name))
			throw(SQL, "datacell.register", "Basket defined twice.");
		msg = BSKTnewbasket(s, t, tr);
	}
	for (o = s->funcs.set->h; msg == MAL_SUCCEED && o; o = o->next) {
		f = o->data;
		printf("function %s\n", f->base.name);
	}
	/*DCprepared = DCINITIALIZED;*/
	(void) stk;
	(void) pci;
	return msg;
}

str
DCreceptor(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *) getArgReference(stk, pci, 0);
	str *tbl = (str *) getArgReference(stk, pci, 1);
	str *host = (str *) getArgReference(stk, pci, 2);
	int *port = (int *) getArgReference(stk, pci, 3);
	int idx = BSKTlocate(*tbl);
	str *protocol;
	str *mode;
	Receptor rc;
	str msg= MAL_SUCCEED;
	
	if (idx == 0)
		msg = BSKTregister(cntxt, mb, stk, pci);
	if ( msg) 
		return msg;
	rc = RCfind(*tbl);
	if ( pci->argc == 6 && rc != NULL ){
		protocol = (str *) getArgReference(stk, pci, 4);
		if ( strcmp("tcp", *protocol) == 0)
			rc->protocol = TCP;
		else
		if ( strcmp("TCP", *protocol) == 0)
			rc->protocol = TCP;
		else
		if ( strcmp("udp", *protocol) == 0)
			rc->protocol = TCP;
		else
		if ( strcmp("UDP", *protocol) == 0)
			rc->protocol = TCP;
		else
			throw(SQL,"datacell.register","Illegal protocol");

		mode = (str *) getArgReference(stk, pci, 5);
		if ( strcmp("active", *mode) == 0)
			rc->mode = BSKTACTIVE;
		else
		if ( strcmp("passive", *mode) == 0)
			rc->mode = BSKTPASSIVE;
		else
			throw(SQL,"datacell.register","Illegal mode");
	}
	return RCreceptorStart(ret, tbl, host, port);
}

str
DCbasket(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	return BSKTregister(cntxt, mb, stk, pci);
}

str
DCemitter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *) getArgReference(stk, pci, 0);
	str *tbl = (str *) getArgReference(stk, pci, 1);
	str *host = (str *) getArgReference(stk, pci, 2);
	int *port = (int *) getArgReference(stk, pci, 3);
	int idx = BSKTlocate(*tbl);
	Emitter em;
	str *protocol, *mode;
	str msg= MAL_SUCCEED;

	if (idx == 0)
		msg = BSKTregister(cntxt, mb, stk, pci);
	if ( msg) 
		return msg;
	em = EMfind(*tbl);
	if ( pci->argc == 6 && em != NULL ){
		protocol = (str *) getArgReference(stk, pci, 4);
		if ( strcmp("tcp", *protocol) == 0)
			em->protocol = TCP;
		else
		if ( strcmp("TCP", *protocol) == 0)
			em->protocol = TCP;
		else
		if ( strcmp("udp", *protocol) == 0)
			em->protocol = TCP;
		else
		if ( strcmp("UDP", *protocol) == 0)
			em->protocol = TCP;
		else
			throw(SQL,"datacell.register","Illegal protocol");

		mode = (str *) getArgReference(stk, pci, 5);
		if ( strcmp("active", *mode) == 0)
			em->mode = BSKTACTIVE;
		else
		if ( strcmp("passive", *mode) == 0)
			em->mode = BSKTPASSIVE;
		else
			throw(SQL,"datacell.register","Illegal mode");
	}
	return EMemitterStart(ret, tbl, host, port);
}

str
DCpauseObject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, ret = 0;
	str tbl = *(str *) getArgReference(stk, pci, 1);
	str msg1= MAL_SUCCEED, msg2 = MAL_SUCCEED;

	if ( strcmp(tbl,"*")== 0){
		str msg = RCpause(&ret);
		if ( msg) 
			return msg;
		msg = EMpause(&ret);
		return msg;
	}
	idx = BSKTlocate(tbl);
	if (idx ) {
		msg1 = RCreceptorPause(&ret, &tbl);
		if ( msg1 == MAL_SUCCEED)
			return msg1;
		msg2 = EMemitterPause(&ret, &tbl);
		if ( msg2 == MAL_SUCCEED ){
			GDKfree(msg1);
			return MAL_SUCCEED;
		}
		return msg2;
	}
	return PNpauseQuery(cntxt,mb,stk,pci);
}

str
DCresumeObject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, ret = 0;
	str tbl = *(str *) getArgReference(stk, pci, 1);

	if ( strcmp(tbl,"*")== 0){
		RCresume(&ret);
		EMresume(&ret);
		return DCresumeScheduler(cntxt, mb, stk, pci);
	}
	idx = BSKTlocate(tbl);
	if (idx ) {
		RCreceptorResume(&ret, &tbl);
		EMemitterResume(&ret, &tbl);
	}
	return PNresumeQuery(cntxt,mb,stk,pci);
}

str
DCstopObject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, ret;
	str nme = *(str *) getArgReference(stk, pci, 1);

	(void) cntxt;
	(void) mb;
	idx = BSKTlocate(nme);
	if (idx == 0)
		throw(MAL, "datacell.remove", "Basket not found");
	/* first remove the dependent continous queries */

	/* finally remove the basket itself, the underlying table is *not* dropped */
	return BSKTdrop(&ret, &nme);
}

/* locate the MAL representation of this operation and extract the flow */
/* If the operation is not available yet, it should be compiled from its
   definition retained in the SQL catalog */
str
DCquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str nme = *(str *) getArgReference(stk, pci, 1);
	str def;
	Symbol s = NULL;
	MalBlkPtr qry;
	str msg = NULL;
	InstrPtr p;
	Module scope;
	lng clk = GDKusec();
	char buf[BUFSIZ], *lsch, *lnme;

	if ( mb->errors)
		throw(SQL, "datacell.query", "Query contains errors");
	BSKTelements(nme, buf, &lsch, &lnme);
	BSKTtolower(lsch);
	BSKTtolower(lnme);

	/* check if the argument denotes a procedure name */
	/* if so, get its definition to be compiled */

	/* check existing of the pre-compiled function */
	scope = findModule(cntxt->nspace, putName(lsch, strlen(lsch)));
	if (scope)
		s = findSymbolInModule(scope, putName(lnme, strlen(lnme)));
	/* is it defined in module user */
	if (s == NULL)
		s = findSymbolInModule(cntxt->nspace, putName(lnme, strlen(lnme)));

	if (s == NULL) {
		if (pci->argc == 3) {
			def = *(str *) getArgReference(stk, pci, 2);
			msg = SQLstatementIntern(cntxt, &def, lnme, 0, 0);
			if (msg)
				return msg;
			qry = cntxt->curprg->def;
		} else {
			/* get definition from catalog */
			msg = DCprocedureStmt(cntxt, mb, lsch, lnme);
			if (msg)
				return msg;
			s = findSymbolInModule(cntxt->nspace, putName(lnme, strlen(lnme)));
			if (s == NULL)
				throw(SQL, "datacell.query", "Definition missing");
			qry = s->def;
		}
	} else if (pci->argc == 3)
		throw(SQL, "datacell.query", "Query already defined");
	else
		qry = s->def;

	scope = findModule(cntxt->nspace, putName(lsch, strlen(lsch)));
	s = newFunction(putName(lsch, strlen(lsch)), putName(lnme, strlen(lnme)), FUNCTIONsymbol);
	if (s == NULL)
		throw(SQL, "datacell.query", "Procedure code does not exist");
	freeMalBlk(s->def);
	s->def = copyMalBlk(qry);
	p = getInstrPtr(s->def, 0);
	setModuleId(p, putName(lsch, strlen(lsch)));
	setFunctionId(p, putName(lnme, strlen(lnme)));
	insertSymbol(scope, s);
	/* printFunction(cntxt->fdout, s->def, 0, LIST_MAL_STMT);*/
	/* optimize the code and register at scheduler */
	if (msg == MAL_SUCCEED) {
		OPTdatacellImplementation(cntxt, s->def, 0, 0);
		addOptimizers(cntxt, s->def,"default_pipe");
		if (msg == MAL_SUCCEED)
			msg = optimizeMALBlock(cntxt, s->def);
		if (msg == MAL_SUCCEED)
			msg = optimizerCheck(cntxt, s->def, "optimizer.datacell", 1, GDKusec() - clk, OPT_CHECK_ALL);
		addtoMalBlkHistory(mb, "datacell");
	}
	PNregister(cntxt, mb, stk, pci);
	return msg;
}

str
DCresumeScheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret = 0;
	str msg;

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
    msg = RCresume(&ret);
	if ( msg )
		return msg;
    msg = EMresume(&ret);
	if ( msg )
		return msg;
	return PNresumeScheduler(&ret);
}

str
DCpauseScheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret = 0;
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	RCpause(&ret);
	EMpause(&ret);
	return PNpauseScheduler(&ret);
}

str
DCstopScheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret = 0;
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	RCstop(&ret);
	EMstop(&ret);
	return PNstopScheduler(&ret);
}

str
DCpostlude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret = 0;
	RCstop(&ret);
	EMstop(&ret);
	PNstopScheduler(&ret);
	BSKTreset(&ret);
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	/*DCprepared = DCNONINITIALIZED;*/
	return MAL_SUCCEED;
}

str
DCdump(int *ret)
{
	BSKTdump(ret);
	RCdump();
	EMdump();
	PNdump(ret);
	return MAL_SUCCEED;
}

str
DCthreshold(int *ret, str *bskt, int *mi)
{
	return BSKTthreshold(ret, bskt, mi);
}

str
DCwindow(int *ret, str *bskt, lng *sz, lng *slide)
{
	return BSKTwindow(ret, bskt, sz, slide);
}

str
DCtimewindow(int *ret, str *bskt, lng *sz, lng *slide)
{
	return BSKTtimewindow(ret, bskt, sz, slide);
}

str
DCbeat(int *ret, str *bskt, lng *beat)
{
	return BSKTbeat(ret, bskt, beat);
}
