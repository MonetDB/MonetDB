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
 * @f datacell
 * The interface from SQL passes through here.
 *
 */

#include "monetdb_config.h"
#include "datacell.h"
#include "opt_datacell.h"
#include "sql_optimizer.h"
#include "sql_gencode.h"

#ifdef WIN32
#include "winsock2.h"
#endif

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
	str msg = getContext(cntxt, mb, &m, NULL);
	sql_schema  *s;
	backend *be;
	node *o;
	sql_func *f;
	/*sql_trans *tr;*/

	if ( msg)
		return msg;
	s = mvc_bind_schema(m, schema);
	if (s == NULL)
		throw(SQL, "datacell.query", "Schema missing");
	/*tr = m->session->tr;*/
	for (o = s->funcs.set->h; o; o = o->next) {
		f = o->data;
		if ( strcmp(f->base.name, nme) == 0 ){
			be = (void *) backend_create(m, cntxt);
			backend_create_func(be, f);
			break;
		}
	}
	return MAL_SUCCEED;
}

str
DCprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	mvc *m = NULL;
	str msg = getContext(cntxt, mb, &m, NULL);
	sql_schema  *s;
	node *o;
	sql_table *t;
	sql_func *f;
	sql_trans *tr;

	if (m == NULL) {
		fprintf(stdout, "# MonetDB/DataCell module loaded\n");
		fflush(stdout); /* make merovingian see this *now* */
		return MAL_SUCCEED;
	}

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
	int *ret = (int *) getArgReference(stk,pci,0);
	str *tbl = (str *) getArgReference(stk,pci,1);
	str *host = (str *) getArgReference(stk,pci,2);
	int *port  = (int *) getArgReference(stk,pci,3);
	int  idx = BSKTlocate(*tbl);
	if ( idx == 0)
		BSKTregister(cntxt,mb,stk,pci);
	return DCreceptorNew(ret,tbl,host,port);
}

str
DCemitter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *) getArgReference(stk,pci,0);
	str *tbl = (str *) getArgReference(stk,pci,1);
	str *host = (str *) getArgReference(stk,pci,2);
	int *port  = (int *) getArgReference(stk,pci,3);
	int  idx = BSKTlocate(*tbl);
	if ( idx == 0)
		BSKTregister(cntxt,mb,stk,pci);
	return DCemitterNew(ret,tbl,host,port);
}

str
DCregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return BSKTregister(cntxt,mb,stk,pci);
}

str
DCpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, ret=0;
	str tbl= *(str*) getArgReference(stk, pci,1);

	idx = BSKTlocate(tbl);
	if ( idx == 0)
		throw(SQL, "datacell.pause", "Basket not found");

	DCreceptorPause(&ret, &tbl);
	DCemitterPause(&ret, &tbl);
	(void) cntxt;
	(void) mb;
	return MAL_SUCCEED;
}

str
DCresumeObject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, ret= 0;
	str tbl= *(str*) getArgReference(stk, pci,1);

	idx = BSKTlocate(tbl);
	if ( idx == 0)
		throw(SQL, "datacell.pause", "Basket not found");

	DCreceptorResume(&ret, &tbl);
	DCemitterResume(&ret, &tbl);
	(void) cntxt;
	(void) mb;
	return MAL_SUCCEED;
}

str
DCresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret;
	RCresume(&ret);
	EMresume(&ret);
	return DCresumeScheduler(cntxt,mb,stk,pci);
}

str
DCremove(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx;
	str nme= *(str*) getArgReference(stk, pci,1);

	(void) cntxt;
	(void) mb;
	idx = BSKTlocate(nme);
	if ( idx == 0)
		throw(MAL,"datacell.remove","Basket not found");
	/* remove basket  and corresponding receptor/emitters depending on it*/
	return MAL_SUCCEED;
}

str
DCmode(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, ret=0;
	str *tbl= (str*) getArgReference(stk, pci,1);
	str *arg= (str*) getArgReference(stk, pci,2);

	idx = BSKTlocate(*tbl);
	if ( idx == 0)
		throw(SQL, "datacell.mode", "Basket not found");

	RCmode(&ret, tbl,arg);
	EMmode(&ret, tbl,arg);
	(void) cntxt;
	(void) mb;
	return MAL_SUCCEED;
}

str
DCprotocol(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, ret=0;
	str *tbl= (str*) getArgReference(stk, pci,1);
	str *arg= (str*) getArgReference(stk, pci,2);

	idx = BSKTlocate(*tbl);
	if ( idx == 0)
		throw(SQL, "datacell.protocol", "Basket not found");

	RCprotocol(&ret, tbl,arg);
	EMprotocol(&ret, tbl,arg);
	(void) cntxt;
	(void) mb;
	return MAL_SUCCEED;
}


/* locate the MAL representation of this operation and extract the flow */
/* If the operation is not available yet, it should be compiled from its
   definition retained in the SQL catalog */
str
DCquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str nme = *(str*) getArgReference(stk,pci,1);
	str def;
	Symbol s = NULL;
	MalBlkPtr qry;
	str msg;
	InstrPtr p;
	Module scope;
	lng clk = GDKusec();
	char buf[BUFSIZ], *lsch, *lnme;

	BSKTelements( nme, buf, &lsch, &lnme);
	BSKTtolower(lsch);
	BSKTtolower(lnme);

	(void) mb;
	/* check if the argument denotes a procedure name */
	/* if so, get its definition to be compiled */

	/* check existing of the pre-compiled function */
	scope = findModule(cntxt->nspace,putName(lsch,strlen(lsch)));
	if ( scope)
		s = findSymbolInModule(scope,putName(lnme,strlen(lnme)));
	/* is it defined in module user */
	if ( s == NULL)
		s = findSymbolInModule(cntxt->nspace,putName(lnme,strlen(lnme)));

	if (s == NULL){
		if ( pci->argc == 3 ) {
			def = *(str*) getArgReference(stk,pci,2);
			msg = SQLstatementIntern(cntxt, &def, lnme, 0, 0);
			if ( msg )
				return msg;
			qry =cntxt->curprg->def;
		} else {
			/* get definition from catalog */
			msg =DCprocedureStmt(cntxt, mb, lsch, lnme);
			if ( msg)
				return msg;
			s = findSymbolInModule(cntxt->nspace,putName(lnme,strlen(lnme)));
			if ( s == NULL)
				throw(SQL,"datacell.query","Definition missing");
			qry= s->def;
		}
	} else
		if ( pci->argc == 3)
			throw(SQL,"datacell.query","Query already defined");
		else qry = s->def;

	scope = findModule(cntxt->nspace,putName(lsch,strlen(lsch)));
	s = newFunction(putName(lsch,strlen(lsch)), putName(lnme, strlen(lnme)),FUNCTIONsymbol);
	if ( s == NULL)
		throw(SQL,"datacell.query","Procedure code does not exist");
    freeMalBlk(s->def);
    s->def = copyMalBlk(qry);
	p= getInstrPtr(s->def,0);
	setModuleId(p, putName(lsch,strlen(lsch)));
	setFunctionId(p, putName(lnme,strlen(lnme)));
    insertSymbol(scope,s);
	/* printFunction(cntxt->fdout, s->def, 0, LIST_MAL_STMT);*/
	/* optimize the code and register at scheduler */
	if ( msg == MAL_SUCCEED) {
		OPTdatacellImplementation(cntxt,s->def,0,0);
		addOptimizers(cntxt,s->def,0);
		if ( msg == MAL_SUCCEED)
			msg = optimizeMALBlock(cntxt,s->def);
		if ( msg == MAL_SUCCEED)
			msg = optimizerCheck(cntxt, s->def, "optimizer.datacell", 1, GDKusec() - clk, OPT_CHECK_ALL);
		addtoMalBlkHistory(mb, "datacell");
	}
	PNregister(cntxt,mb,stk,pci);
	return msg;
}

str
DCresumeScheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret=0;
	PNresumeScheduler(&ret);
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
DCpauseScheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret=0;
	PNpauseScheduler(&ret);
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

str
DCpostlude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ret=0;
	RCreset(&ret);
	EMreset(&ret);
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
	return BSKTthreshold(ret,bskt,mi);
}

str
DCwindow(int *ret, str *bskt, int *sz, int *slide)
{
	return BSKTwindow(ret,bskt,sz,slide);
}

str
DCtimewindow(int *ret, str *bskt, int *sz, int *slide)
{
	return BSKTtimewindow(ret,bskt, sz,slide);
}

str
DCbeat(int *ret, str *bskt, int *beat)
{
	return BSKTbeat(ret,bskt,beat);
}
