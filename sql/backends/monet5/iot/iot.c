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
 * The interface from SQL passes through here.
 */

#include "monetdb_config.h"
#include "iot.h"
#include "opt_iot.h"
#include "sql_optimizer.h"
#include "sql_gencode.h"

MT_Lock iotLock MT_LOCK_INITIALIZER("iotLock");

// locate the SQL procedure in the catalog
static str
IOTprocedureStmt(Client cntxt, MalBlkPtr mb, str schema, str nme)
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
		throw(SQL, "iot.query", "Schema missing");
	/*tr = m->session->tr;*/
	for (o = s->funcs.set->h; o; o = o->next) {
		f = o->data;
		if (strcmp(f->base.name, nme) == 0) {
			be = (void *) backend_create(m, cntxt);
			if ( be->mvc->sa == NULL)
				be->mvc->sa = sa_create();
			//TODO fix result type
			backend_create_func(be, f, f->res,NULL);
			return MAL_SUCCEED;
		}
	}
	throw(SQL, "iot.query", "Procedure missing");
}

/* locate the MAL representation of this operation and extract the flow */
/* If the operation is not available yet, it should be compiled from its
   definition retained in the SQL catalog */
str
IOTquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str sch= NULL;
	str nme= NULL;
	str def= NULL;
	Symbol s = NULL;
	MalBlkPtr qry;
	str msg = NULL;
	InstrPtr p;
	Module scope;
	lng clk = GDKusec();
	char buf[BUFSIZ];
	static int iotquerycnt=0;


	_DEBUG_IOT_ fprintf(stderr,"#iot: register the continues query %s.%s()\n",sch,nme);

	/* check existing of the pre-compiled and activated function */
	// if( pci->argc == 3&&  PNisregistered(sch,nme) ) return MAL_SUCCEED;
		//throw(SQL, "iot.query", "already activated");

	if (pci->argc == 3) {
		sch = *getArgReference_str(stk, pci, 1);
		nme = *getArgReference_str(stk, pci, 2);
		/* check existing of the pre-compiled function */
		_DEBUG_IOT_ fprintf(stderr,"#iot: locate a SQL procedure %s.%s()\n",sch,nme);
		msg = IOTprocedureStmt(cntxt, mb, sch, nme);
		if (msg)
			return msg;
		s = findSymbolInModule(cntxt->nspace, putName(nme, strlen(nme)));
		if (s == NULL)
			throw(SQL, "iot.query", "Definition missing");
		qry = s->def;
	} else if (pci->argc == 2){
		sch = "iot";
		snprintf(buf,BUFSIZ,"iot_%d",iotquerycnt++);
		nme = buf;
		def = *getArgReference_str(stk, pci, 1);
		_DEBUG_IOT_ fprintf(stderr,"#iot: compile a compound expression %s()\n",def);
		// package it as a procedure in the current schema [todo]
		msg = SQLstatementIntern(cntxt, &def, nme, 1, 0, 0);
		if (msg)
			return msg;
		qry = cntxt->curprg->def;
	}

	_DEBUG_IOT_ fprintf(stderr,"#iot: bake a new continuous query plan\n");
	scope = findModule(cntxt->nspace, putName(sch, strlen(sch)));
	s = newFunction(putName(sch, strlen(sch)), putName(nme, strlen(nme)), FUNCTIONsymbol);
	if (s == NULL)
		throw(SQL, "iot.query", "Procedure code does not exist.");
	freeMalBlk(s->def);
	s->def = copyMalBlk(qry);
	p = getInstrPtr(s->def, 0);
	setModuleId(p, putName(sch, strlen(sch)));
	setFunctionId(p, putName(nme, strlen(nme)));
	insertSymbol(scope, s);
	_DEBUG_IOT_ printFunction(cntxt->fdout, s->def, 0, LIST_MAL_ALL);
	/* optimize the code and register at scheduler */
	if (msg == MAL_SUCCEED) {
		addOptimizers(cntxt, s->def,"iot_pipe");
		msg = optimizeMALBlock(cntxt, s->def);
		if (msg == MAL_SUCCEED) 
			msg = optimizerCheck(cntxt, s->def, "optimizer.iot", 1, GDKusec() - clk);
		addtoMalBlkHistory(mb, "iot");
	}
	if (msg == MAL_SUCCEED) {
		msg = PNregisterInternal(cntxt, s->def);
	}
	return msg;
}

str
IOTresume(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return PNresume(cntxt,mb,stk,pci);
}

str
IOTpause(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return PNpause(cntxt,mb,stk,pci);
}

str
IOTstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return PNstop(cntxt,mb,stk,pci);
}

str
IOTdump(void *ret)
{
	BSKTdump(ret);
	return PNdump(ret);
}

