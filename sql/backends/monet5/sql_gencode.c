/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @f sql_gencode
 * @t SQL to MAL code generation.
 * @a N. Nes, M. Kersten
 * @+ MAL Code generation
 * This module contains the actions to construct a MAL program, ready for
 * optimization and execution by the Monet V5 kernel.
 *
 * The code base is modeled directly after its MIL variant, replacing
 * each IO request by instructions to initialize the corresponding MAL data
 * structure.
 * To speed up the compilation, we may consider keeping a cache of pre-compiled
 * statements.
 *
 * MAL extensions needed. A temporary variable used as an argument
 * should be printed (done). Consider replacing modname/fcnname by
 * an integer constant and a global lookup table. This should
 * reduce the cost to prepare MAL statements significantly.
 *
 * A dummy module is needed to load properly.
 */
#include "monetdb_config.h"
#include "sql_gencode.h"
#include "sql_optimizer.h"
#include "sql_scenario.h"
#include "sql_mvc.h"
#include "sql_qc.h"
#include "sql_optimizer.h"
#include "mal_namespace.h"
#include "opt_prelude.h"
#include "querylog.h"
#include "mal_builder.h"
#include "mal_debugger.h"

#include <rel_select.h>
#include <rel_optimizer.h>
#include <rel_prop.h>
#include <rel_rel.h>
#include <rel_exp.h>
#include <rel_psm.h>
#include <rel_bin.h>
#include <rel_dump.h>
#include <rel_remote.h>

int
constantAtom(backend *sql, MalBlkPtr mb, atom *a)
{
	int idx;
	ValPtr vr = (ValPtr) &a->data;
	ValRecord cst;

	(void) sql;
	cst.vtype = 0;
	VALcopy(&cst, vr);
	idx = defConstant(mb, vr->vtype, &cst);
	return idx;
}

/*
 * To speedup code generation we freeze the references to the major module names.
 */

void
initSQLreferences(void)
{
	if (algebraRef == NULL)
		GDKfatal("error initSQLreferences");
}

InstrPtr
table_func_create_result(MalBlkPtr mb, InstrPtr q, sql_func *f, list *restypes)
{
	node *n;
	int i;

	if (q == NULL)
		return NULL;
	if (f->varres) {
		for (i = 0, n = restypes->h; n; n = n->next, i++) {
			sql_subtype *st = n->data;
			int type = st->type->localtype;

			type = newBatType(type);
			if (i) {
				if ((q = pushReturn(mb, q, newTmpVariable(mb, type))) == NULL)
					return NULL;
			} else
				setVarType(mb, getArg(q, 0), type);
			setVarUDFtype(mb, getArg(q, i));
		}
	} else {
		for (i = 0, n = f->res->h; n; n = n->next, i++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;

			type = newBatType(type);
			if (i) {
				if ((q = pushReturn(mb, q, newTmpVariable(mb, type))) == NULL)
					return NULL;
			} else
				setVarType(mb, getArg(q, 0), type);
			setVarUDFtype(mb, getArg(q, i));
		}
	}
	return q;
}

InstrPtr
relational_func_create_result(mvc *sql, MalBlkPtr mb, InstrPtr q, sql_rel *f)
{
	sql_rel *r = f;
	node *n;
	int i;

	if (q == NULL)
		return NULL;
	if (is_topn(r->op))
		r = r->l;
	if (!is_project(r->op))
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
	q->argc = q->retc = 0;
	for (i = 0, n = r->exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		int type = exp_subtype(e)->type->localtype;

		type = newBatType(type);
		q = pushReturn(mb, q, newTmpVariable(mb, type));
	}
	return q;
}


static int
_create_relational_function(mvc *m, const char *mod, const char *name, sql_rel *rel, stmt *call, list *rel_ops, int inline_func)
{
	sql_rel *r;
	Client c = MCgetClient(m->clientid);
	backend *be = (backend *) c->sqlcontext;
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr = 0;
	Symbol backup = NULL, curPrg = NULL;
	int old_argc = be->mvc->argc;

	r = rel_optimizer(m, rel);

	backup = c->curprg;
	curPrg = c->curprg = newFunction(putName(mod), putName(name), FUNCTIONsymbol);
	if( curPrg == NULL)
		return -1;

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, r);
	setVarUDFtype(curBlk, 0);

	/* ops */
	if (call && call->type == st_list) {
		node *n;
		list *ops = call->op4.lval;

		for (n = ops->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			int type = t->type->localtype;
			int varid = 0;
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;
			char buf[64];

			snprintf(buf,64,"A%s",nme);
			varid = newVariable(curBlk, buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	} else if (rel_ops) {
		node *n;

		for (n = rel_ops->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = &e->tpe;
			int type = t->type->localtype;
			int varid = 0;
			char buf[64];

			snprintf(buf,64,"A%d",e->flag);
			varid = newVariable(curBlk, (char *)buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}

	/* add return statement */
	r = rel_psm_stmt(m->sa, exp_return(m->sa,  exp_rel(m, r), 0));
	be->mvc->argc = 0;
	if (backend_dumpstmt(be, curBlk, r, 0, 1, NULL) < 0) {
		freeSymbol(curPrg);
		if (backup)
			c->curprg = backup;
		return -1;
	}
	be->mvc->argc = old_argc;
	/* SQL function definitions meant for inlineing should not be optimized before */
	if (inline_func)
		curBlk->inlineProp = 1;
	/* optimize the code */
	SQLaddQueryToCache(c);
	if( curBlk->inlineProp == 0)
		SQLoptimizeQuery(c, c->curprg->def);
	else{
		chkProgram(c->fdout, c->nspace, c->curprg->def);
		SQLoptimizeFunction(c,c->curprg->def);
	}
	if (backup)
		c->curprg = backup;
	return 0;
}

static str
rel2str( mvc *sql, sql_rel *rel)
{
	buffer *b;
	stream *s = buffer_wastream(b = buffer_create(1024), "rel_dump");
	list *refs = sa_list(sql->sa);
	char *res = NULL; 

	rel_print_refs(sql, s, rel, 0, refs, 0);
	rel_print_(sql, s, rel, 0, refs, 0);
	mnstr_printf(s, "\n");
	res = buffer_get_buf(b);
	buffer_destroy(b);
	mnstr_destroy(s);
	return res;
}


/* stub and remote function */
static int
_create_relational_remote(mvc *m, const char *mod, const char *name, sql_rel *rel, stmt *call, prop *prp)
{
	Client c = MCgetClient(m->clientid);
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr = 0, p, o;
	Symbol backup = NULL;
	const char *uri = mapiuri_uri(prp->value, m->sa);
	node *n;
	int i, q, v;
	int *lret, *rret;
	char *lname = GDKstrdup(name);
	sql_rel *r = rel;

	if (is_topn(r->op))
		r = r->l;
	if (!is_project(r->op))
		r = rel_project(m->sa, r, rel_projections(m, r, NULL, 1, 1));
	lret = SA_NEW_ARRAY(m->sa, int, list_length(r->exps));
	rret = SA_NEW_ARRAY(m->sa, int, list_length(r->exps));

	/* create stub */
	backup = c->curprg;
	c->curprg = newFunction(putName(mod), putName(name), FUNCTIONsymbol);
	if( c->curprg == NULL)
		return -1;
	lname[0] = 'l';
	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, rel);
	setVarUDFtype(curBlk, 0);

	/* ops */
	if (call && call->type == st_list) {
		node *n;

		for (n = call->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			int type = t->type->localtype;
			int varid = 0;
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;
			char buf[64];

			snprintf(buf,64,"A%s",nme);
			varid = newVariable(curBlk, buf,strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}

	/* declare return variables */
	for (i = 0, n = r->exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		int type = exp_subtype(e)->type->localtype;

		type = newBatType(type);
		p = newFcnCall(curBlk, batRef, newRef);
		p = pushType(curBlk, p, getBatType(type));
		setArgType(curBlk, p, 0, type);
		lret[i] = getArg(p, 0);
	}

	/* q := remote.connect("uri", "user", "pass"); */
	p = newStmt(curBlk, remoteRef, connectRef);
	p = pushStr(curBlk, p, uri);
	p = pushStr(curBlk, p, "monetdb");
	p = pushStr(curBlk, p, "monetdb");
	p = pushStr(curBlk, p, "msql");
	q = getArg(p, 0);

	/* remote.exec(q, "sql", "register", "mod", "name", "relational_plan", "signature"); */
	p = newInstruction(curBlk, remoteRef, execRef);
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, sqlRef);
	p = pushStr(curBlk, p, registerRef);

	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushInt(curBlk, o, TYPE_str); /* dummy result type */
	p = pushReturn(curBlk, p, getArg(o, 0));

	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, mod);
	p = pushArgument(curBlk, p, getArg(o,0));

	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, lname);
	p = pushArgument(curBlk, p, getArg(o,0));

	{ 
	int len = 1024, nr = 0;
	char *s, *buf = GDKmalloc(len);
	s = rel2str(m, rel);
	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, s);	/* relational plan */
	p = pushArgument(curBlk, p, getArg(o,0));
	free(s); 

	s = "";
	if (call && call->type == st_list) {
		node *n;

		buf[0] = 0;
		for (n = call->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;

			nr += snprintf(buf+nr, len-nr, "%s %s(%u,%u)%c", nme, t->type->sqlname, t->digits, t->scale, n->next?',':' ');
		}
		s = buf;
	}
	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, s);	/* signature */
	p = pushArgument(curBlk, p, getArg(o,0));
	GDKfree(buf);
	}
	pushInstruction(curBlk, p);

	/* (x1, x2, ..., xn) := remote.exec(q, "mod", "fcn"); */
	p = newInstruction(curBlk, remoteRef, execRef);
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, mod);
	p = pushStr(curBlk, p, lname);
	getArg(p, 0) = -1;

	for (i = 0, n = r->exps->h; n; n = n->next, i++) {
		/* x1 := remote.put(q, :type) */
		o = newFcnCall(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushArgument(curBlk, o, lret[i]);
		v = getArg(o, 0);
		p = pushReturn(curBlk, p, v);
		rret[i] = v;
	}

	/* send arguments to remote */
	for (i = curInstr->retc; i < curInstr->argc; i++) {
		/* x1 := remote.put(q, A0); */
		o = newStmt(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushArgument(curBlk, o, getArg(curInstr, i));
		p = pushArgument(curBlk, p, getArg(o, 0));
	}
	pushInstruction(curBlk, p);

	/* return results */
	for (i = 0; i < curInstr->retc; i++) {
		/* y1 := remote.get(q, x1); */
		p = newFcnCall(curBlk, remoteRef, getRef);
		p = pushArgument(curBlk, p, q);
		p = pushArgument(curBlk, p, rret[i]);
		getArg(p, 0) = lret[i];
	}

	/* remote.disconnect(q); */
	p = newStmt(curBlk, remoteRef, disconnectRef);
	p = pushArgument(curBlk, p, q);

	p = newInstruction(curBlk, NULL, NULL);
	p->barrier= RETURNsymbol;
	p->retc = p->argc = 0;
	for (i = 0; i < curInstr->retc; i++)
		p = pushArgument(curBlk, p, lret[i]);
	p->retc = p->argc;
	/* assignment of return */
	for (i = 0; i < curInstr->retc; i++)
		p = pushArgument(curBlk, p, lret[i]);
	pushInstruction(curBlk, p);
	pushEndInstruction(curBlk);

	/* SQL function definitions meant for inlineing should not be optimized before */
	//for now no inline of the remote function, this gives garbage collection problems
	//curBlk->inlineProp = 1;

	SQLaddQueryToCache(c);
	//chkProgram(c->fdout, c->nspace, c->curprg->def);
	SQLoptimizeFunction(c, c->curprg->def);
	if (backup)
		c->curprg = backup;
	GDKfree(lname);		/* make sure stub is called */
	return 0;
}

int
monet5_create_relational_function(mvc *m, const char *mod, const char *name, sql_rel *rel, stmt *call, list *rel_ops, int inline_func)
{
	prop *p = NULL;

	if (rel && (p = find_prop(rel->p, PROP_REMOTE)) != NULL)
		return _create_relational_remote(m, mod, name, rel, call, p);
	else
		return _create_relational_function(m, mod, name, rel, call, rel_ops, inline_func);
}

/*
 * The kernel uses two calls to procedures defined in SQL.
 * They have to be initialized, which is currently hacked
 * by using the SQLstatment.
 */
static stmt *
sql_relation2stmt(backend *be, sql_rel *r)
{
	mvc *c = be->mvc;
	stmt *s = NULL;

	if (!r) {
		return NULL;
	} else {
		if (c->emode == m_plan) {
			rel_print(c, r, 0);
		} else {
			s = output_rel_bin(be, r);
		}
	}
	return s;
}

int
backend_dumpstmt(backend *be, MalBlkPtr mb, sql_rel *r, int top, int add_end, char *query)
{
	mvc *c = be->mvc;
	InstrPtr q, querylog;
	int old_mv = be->mvc_var;
	MalBlkPtr old_mb = be->mb;
	stmt *s;
	char *t, *tt;
       
	// Always keep the SQL query around for monitoring

	if( query == 0)
		tt = t = GDKstrdup("-- no query");
	else
		tt = t = GDKstrdup(query);
	while (t && isspace((int) *t))
		t++;

	querylog = q = newStmt(mb, querylogRef, defineRef);
	if (q == NULL) {
		GDKfree(tt);
		return -1;
	}
	setVarType(mb, getArg(q, 0), TYPE_void);
	setVarUDFtype(mb, getArg(q, 0));
	q = pushStr(mb, q, t);
	GDKfree(tt);
	q = pushStr(mb, q, getSQLoptimizer(be->mvc));

	/* announce the transaction mode */
	q = newStmt(mb, sqlRef, "mvc");
	if (q == NULL)
		return -1;
	be->mvc_var = getDestVar(q);
	be->mb = mb;
       	s = sql_relation2stmt(be, r);
	if (!s) {
		(void) pushInt(mb, querylog, mb->stop);
		return 0;
	}

	be->mvc_var = old_mv;
	be->mb = old_mb;
	if (top && c->caching && (c->type == Q_SCHEMA || c->type == Q_TRANS)) {
		q = newStmt(mb, sqlRef, exportOperationRef);
		if (q == NULL)
			return -1;
	}
	/* generate a dummy return assignment for functions */
	if (getArgType(mb, getInstrPtr(mb, 0), 0) != TYPE_void && getInstrPtr(mb, mb->stop - 1)->barrier != RETURNsymbol) {
		q = newAssignment(mb);
		if (q == NULL)
			return -1;
		getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
		q->barrier = RETURNsymbol;
	}
	if (add_end)
		pushEndInstruction(mb);
	(void) pushInt(mb, querylog, mb->stop);
	return 0;
}

/* Generate the assignments of the query arguments to the query template*/
int
backend_callinline(backend *be, Client c)
{
	mvc *m = be->mvc;
	InstrPtr curInstr = 0;
	MalBlkPtr curBlk = c->curprg->def;

	if (m->argc) {	
		int argc = 0;

		for (; argc < m->argc; argc++) {
			atom *a = m->args[argc];
			int type = atom_type(a)->type->localtype;
			int varid = 0;

			curInstr = newAssignment(curBlk);
			if (curInstr == NULL)
				return -1;
			a->varid = varid = getDestVar(curInstr);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);

			if (atom_null(a)) {
				sql_subtype *t = atom_type(a);
				(void) pushNil(curBlk, curInstr, t->type->localtype);
			} else {
				int _t = constantAtom(be, curBlk, a);
				(void) pushArgument(curBlk, curInstr, _t);
			}
		}
	}
	c->curprg->def = curBlk;
	return 0;
}

/* SQL procedures, functions and PREPARE statements are compiled into a parameterised plan */
Symbol
backend_dumpproc(backend *be, Client c, cq *cq, sql_rel *r)
{
	mvc *m = be->mvc;
	MalBlkPtr mb = 0;
	Symbol curPrg = 0, backup = NULL;
	InstrPtr curInstr = 0;
	int argc = 0;
	char arg[IDLENGTH];
	node *n;

	backup = c->curprg;
	if (cq)
		c->curprg = newFunction(userRef, putName(cq->name), FUNCTIONsymbol);
	else
		c->curprg = newFunction(userRef, "tmp", FUNCTIONsymbol);
	if (c->curprg == NULL)
		return NULL;

	curPrg = c->curprg;
	curPrg->def->keephistory = backup->def->keephistory;
	mb = curPrg->def;
	curInstr = getInstrPtr(mb, 0);
	/* we do not return anything */
	setVarType(mb, 0, TYPE_void);
	setVarUDFtype(mb, 0);
	setModuleId(curInstr, userRef);

	if (m->argc) {
		for (argc = 0; argc < m->argc; argc++) {
			atom *a = m->args[argc];
			int type = atom_type(a)->type->localtype;
			int varid = 0;

			snprintf(arg, IDLENGTH, "A%d", argc);
			a->varid = varid = newVariable(mb, arg,strlen(arg), type);
			curInstr = pushArgument(mb, curInstr, varid);
			assert(curInstr);
			if (curInstr == NULL) 
				goto cleanup;
			setVarType(mb, varid, type);
			setVarUDFtype(mb, 0);
		}
	} else if (m->params) {	/* needed for prepare statements */

		for (n = m->params->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;
			int varid = 0;

			snprintf(arg, IDLENGTH, "A%d", argc);
			varid = newVariable(mb, arg,strlen(arg), type);
			curInstr = pushArgument(mb, curInstr, varid);
			assert(curInstr);
			if (curInstr == NULL) 
				goto cleanup;
			setVarType(mb, varid, type);
			setVarUDFtype(mb, varid);
		}
	}

	if (backend_dumpstmt(be, mb, r, 1, 1, be->q?be->q->codestring:NULL) < 0) 
		goto cleanup;

	if (cq){
		SQLaddQueryToCache(c);
		// optimize this code the 'old' way
		if ( m->emode == m_prepare || !qc_isaquerytemplate(getFunctionId(getInstrPtr(c->curprg->def,0))) )
			SQLoptimizeFunction(c,c->curprg->def);
	}

	// restore the context for the wrapper code
	curPrg = c->curprg;
	if (backup)
		c->curprg = backup;
	return curPrg;

cleanup:
	freeSymbol(curPrg);
	if (backup)
		c->curprg = backup;
	return NULL;
}

void
backend_call(backend *be, Client c, cq *cq)
{
	mvc *m = be->mvc;
	InstrPtr q;
	MalBlkPtr mb = c->curprg->def;

	q = newStmt(mb, userRef, cq->name);
	/* cached (factorized queries return bit??) */
	if (cq->code && getInstrPtr(((Symbol)cq->code)->def, 0)->token == FACTORYsymbol) {
		setVarType(mb, getArg(q, 0), TYPE_bit);
		setVarUDFtype(mb, getArg(q, 0));
	} else {
		setVarType(mb, getArg(q, 0), TYPE_void);
		setVarUDFtype(mb, getArg(q, 0));
	}
	if (m->argc) {
		int i;

		for (i = 0; i < m->argc; i++) {
			atom *a = m->args[i];
			sql_subtype *pt = cq->params + i;

			if (!atom_cast(m->sa, a, pt)) {
				sql_error(m, 003, "wrong type for argument %d of " "function call: %s, expected %s\n", i + 1, atom_type(a)->type->sqlname, pt->type->sqlname);
				break;
			}
			if (atom_null(a)) {
				sql_subtype *t = cq->params + i;
				/* need type from the prepared argument */
				q = pushNil(mb, q, t->type->localtype);
			} else {
				int _t = constantAtom(be, mb, a);
				q = pushArgument(mb, q, _t);
			}
		}
	}
}

int
monet5_resolve_function(ptr M, sql_func *f)
{
	mvc *sql = (mvc *) M;
	Client c = MCgetClient(sql->clientid);
	Module m;

	/*
	   fails to search outer modules!
	   if (!findSymbol(c->nspace, f->mod, f->imp))
	   return 0;
	 */

	for (m = findModule(c->nspace, f->mod); m; m = m->link) {
		if (strcmp(m->name, f->mod) == 0) {
			Symbol s = m->space[(int) (getSymbolIndex(f->imp))];
			for (; s; s = s->peer) {
				InstrPtr sig = getSignature(s);
				int argc = sig->argc - sig->retc;

				if (strcmp(s->name, f->imp) == 0 && ((!f->ops && argc == 0) || list_length(f->ops) == argc || (sig->varargs & VARARGS) == VARARGS))
					return 1;

			}
		}
	}
	return 0;
/*
	node *n;
	newFcnCall(f->mod, f->imp);
	for (n = f->ops->h; n; n = n->next) {
		sql_arg *a = n->data;

		q = push ?type? (mb, q, a->);
	}
*/
}

static int
backend_create_r_func(backend *be, sql_func *f)
{
	(void)be;
	switch(f->type) {
	case  F_AGGR:
		f->mod = "rapi";
		f->imp = "eval_aggr";
		break;
	case  F_PROC: /* no output */
	case  F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "rapi";
		f->imp = "eval";
		break;
	}
	return 0;
}

/* Create the MAL block for a registered function and optimize it */
static int
backend_create_py_func(backend *be, sql_func *f)
{
	(void)be;
	switch(f->type) {
	case  F_AGGR:
		f->mod = "pyapi";
		f->imp = "eval_aggr";
		break;
	case F_LOADER:
		f->mod = "pyapi";
		f->imp = "eval_loader";
		break;
	case  F_PROC: /* no output */
	case  F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "pyapi";
		f->imp = "eval";
		break;
	}
	return 0;
}

static int
backend_create_map_py_func(backend *be, sql_func *f)
{
	(void)be;
	switch(f->type) {
	case  F_AGGR:
		f->mod = "pyapimap";
		f->imp = "eval_aggr";
		break;
	case  F_PROC: /* no output */
	case  F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "pyapimap";
		f->imp = "eval";
		break;
	}
	return 0;
}

static int
backend_create_sql_func(backend *be, sql_func *f, list *restypes, list *ops)
{
	mvc *m = be->mvc;
	MalBlkPtr curBlk = NULL;
	InstrPtr curInstr = NULL;
	Client c = be->client;
	Symbol backup = NULL, curPrg = NULL;
	int i, retseen = 0, sideeffects = 0, vararg = (f->varres || f->vararg), no_inline = 0;
	sql_rel *r;

	/* nothing to do for internal and ready (not recompiling) functions */
	if (!f->sql || (!vararg && f->sql > 1))
		return 0;
	if (!vararg)
		f->sql++;
	r = rel_parse(m, f->s, f->query, m_instantiate);
	if (r)
		r = rel_optimizer(m, r);
	if (r && !f->sql) 	/* native function */
		return 0;

	if (!r) {
		if (!vararg)
			f->sql--;
		return -1;
	}
	assert(r);

	backup = c->curprg;
	curPrg = c->curprg = newFunction(userRef, putName(f->base.name), FUNCTIONsymbol);
	if( curPrg == NULL)
		goto cleanup;

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	if (f->res) {
		sql_arg *res = f->res->h->data;
		if (f->type == F_UNION)
			curInstr = table_func_create_result(curBlk, curInstr, f, restypes);
		else
			setVarType(curBlk, 0, res->type.type->localtype);
	} else {
		setVarType(curBlk, 0, TYPE_void);
	}
	setVarUDFtype(curBlk, 0);

	if (f->vararg && ops) {
		int argc = 0;
		node *n;

		for (n = ops->h; n; n = n->next, argc++) {
			stmt *s = n->data;
			int type = tail_type(s)->type->localtype;
			int varid = 0;
			char buf[IDLENGTH];

			(void) snprintf(buf, IDLENGTH, "A%d", argc);
			varid = newVariable(curBlk, buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	} else if (f->ops) {
		int argc = 0;
		node *n;

		for (n = f->ops->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;
			int varid = 0;
			char buf[IDLENGTH];

			if (a->name)
				(void) snprintf(buf, IDLENGTH, "A%s", a->name);
			else
				(void) snprintf(buf, IDLENGTH, "A%d", argc);
			varid = newVariable(curBlk, buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
			setVarUDFtype(curBlk, varid);
		}
	}
	/* announce the transaction mode */
	if (backend_dumpstmt(be, curBlk, r, 0, 1, NULL) < 0) 
		goto cleanup;
	/* selectively make functions available for inlineing */
	/* for the time being we only inline scalar functions */
	/* and only if we see a single return value */
	/* check the function for side effects and make that explicit */
	sideeffects = 0;
	for (i = 1; i < curBlk->stop; i++) {
		InstrPtr p = getInstrPtr(curBlk, i);
		if (getFunctionId(p) == bindRef || getFunctionId(p) == bindidxRef)
			continue;
		sideeffects = sideeffects || hasSideEffects(curBlk, p, FALSE); 
		no_inline |= (getModuleId(p) == malRef && getFunctionId(p) == multiplexRef);
		if (p->token == RETURNsymbol || p->token == YIELDsymbol || p->barrier == RETURNsymbol || p->barrier == YIELDsymbol)
			retseen++;
	}
	if (i == curBlk->stop && retseen == 1 && f->type != F_UNION && !no_inline)
		curBlk->inlineProp =1;
	if (sideeffects)
		curBlk->unsafeProp = 1;
	/* optimize the code */
	SQLaddQueryToCache(c);
	if( curBlk->inlineProp == 0)
		SQLoptimizeFunction(c, c->curprg->def);
	else{
		chkProgram(c->fdout, c->nspace, c->curprg->def);
		SQLoptimizeFunction(c,c->curprg->def);
	}
	if (backup)
		c->curprg = backup;
	return 0;
cleanup:
	freeSymbol(curPrg);
	if (backup)
		c->curprg = backup;
	return -1;
}

/* TODO handle aggr */
int
backend_create_func(backend *be, sql_func *f, list *restypes, list *ops)
{
	switch(f->lang) {
	case FUNC_LANG_INT:
	case FUNC_LANG_MAL:
	case FUNC_LANG_SQL:
		return backend_create_sql_func(be, f, restypes, ops);
	case FUNC_LANG_R:
		return backend_create_r_func(be, f);
	case FUNC_LANG_PY:
		return backend_create_py_func(be, f);
	case FUNC_LANG_MAP_PY:
		return backend_create_map_py_func(be, f);
	case FUNC_LANG_C:
	case FUNC_LANG_J:
	default:
		return -1;
	}
}

int
backend_create_subfunc(backend *be, sql_subfunc *f, list *ops)
{
	int res;
	MalBlkPtr mb = be->mb;

	be->mb = NULL;
	res = backend_create_func(be, f->func, f->res, ops);
	be->mb = mb;
	return res;
}

int
backend_create_subaggr(backend *be, sql_subaggr *f)
{
	int res;
	MalBlkPtr mb = be->mb;

	be->mb = NULL;
	res = backend_create_func(be, f->aggr, f->res, NULL);
	be->mb = mb;
	return res;
}

void
_rel_print(mvc *sql, sql_rel *rel) 
{
	list *refs = sa_list(sql->sa);
	rel_print_refs(sql, GDKstdout, rel, 0, refs, 1);
	rel_print_(sql, GDKstdout, rel, 0, refs, 1);
	mnstr_printf(GDKstdout, "\n");
}

void
rel_print(mvc *sql, sql_rel *rel, int depth) 
{
	list *refs = sa_list(sql->sa);
	size_t pos;
	size_t nl = 0;
	size_t len = 0, lastpos = 0;
	stream *fd = sql->scanner.ws;
	stream *s;
	buffer *b = buffer_create(16364); /* hopefully enough */
	if (!b)
		return; /* signal somehow? */
	s = buffer_wastream(b, "SQL Plan");
	if (!s) {
		buffer_destroy(b);
		return; /* signal somehow? */
	}

	rel_print_refs(sql, s, rel, depth, refs, 1);
	rel_print_(sql, s, rel, depth, refs, 1);
	mnstr_printf(s, "\n");

	/* count the number of lines in the output, skip the leading \n */
	for (pos = 1; pos < b->pos; pos++) {
		if (b->buf[pos] == '\n') {
			nl++;
			if (len < pos - lastpos)
				len = pos - lastpos;
			lastpos = pos + 1;
		}
	}
	b->buf[b->pos - 1] = '\0';  /* should always end with a \n, can overwrite */

	/* craft a semi-professional header */
	mnstr_printf(fd, "&1 0 " SZFMT " 1 " SZFMT "\n", /* type id rows columns tuples */
			nl, nl);
	mnstr_printf(fd, "%% .plan # table_name\n");
	mnstr_printf(fd, "%% rel # name\n");
	mnstr_printf(fd, "%% clob # type\n");
	mnstr_printf(fd, "%% " SZFMT " # length\n", len - 1 /* remove = */);

	/* output the data */
	mnstr_printf(fd, "%s\n", b->buf + 1 /* omit starting \n */);

	mnstr_close(s);
	mnstr_destroy(s);
	buffer_destroy(b);
}

