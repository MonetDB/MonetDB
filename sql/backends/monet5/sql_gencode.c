/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal_namespace.h"
#include "opt_prelude.h"
#include "querylog.h"
#include "mal_builder.h"
#include "mal_debugger.h"

#include "rel_select.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "rel_distribute.h"
#include "rel_partition.h"
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_psm.h"
#include "rel_bin.h"
#include "rel_dump.h"
#include "rel_remote.h"

#include "msabaoth.h"		/* msab_getUUID */
#include "muuid.h"

int
constantAtom(backend *sql, MalBlkPtr mb, atom *a)
{
	int idx;
	ValPtr vr = (ValPtr) &a->data;
	ValRecord cst;

	(void) sql;
	cst.vtype = 0;
	if (VALcopy(&cst, vr) == NULL)
		return -1;
	idx = defConstant(mb, vr->vtype, &cst);
	return idx;
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
	if (is_topn(r->op) || is_sample(r->op))
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
_create_relational_function(mvc *m, const char *mod, const char *name, sql_rel *r, stmt *call, list *rel_ops, int inline_func)
{
	Client c = MCgetClient(m->clientid);
	backend *be = (backend *) c->sqlcontext;
	MalBlkPtr curBlk = 0;
	InstrPtr curInstr = 0;
	Symbol backup = NULL, curPrg = NULL;
	int res = 0;
	str msg = MAL_SUCCEED;

	backup = c->curprg;
	curPrg = c->curprg = newFunction(putName(mod), putName(name), FUNCTIONsymbol);
	if( curPrg == NULL) {
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, r);
	if( curInstr == NULL) {
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	/* ops */
	if (call && call->type == st_list) {
		node *n;
		list *ops = call->op4.lval;

		for (n = ops->h; n && !curBlk->errors; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			int type = t->type->localtype;
			int varid = 0;
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;
			char *buf;

			if (nme[0] != 'A') {
				buf = SA_NEW_ARRAY(m->sa, char, strlen(nme) + 2);
				if (buf)
					stpcpy(stpcpy(buf, "A"), nme);
			} else {
				buf = sa_strdup(m->sa, nme);
			}
			if (!buf) {
				sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return -1;
			}
			varid = newVariable(curBlk, buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
		}
	} else if (rel_ops) {
		node *n;

		for (n = rel_ops->h; n && !curBlk->errors; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			int type = t->type->localtype;
			int varid = 0;
			char *buf;

			if (e->type == e_atom) {
				buf = SA_NEW_ARRAY(m->sa, char, IDLENGTH);
				if (buf)
					snprintf(buf, IDLENGTH, "A%u", e->flag);
			} else {
				const char *nme = exp_name(e);
				buf = SA_NEW_ARRAY(m->sa, char, strlen(nme) + 2);
				if (buf)
					stpcpy(stpcpy(buf, "A"), nme);
			}
			if (!buf) {
				sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return -1;
			}
			varid = newVariable(curBlk, (char *)buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
		}
	}
	if (curBlk->errors) {
		sql_error(m, 003, SQLSTATE(42000) "Internal error while compiling statement: %s", curBlk->errors);
		return -1;
	}

	/* add return statement */
	sql_exp *e;
	r = rel_psm_stmt(m->sa, e = exp_return(m->sa,  exp_rel(m, r), 0));
	e->card = CARD_MULTI;
	if (backend_dumpstmt(be, curBlk, r, 0, 1, NULL) < 0) {
		if (backup)
			c->curprg = backup;
		return -1;
	}
	/* SQL function definitions meant for inlining should not be optimized before */
	if (inline_func)
		curBlk->inlineProp = 1;
	/* optimize the code */
	SQLaddQueryToCache(c);
	if (curBlk->inlineProp == 0 && !c->curprg->def->errors) {
		msg = SQLoptimizeQuery(c, c->curprg->def);
	} else if (curBlk->inlineProp != 0) {
		if( msg == MAL_SUCCEED)
			msg = chkProgram(c->usermodule, c->curprg->def);
		if (msg == MAL_SUCCEED && !c->curprg->def->errors)
			msg = SQLoptimizeFunction(c,c->curprg->def);
	}
	if (msg) {
		if (c->curprg->def->errors)
			GDKfree(msg);
		else
			c->curprg->def->errors = msg;
	}
	if (c->curprg->def->errors) {
		sql_error(m, 003, SQLSTATE(42000) "Internal error while compiling statement: %s", c->curprg->def->errors);
		res = -1;
	}
	if (backup)
		c->curprg = backup;
	return res;
}

static str
rel2str( mvc *sql, sql_rel *rel)
{
	buffer *b = NULL;
	stream *s = NULL;
	list *refs = NULL;
	char *res = NULL;

	b = buffer_create(1024);
	if(b == NULL)
		goto cleanup;
	s = buffer_wastream(b, "rel_dump");
	if(s == NULL)
		goto cleanup;
	refs = sa_list(sql->sa);
	if (!refs)
		goto cleanup;

	rel_print_refs(sql, s, rel, 0, refs, 0);
	rel_print_(sql, s, rel, 0, refs, 0);
	mnstr_printf(s, "\n");
	res = buffer_get_buf(b);

cleanup:
	if(b)
		buffer_destroy(b);
	if(s)
		close_stream(s);
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
	const char *local_tbl = prp->value;
	node *n;
	int i, q, v, res = 0;
	int *lret, *rret;
	char *lname;
	sql_rel *r = rel;

	if (local_tbl == NULL) {
		sql_error(m, 003, SQLSTATE(42000) "Missing property on the input relation");
		return -1;
	}

	lname = GDKstrdup(name);
	if (lname == NULL) {
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	if (is_topn(r->op) || is_sample(r->op))
		r = r->l;
	if (!is_project(r->op))
		r = rel_project(m->sa, r, rel_projections(m, r, NULL, 1, 1));
	lret = SA_NEW_ARRAY(m->sa, int, list_length(r->exps));
	if (lret == NULL) {
		GDKfree(lname);
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	rret = SA_NEW_ARRAY(m->sa, int, list_length(r->exps));
	if (rret == NULL) {
		GDKfree(lname);
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	/* create stub */
	backup = c->curprg;
	c->curprg = newFunction(putName(mod), putName(name), FUNCTIONsymbol);
	if( c->curprg == NULL) {
		GDKfree(lname);
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	lname[0] = 'l';
	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, rel);
	if( curInstr == NULL) {
		GDKfree(lname);
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	/* ops */
	if (call && call->type == st_list) {
		node *n;

		for (n = call->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			int type = t->type->localtype;
			int varid = 0;
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;
			char *buf = SA_NEW_ARRAY(m->sa, char, strlen(nme) + 2);

			if (!buf) {
				GDKfree(lname);
				sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return -1;
			}
			stpcpy(stpcpy(buf, "A"), nme);
			varid = newVariable(curBlk, buf,strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
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

	/* q := remote.connect("schema.table", "msql"); */
	p = newStmt(curBlk, remoteRef, connectRef);
	p = pushStr(curBlk, p, local_tbl);
	p = pushStr(curBlk, p, "msql");
	q = getArg(p, 0);

	/* remote.exec(q, "sql", "register", "mod", "name", "relational_plan", "signature"); */
	p = newInstructionArgs(curBlk, remoteRef, execRef, 10);
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
	if (!buf) {
		GDKfree(lname);
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	s = rel2str(m, rel);
	if (!s) {
		GDKfree(lname);
		GDKfree(buf);
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, s);	/* relational plan */
	p = pushArgument(curBlk, p, getArg(o,0));
	free(s);

	s = "";
	if (call && call->type == st_list) { /* Send existing variables in the plan */
		node *n;

		buf[0] = 0;
		for (n = call->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;

			if ((nr + 100) > len) {
				char *tmp = GDKrealloc(buf, len*=2);
				if (tmp == NULL) {
					GDKfree(buf);
					buf = NULL;
					break;
				}
				buf = tmp;
			}

			nr += snprintf(buf+nr, len-nr, "%s %s(%u,%u)%c", nme, t->type->sqlname, t->digits, t->scale, n->next?',':' ');
		}
		s = buf;
	}
	if (buf) {
		o = newFcnCall(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushStr(curBlk, o, s);	/* signature */
		p = pushArgument(curBlk, p, getArg(o,0));
	} else {
		GDKfree(lname);
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	buf[0] = 0;
	nr = 0;
	for (n = r->exps->h; n; n = n->next) { /* Send SQL types of the projection's expressions */
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);
		str next = sql_subtype_string(m->ta, t);

		if (!next) {
			GDKfree(buf);
			buf = NULL;
			break;
		}
		if ((nr + 100) > len) {
			char *tmp = GDKrealloc(buf, len*=2);
			if (tmp == NULL) {
				GDKfree(buf);
				buf = NULL;
				break;
			}
			buf = tmp;
		}

		nr += snprintf(buf+nr, len-nr, "%s%s", next, n->next?"%":"");
	}
	sa_reset(m->ta);
	if (buf) {
		o = newFcnCall(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushStr(curBlk, o, s);	/* SQL types as a single string */
		p = pushArgument(curBlk, p, getArg(o,0));
		GDKfree(buf);
	} else {
		GDKfree(lname);
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	}
	pushInstruction(curBlk, p);

	char *mal_session_uuid, *err = NULL;
	if (!GDKinmemory(0) && !GDKembedded() && (err = msab_getUUID(&mal_session_uuid)) == NULL) {
		str lsupervisor_session = GDKstrdup(mal_session_uuid);
		str rsupervisor_session = GDKstrdup(mal_session_uuid);
		free(mal_session_uuid);
		if (lsupervisor_session == NULL || rsupervisor_session == NULL) {
			GDKfree(lsupervisor_session);
			GDKfree(rsupervisor_session);
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}

		str rworker_plan_uuid = generateUUID();
		if (rworker_plan_uuid == NULL) {
			GDKfree(rsupervisor_session);
			GDKfree(lsupervisor_session);
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		str lworker_plan_uuid = GDKstrdup(rworker_plan_uuid);
		if (lworker_plan_uuid == NULL) {
			free(rworker_plan_uuid);
			GDKfree(lsupervisor_session);
			GDKfree(rsupervisor_session);
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}

		/* remote.supervisor_register(connection, supervisor_uuid, plan_uuid) */
		p = newInstruction(curBlk, remoteRef, execRef);
		p = pushArgument(curBlk, p, q);
		p = pushStr(curBlk, p, remoteRef);
		p = pushStr(curBlk, p, register_supervisorRef);
		getArg(p, 0) = -1;

		/* We don't really care about the return value of supervisor_register,
		 * but I have not found a good way to remotely execute a void mal function
		 */
		o = newFcnCall(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushInt(curBlk, o, TYPE_int);
		p = pushReturn(curBlk, p, getArg(o, 0));

		o = newFcnCall(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushStr(curBlk, o, rsupervisor_session);
		p = pushArgument(curBlk, p, getArg(o, 0));

		o = newFcnCall(curBlk, remoteRef, putRef);
		o = pushArgument(curBlk, o, q);
		o = pushStr(curBlk, o, rworker_plan_uuid);
		p = pushArgument(curBlk, p, getArg(o, 0));

		pushInstruction(curBlk, p);

		/* Execute the same instruction locally */
		p = newStmt(curBlk, remoteRef, register_supervisorRef);
		p = pushStr(curBlk, p, lsupervisor_session);
		p = pushStr(curBlk, p, lworker_plan_uuid);

		GDKfree(lworker_plan_uuid);
		free(rworker_plan_uuid);   /* This was created with strdup */
		GDKfree(lsupervisor_session);
		GDKfree(rsupervisor_session);
	} else if (err)
		free(err);

	/* (x1, x2, ..., xn) := remote.exec(q, "mod", "fcn"); */
	p = newInstructionArgs(curBlk, remoteRef, execRef, list_length(r->exps) + curInstr->argc - curInstr->retc + 4);
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

	p = newInstructionArgs(curBlk, NULL, NULL, 2 * curInstr->retc);
	p->barrier= RETURNsymbol;
	p->retc = p->argc = 0;
	for (i = 0; i < curInstr->retc; i++)
		p = pushArgument(curBlk, p, lret[i]);
	p->retc = p->argc;
	/* assignment of return */
	for (i = 0; i < curInstr->retc; i++)
		p = pushArgument(curBlk, p, lret[i]);
	pushInstruction(curBlk, p);

	/* catch exceptions */
	p = newCatchStmt(curBlk,"MALexception");
	p = newExitStmt(curBlk,"MALexception");
	p = newCatchStmt(curBlk,"SQLexception");
	p = newExitStmt(curBlk,"SQLexception");
	/* remote.disconnect(q); */
	p = newStmt(curBlk, remoteRef, disconnectRef);
	p = pushArgument(curBlk, p, q);

	pushEndInstruction(curBlk);

	/* SQL function definitions meant for inlineing should not be optimized before */
	//for now no inline of the remote function, this gives garbage collection problems
	//curBlk->inlineProp = 1;

	SQLaddQueryToCache(c);
	// (str) chkProgram(c->usermodule, c->curprg->def);
	if (!c->curprg->def->errors)
		c->curprg->def->errors = SQLoptimizeFunction(c, c->curprg->def);
	if (c->curprg->def->errors) {
		sql_error(m, 003, SQLSTATE(42000) "Internal error while compiling statement: %s", c->curprg->def->errors);
		res = -1;
	}
	if (backup)
		c->curprg = backup;
	GDKfree(lname);		/* make sure stub is called */
	return res;
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
		sql_error(c, 003, SQLSTATE(42000) "Missing relation to convert into statements");
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
backend_dumpstmt(backend *be, MalBlkPtr mb, sql_rel *r, int top, int add_end, const char *query)
{
	mvc *m = be->mvc;
	InstrPtr q, querylog = NULL;
	int old_mv = be->mvc_var;
	MalBlkPtr old_mb = be->mb;
	stmt *s;

	/* Always keep the SQL query around for monitoring */
	if (query) {
		char *escaped_q;

		while (*query && isspace((unsigned char) *query))
			query++;

		querylog = q = newStmt(mb, querylogRef, defineRef);
		if (q == NULL) {
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		setVarType(mb, getArg(q, 0), TYPE_void);
		if (!(escaped_q = sql_escape_str(m->ta, (char*) query))) {
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		q = pushStr(mb, q, escaped_q);
		if (q == NULL) {
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		q = pushStr(mb, q, getSQLoptimizer(be->mvc));
		if (q == NULL) {
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
	}

	/* announce the transaction mode */
	q = newStmt(mb, sqlRef, mvcRef);
	if (q == NULL) {
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	be->mvc_var = getDestVar(q);
	be->mb = mb;
	s = sql_relation2stmt(be, r);
	if (!s) {
		if (querylog)
			(void) pushInt(mb, querylog, mb->stop);
		return (be->mvc->errstr[0] == '\0') ? 0 : -1;
	}

	be->mvc_var = old_mv;
	be->mb = old_mb;
	if (top && !be->depth && (m->type == Q_SCHEMA || m->type == Q_TRANS) && !GDKembedded()) {
		q = newStmt(mb, sqlRef, exportOperationRef);
		if (q == NULL) {
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
	}
	/* generate a dummy return assignment for functions */
	if (getArgType(mb, getInstrPtr(mb, 0), 0) != TYPE_void && getInstrPtr(mb, mb->stop - 1)->barrier != RETURNsymbol) {
		q = newAssignment(mb);
		if (q == NULL) {
			sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		getArg(q, 0) = getArg(getInstrPtr(mb, 0), 0);
		q->barrier = RETURNsymbol;
	}
	if (add_end)
		pushEndInstruction(mb);
	if (querylog)
		(void) pushInt(mb, querylog, mb->stop);
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
	char arg[IDLENGTH];
	node *n;
	int argc, res, added_to_cache = 0;

	backup = c->curprg;
	argc = 1;
	if (m->params)
		argc += list_length(m->params);
	if (argc < MAXARG)
		argc = MAXARG;
	if (cq)
		c->curprg = newFunctionArgs(putName(sql_private_module_name), putName(cq->name), FUNCTIONsymbol, argc);
	else
		c->curprg = newFunctionArgs(putName(sql_private_module_name), "tmp", FUNCTIONsymbol, argc);
	if (c->curprg == NULL) {
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return NULL;
	}

	curPrg = c->curprg;
	curPrg->def->keephistory = backup->def->keephistory;
	mb = curPrg->def;
	curInstr = getInstrPtr(mb, 0);
	/* we do not return anything */
	setVarType(mb, 0, TYPE_void);
	setModuleId(curInstr, putName(sql_private_module_name));

	if (m->params) {	/* needed for prepare statements */

		argc = 0;
		for (n = m->params->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			sql_type *tpe = a->type.type;
			int type, varid = 0;

			if (!tpe || tpe->eclass == EC_ANY) {
				sql_error(m, 003, SQLSTATE(42000) "Could not determine type for argument number %d", argc+1);
				goto cleanup;
			}
			type = tpe->localtype;
			snprintf(arg, IDLENGTH, "A%d", argc);
			varid = newVariable(mb, arg,strlen(arg), type);
			curInstr = pushArgument(mb, curInstr, varid);
			if (c->curprg == NULL) {
				sql_error(m, 003, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			if (mb->errors) {
				sql_error(m, 003, SQLSTATE(42000) "Internal error while compiling statement: %s", mb->errors);
				goto cleanup;
			}
			setVarType(mb, varid, type);
		}
	}

	if ((res = backend_dumpstmt(be, mb, r, 1, 1, be->q ? be->q->f->query : NULL)) < 0)
		goto cleanup;

	if (cq) {
		SQLaddQueryToCache(c);
		added_to_cache = 1;
		// optimize this code the 'old' way
		if (m->emode == m_prepare && !c->curprg->def->errors)
			c->curprg->def->errors = SQLoptimizeFunction(c,c->curprg->def);
	}
	if (c->curprg->def->errors) {
		sql_error(m, 003, SQLSTATE(42000) "Internal error while compiling statement: %s", c->curprg->def->errors);
		goto cleanup;
	}

	// restore the context for the wrapper code
	curPrg = c->curprg;
	if (backup)
		c->curprg = backup;
	return curPrg;

cleanup:
	if (!added_to_cache)
		freeSymbol(curPrg);
	if (backup)
		c->curprg = backup;
	return NULL;
}

int
monet5_resolve_function(ptr M, sql_func *f)
{
	Client c;
	Module m;
	int clientID = *(int*) M;
	const char *mname = putName(f->mod), *fname = putName(f->imp);

	if (!mname || !fname)
		return 0;

	/* Some SQL functions MAL mapping such as count(*) aggregate, the number of arguments don't match */
	if (mname == calcRef && fname == getName("="))
		return 1;
	if (mname == aggrRef && (fname == countRef || fname == count_no_nilRef))
		return 1;
	if (f->type == F_ANALYTIC)
		return 1;

	c = MCgetClient(clientID);
	for (m = findModule(c->usermodule, mname); m; m = m->link) {
		for (Symbol s = findSymbolInModule(m, fname); s; s = s->peer) {
			InstrPtr sig = getSignature(s);
			int argc = sig->argc - sig->retc, nfargs = list_length(f->ops), nfres = list_length(f->res);

			if ((sig->varargs & VARARGS) == VARARGS || f->vararg || f->varres)
				return 1;
			else if (nfargs == argc && (nfres == sig->retc || (sig->retc == 1 && (IS_FILT(f) || IS_PROC(f))))) {
				/* I removed this code because, it was triggering many errors on te SQL <-> MAL translation */
				/* Check for types of inputs and outputs. SQL procedures and filter functions always return 1 value in the MAL implementation
				bool all_match = true;
				if (nfres != 0) { if function has output variables, test types are equivalent
					int i = 0;
					for (node *n = f->res->h; n && all_match; n = n->next, i++) {
						sql_arg *arg = (sql_arg *) n->data;
						int nsql_tpe = arg->type.type->localtype;
						int nmal_tpe = getArgType(s->def, sig, i);
						if (isaBatType(nmal_tpe) || (nmal_tpe & 0377) == TYPE_any) any type is excluded from isaBatType
							nmal_tpe = getBatType(nmal_tpe);

						 any/void types allways match
						if (nsql_tpe != TYPE_any && nmal_tpe != TYPE_any && nsql_tpe != TYPE_void && nmal_tpe != TYPE_void)
							all_match = nsql_tpe == nmal_tpe;
					}
				}

				if (all_match && nfargs != 0) {  if function has arguments, test types are equivalent
					int i = sig->retc;
					for (node *n = f->ops->h; n && all_match; n = n->next, i++) {
						sql_arg *arg = (sql_arg *) n->data;
						int nsql_tpe = arg->type.type->localtype;
						int nmal_tpe = getArgType(s->def, sig, i);
						if (isaBatType(nmal_tpe) || (nmal_tpe & 0377) == TYPE_any)  any type is excluded from isaBatType
							nmal_tpe = getBatType(nmal_tpe);

						 any/void types allways match
						if (nsql_tpe != TYPE_any && nmal_tpe != TYPE_any && nsql_tpe != TYPE_void && nmal_tpe != TYPE_void)
							all_match = nsql_tpe == nmal_tpe;
					}
				}
				if (all_match)*/
					return 1;
			}
		}
	}
	return 0;
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
		f->mod = "pyapi3";
		f->imp = "eval_aggr";
		break;
	case F_LOADER:
		f->mod = "pyapi3";
		f->imp = "eval_loader";
		break;
	case  F_PROC: /* no output */
	case  F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "pyapi3";
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
		f->mod = "pyapi3map";
		f->imp = "eval_aggr";
		break;
	case  F_PROC: /* no output */
	case  F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "pyapi3map";
		f->imp = "eval";
		break;
	}
	return 0;
}

static int
backend_create_py3_func(backend *be, sql_func *f)
{
	backend_create_py_func(be, f);
	f->mod = "pyapi3";
	return 0;
}

static int
backend_create_map_py3_func(backend *be, sql_func *f)
{
	backend_create_map_py_func(be, f);
	f->mod = "pyapi3map";
	return 0;
}

/* Create the MAL block for a registered function and optimize it */
static int
backend_create_c_func(backend *be, sql_func *f)
{
	(void)be;
	switch(f->type) {
	case  F_AGGR:
		f->mod = "capi";
		f->imp = "eval_aggr";
		break;
	case F_LOADER:
	case F_PROC: /* no output */
	case F_FUNC:
	default: /* ie also F_FILT and F_UNION for now */
		f->mod = "capi";
		f->imp = "eval";
		break;
	}
	return 0;
}

/* Parse the SQL query from the function, and extract the MAL function from the generated abstract syntax tree */
static int
mal_function_find_implementation_address(mvc *m, sql_func *f)
{
	mvc *o = m;
	buffer *b = NULL;
	bstream *bs = NULL;
	stream *buf = NULL;
	char *n = NULL;
	int len = _strlen(f->query);
	sql_schema *s = cur_schema(m);
	dlist *l, *ext_name;

	if (!(m = ZNEW(mvc))) {
		(void) sql_error(o, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	m->type = Q_PARSE;
	m->user_id = m->role_id = USER_MONETDB;

	store_lock(m->session->tr->store);
	m->session = sql_session_create(m->store, m->pa, 0);
	store_unlock(m->session->tr->store);
	if (!m->session) {
		(void) sql_error(o, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (s)
		m->session->schema = s;
	if (!(m->sa = sa_create(NULL))) {
		(void) sql_error(o, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = (buffer*)GDKmalloc(sizeof(buffer)))) {
		(void) sql_error(o, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(n = GDKmalloc(len + 2))) {
		(void) sql_error(o, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	snprintf(n, len + 2, "%s\n", f->query);
	len++;
	buffer_init(b, n, len);
	if (!(buf = buffer_rastream(b, "sqlstatement"))) {
		(void) sql_error(o, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(bs = bstream_create(buf, b->len))) {
		(void) sql_error(o, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	scanner_init(&m->scanner, bs, NULL);
	m->scanner.mode = LINE_1;
	bstream_next(m->scanner.rs);

	(void) sqlparse(m); /* blindly ignore errors */
	assert(m->sym->token == SQL_CREATE_FUNC);
	l = m->sym->data.lval;
	ext_name = l->h->next->next->next->data.lval;
	f->imp = sa_strdup(f->sa, qname_schema_object(ext_name)); /* found the implementation, set it */

bailout:
	if (m) {
		bstream_destroy(m->scanner.rs);
		if (m->session) {
			store_lock(m->session->tr->store);
			sql_session_destroy(m->session);
			store_unlock(m->session->tr->store);
		}
		if (m->sa)
			sa_destroy(m->sa);
		_DELETE(m);
	}
	m = o;
	if (n)
		GDKfree(n);
	if (b)
		GDKfree(b);
	return m->errstr[0] == '\0'; /* m was set back to o */
}

static int
backend_create_sql_func(backend *be, sql_func *f, list *restypes, list *ops)
{
	mvc *m = be->mvc;
	MalBlkPtr curBlk = NULL;
	InstrPtr curInstr = NULL;
	Client c = be->client;
	Symbol backup = NULL, curPrg = NULL;
	int i, retseen = 0, sideeffects = 0, vararg = (f->varres || f->vararg), no_inline = 0, clientid = be->mvc->clientid;
	sql_rel *r;
	str msg = MAL_SUCCEED;

	/* nothing to do for internal and ready (not recompiling) functions, besides finding respective MAL implementation */
	if (!f->sql && (f->lang == FUNC_LANG_INT || f->lang == FUNC_LANG_MAL)) {
		if (f->lang == FUNC_LANG_MAL && !f->imp && !mal_function_find_implementation_address(m, f))
			return -1;
		if (!backend_resolve_function(&clientid, f)) {
			if (f->lang == FUNC_LANG_INT)
				(void) sql_error(m, 02, SQLSTATE(HY005) "Implementation for function %s.%s not found", f->mod, f->imp);
			else
				(void) sql_error(m, 02, SQLSTATE(HY005) "Implementation for function %s.%s not found (%s.%s)", f->mod, f->imp, f->s->base.name, f->base.name);
			return -1;
		}
	}
	if (!f->sql || (!vararg && f->sql > 1))
		return 0;
	if (!vararg)
		f->sql++;
	r = rel_parse(m, f->s, f->query, m_instantiate);
	if (r)
		r = sql_processrelation(m, r, 1, 0);
	if (r)
		r = rel_distribute(m, r);
	if (r)
		r = rel_partition(m, r);
	if (r && !f->sql) 	/* native function */
		return 0;

	if (!r) {
		if (!vararg)
			f->sql--;
		return -1;
	}

	backup = c->curprg;
	curPrg = c->curprg = newFunctionArgs(putName(sql_shared_module_name), putName(f->base.name), FUNCTIONsymbol, (f->res && f->type == F_UNION ? list_length(f->res) : 1) + (f->vararg && ops ? list_length(ops) : f->ops ? list_length(f->ops) : 0));
	if( curPrg == NULL) {
		sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	if (f->res) {
		sql_arg *res = f->res->h->data;
		if (f->type == F_UNION) {
			curInstr = table_func_create_result(curBlk, curInstr, f, restypes);
			if( curInstr == NULL) {
				sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
		} else {
			setArgType(curBlk, curInstr, 0, res->type.type->localtype);
		}
	} else {
		setArgType(curBlk, curInstr, 0, TYPE_void);
	}

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
		}
	} else if (f->ops) {
		int argc = 0;
		node *n;

		for (n = f->ops->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			int type = a->type.type->localtype;
			int varid = 0;
			char *buf;

			if (a->name) {
				buf = SA_NEW_ARRAY(m->sa, char, strlen(a->name) + 4);
				if (buf)
					stpcpy(stpcpy(buf, "A1%"), a->name);  /* mangle variable name */
			} else {
				buf = SA_NEW_ARRAY(m->sa, char, IDLENGTH);
				if (buf)
					(void) snprintf(buf, IDLENGTH, "A%d", argc);
			}
			if (!buf) {
				sql_error(m, 001, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto cleanup;
			}
			varid = newVariable(curBlk, buf, strlen(buf), type);
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
		}
	}
	/* announce the transaction mode */
	if (backend_dumpstmt(be, curBlk, r, 0, 1, NULL) < 0)
		goto cleanup;
	/* selectively make functions available for inlineing */
	/* for the time being we only inline scalar functions */
	/* and only if we see a single return value */
	/* check the function for side effects and make that explicit */
	sideeffects = f->side_effect;
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
		curBlk->inlineProp = 1;
	if (sideeffects)
		curBlk->unsafeProp = 1;
	/* optimize the code */
	SQLaddQueryToCache(c);
	if (curBlk->inlineProp == 0 && !c->curprg->def->errors) {
		msg = SQLoptimizeFunction(c, c->curprg->def);
	} else if (curBlk->inlineProp != 0) {
		if( msg == MAL_SUCCEED)
			msg = chkProgram(c->usermodule, c->curprg->def);
		if (msg == MAL_SUCCEED && !c->curprg->def->errors)
			msg = SQLoptimizeFunction(c,c->curprg->def);
	}
	if (msg) {
		if (c->curprg->def->errors)
			GDKfree(msg);
		else
			c->curprg->def->errors = msg;
	}
	if (c->curprg->def->errors) {
		sql_error(m, 003, SQLSTATE(42000) "Internal error while compiling statement: %s", c->curprg->def->errors);
		goto cleanup;
	}
	if (backup)
		c->curprg = backup;
	return 0;
cleanup:
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
	case FUNC_LANG_PY3:
		return backend_create_py3_func(be, f);
	case FUNC_LANG_MAP_PY3:
		return backend_create_map_py3_func(be, f);
	case FUNC_LANG_C:
	case FUNC_LANG_CPP:
		return backend_create_c_func(be, f);
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
backend_create_subaggr(backend *be, sql_subfunc *f)
{
	int res;
	MalBlkPtr mb = be->mb;

	be->mb = NULL;
	res = backend_create_func(be, f->func, f->res, NULL);
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
	mnstr_printf(fd, "&1 0 %zu 1 %zu\n", /* type id rows columns tuples */
			nl, nl);
	mnstr_printf(fd, "%% .plan # table_name\n");
	mnstr_printf(fd, "%% rel # name\n");
	mnstr_printf(fd, "%% clob # type\n");
	mnstr_printf(fd, "%% %zu # length\n", len - 1 /* remove = */);

	/* output the data */
	mnstr_printf(fd, "%s\n", b->buf + 1 /* omit starting \n */);

	close_stream(s);
	buffer_destroy(b);
}
