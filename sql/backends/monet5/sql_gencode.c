/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_psm.h"
#include "rel_bin.h"
#include "rel_dump.h"

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
	Symbol symbackup = NULL;
	int res = 0, added_to_cache = 0;
	str msg = MAL_SUCCEED;
	backend bebackup;

	if (strlen(mod) >= IDLENGTH) {
		(void) sql_error(m, 10, SQLSTATE(42000) "Module name '%s' too large for the backend", mod);
		return -1;
	}
	if (strlen(name) >= IDLENGTH) {
		(void) sql_error(m, 10, SQLSTATE(42000) "Function name '%s' too large for the backend", name);
		return -1;
	}
	symbackup = c->curprg;
	memcpy(&bebackup, be, sizeof(backend)); /* backup current backend */
	backend_reset(be);

	c->curprg = newFunction(putName(mod), putName(name), FUNCTIONsymbol);
	if(c->curprg  == NULL) {
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		res = -1;
		goto cleanup;
	}

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, r);
	if( curInstr == NULL) {
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		res = -1;
		goto cleanup;
	}

	/* ops */
	if (call && call->type == st_list) {
		list *ops = call->op4.lval;

		for (node *n = ops->h; n && !curBlk->errors; n = n->next) {
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
				sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				res = -1;
				goto cleanup;
			}
			if ((varid = newVariable(curBlk, buf, strlen(buf), type)) < 0) {
				sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: variable id too long");
				res = -1;
				goto cleanup;
			}
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
		}
	} else if (rel_ops) {
		for (node *n = rel_ops->h; n && !curBlk->errors; n = n->next) {
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
				sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				res = -1;
				goto cleanup;
			}
			if ((varid = newVariable(curBlk, (char *)buf, strlen(buf), type)) < 0) {
				sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: variable id too long");
				res = -1;
				goto cleanup;
			}
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
		}
	}
	if (curBlk->errors) {
		sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: %s", curBlk->errors);
		res = -1;
		goto cleanup;
	}

	/* add return statement */
	sql_exp *e;
	r = rel_psm_stmt(m->sa, e = exp_return(m->sa,  exp_rel(m, r), 0));
	e->card = CARD_MULTI;
	if ((res = backend_dumpstmt(be, curBlk, r, 0, 1, NULL) < 0))
		goto cleanup;
	/* SQL function definitions meant for inlining should not be optimized before */
	if (inline_func)
		curBlk->inlineProp = 1;
	/* optimize the code */
	SQLaddQueryToCache(c);
	added_to_cache = 1;
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
			freeException(msg);
		else
			c->curprg->def->errors = msg;
	}
	if (c->curprg->def->errors) {
		sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: %s", c->curprg->def->errors);
		res = -1;
	}

cleanup:
	if (res < 0) {
		if (!added_to_cache)
			freeSymbol(c->curprg);
		else
			SQLremoveQueryFromCache(c);
	}
	memcpy(be, &bebackup, sizeof(backend));
	c->curprg = symbackup;
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
	int i, q, v, res = 0, added_to_cache = 0,  *lret, *rret;
	size_t len = 1024, nr;
	char *lname, *buf;
	sql_rel *r = rel;

	if (local_tbl == NULL) {
		sql_error(m, 003, SQLSTATE(42000) "Missing property on the input relation");
		return -1;
	}
	if (strlen(mod) >= IDLENGTH) {
		sql_error(m, 10, SQLSTATE(42000) "Module name '%s' too large for the backend", mod);
		return -1;
	}
	if (strlen(name) >= IDLENGTH) {
		sql_error(m, 10, SQLSTATE(42000) "Function name '%s' too large for the backend", name);
		return -1;
	}

	lname = GDKstrdup(name);
	if (lname == NULL) {
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	if (is_topn(r->op) || is_sample(r->op))
		r = r->l;
	if (!is_project(r->op))
		r = rel_project(m->sa, r, rel_projections(m, r, NULL, 1, 1));
	lret = SA_NEW_ARRAY(m->sa, int, list_length(r->exps));
	if (lret == NULL) {
		GDKfree(lname);
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	rret = SA_NEW_ARRAY(m->sa, int, list_length(r->exps));
	if (rret == NULL) {
		GDKfree(lname);
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	/* create stub */
	backup = c->curprg;
	c->curprg = newFunction(putName(mod), putName(name), FUNCTIONsymbol);
	if( c->curprg == NULL) {
		GDKfree(lname);
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	lname[0] = 'l';
	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	curInstr = relational_func_create_result(m, curBlk, curInstr, rel);
	if( curInstr == NULL) {
		GDKfree(lname);
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	/* ops */
	if (call && call->type == st_list) {
		char nbuf[IDLENGTH];
		int i = 0;

		for (node *n = call->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			int type = t->type->localtype, varid = 0;

			sprintf(nbuf, "A%d", i++);
			if ((varid = newVariable(curBlk, nbuf, strlen(nbuf), type)) < 0) {
				GDKfree(lname);
				sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: variable id too long");
				return -1;
			}
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
		}
	}

	/* declare return variables */
	if (!list_empty(r->exps)) {
		for (i = 0, n = r->exps->h; n; n = n->next, i++) {
			sql_exp *e = n->data;
			int type = exp_subtype(e)->type->localtype;

			type = newBatType(type);
			p = newFcnCall(curBlk, batRef, newRef);
			p = pushType(curBlk, p, getBatType(type));
			setArgType(curBlk, p, 0, type);
			lret[i] = getArg(p, 0);
		}
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

	if (!(buf = rel2str(m, rel))) {
		GDKfree(lname);
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, buf);	/* relational plan */
	p = pushArgument(curBlk, p, getArg(o,0));
	free(buf);

	if (!(buf = GDKmalloc(len))) {
		GDKfree(lname);
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}

	buf[0] = 0;
	if (call && call->type == st_list) { /* Send existing variables in the plan */
		char dbuf[32], sbuf[32];

		nr = 0;
		for (node *n = call->op4.lval->h; n; n = n->next) {
			stmt *op = n->data;
			sql_subtype *t = tail_type(op);
			const char *nme = (op->op3)?op->op3->op4.aval->data.val.sval:op->cname;

			sprintf(dbuf, "%u", t->digits);
			sprintf(sbuf, "%u", t->scale);
			size_t nlen = strlen(nme) + strlen(t->type->base.name) + strlen(dbuf) + strlen(sbuf) + 6;

			if ((nr + nlen) > len) {
				len = (len + nlen) * 2;
				char *tmp = GDKrealloc(buf, len);
				if (tmp == NULL) {
					GDKfree(lname);
					GDKfree(buf);
					sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					return -1;
				}
				buf = tmp;
			}

			nr += snprintf(buf+nr, len-nr, "%s %s(%s,%s)%c", nme, t->type->base.name, dbuf, sbuf, n->next?',':' ');
		}
	}
	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, buf);	/* signature */
	p = pushArgument(curBlk, p, getArg(o,0));

	buf[0] = 0;
	if (!list_empty(r->exps)) {
		nr = 0;
		for (n = r->exps->h; n; n = n->next) { /* Send SQL types of the projection's expressions */
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			str next = sql_subtype_string(m->ta, t);

			if (!next) {
				GDKfree(lname);
				GDKfree(buf);
				sa_reset(m->ta);
				sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return -1;
			}

			size_t nlen = strlen(next) + 2;
			if ((nr + nlen) > len) {
				len = (len + nlen) * 2;
				char *tmp = GDKrealloc(buf, len);
				if (tmp == NULL) {
					GDKfree(lname);
					GDKfree(buf);
					sa_reset(m->ta);
					sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					return -1;
				}
				buf = tmp;
			}

			nr += snprintf(buf+nr, len-nr, "%s%s", next, n->next?"%":"");
		}
		sa_reset(m->ta);
	}
	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushStr(curBlk, o, buf);	/* SQL types as a single string */
	GDKfree(buf);
	p = pushArgument(curBlk, p, getArg(o,0));
	pushInstruction(curBlk, p);

	char *mal_session_uuid, *err = NULL;
	if (!GDKinmemory(0) && !GDKembedded() && (err = msab_getUUID(&mal_session_uuid)) == NULL) {
		str lsupervisor_session = GDKstrdup(mal_session_uuid);
		str rsupervisor_session = GDKstrdup(mal_session_uuid);
		free(mal_session_uuid);
		if (lsupervisor_session == NULL || rsupervisor_session == NULL) {
			GDKfree(lsupervisor_session);
			GDKfree(rsupervisor_session);
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}

		str rworker_plan_uuid = generateUUID();
		if (rworker_plan_uuid == NULL) {
			GDKfree(rsupervisor_session);
			GDKfree(lsupervisor_session);
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		str lworker_plan_uuid = GDKstrdup(rworker_plan_uuid);
		if (lworker_plan_uuid == NULL) {
			free(rworker_plan_uuid);
			GDKfree(lsupervisor_session);
			GDKfree(rsupervisor_session);
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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

	if (!list_empty(r->exps)) {
		for (i = 0, n = r->exps->h; n; n = n->next, i++) {
			/* x1 := remote.put(q, :type) */
			o = newFcnCall(curBlk, remoteRef, putRef);
			o = pushArgument(curBlk, o, q);
			o = pushArgument(curBlk, o, lret[i]);
			v = getArg(o, 0);
			p = pushReturn(curBlk, p, v);
			rret[i] = v;
		}
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

	/* end remote transaction */
	p = newInstruction(curBlk, remoteRef, execRef);
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, sqlRef);
	p = pushStr(curBlk, p, deregisterRef);
	getArg(p, 0) = -1;

	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushInt(curBlk, o, TYPE_int);
	p = pushReturn(curBlk, p, getArg(o, 0));
	pushInstruction(curBlk, p);

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
	p = newCatchStmt(curBlk, "ANYexception");
	p = newExitStmt(curBlk, "ANYexception");

	/* end remote transaction */
	p = newInstruction(curBlk, remoteRef, execRef);
	p = pushArgument(curBlk, p, q);
	p = pushStr(curBlk, p, sqlRef);
	p = pushStr(curBlk, p, deregisterRef);
	getArg(p, 0) = -1;

	o = newFcnCall(curBlk, remoteRef, putRef);
	o = pushArgument(curBlk, o, q);
	o = pushInt(curBlk, o, TYPE_int);
	p = pushReturn(curBlk, p, getArg(o, 0));
	pushInstruction(curBlk, p);

	/* remote.disconnect(q); */
	p = newStmt(curBlk, remoteRef, disconnectRef);
	p = pushArgument(curBlk, p, q);

	/* the connection may not start (eg bad credentials),
		so calling 'disconnect' on the catch block may throw another exception, add another catch */
	p = newCatchStmt(curBlk, "ANYexception");
	p = newExitStmt(curBlk, "ANYexception");

	/* throw the exception back */
	p = newRaiseStmt(curBlk, "RemoteException");
	p = pushStr(curBlk, p, "Exception occurred in the remote server, please check the log there");

	pushEndInstruction(curBlk);

	/* SQL function definitions meant for inlineing should not be optimized before */
	//for now no inline of the remote function, this gives garbage collection problems
	//curBlk->inlineProp = 1;

	SQLaddQueryToCache(c);
	added_to_cache = 1;
	// (str) chkProgram(c->usermodule, c->curprg->def);
	if (!c->curprg->def->errors)
		c->curprg->def->errors = SQLoptimizeFunction(c, c->curprg->def);
	if (c->curprg->def->errors) {
		sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: %s", c->curprg->def->errors);
		res = -1;
	}

	GDKfree(lname);	/* make sure stub is called */
	if (res < 0) {
		if (!added_to_cache) /* on error, remove generated symbol from cache */
			freeSymbol(c->curprg);
		else
			SQLremoveQueryFromCache(c);
	}
	c->curprg = backup;
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
sql_relation2stmt(backend *be, sql_rel *r, int top)
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
			s = output_rel_bin(be, r, top);
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

	/* Always keep the SQL query around for monitoring */
	if (query) {
		char *escaped_q;

		while (*query && isspace((unsigned char) *query))
			query++;

		querylog = q = newStmt(mb, querylogRef, defineRef);
		if (q == NULL) {
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		setVarType(mb, getArg(q, 0), TYPE_void);
		if (!(escaped_q = sql_escape_str(m->ta, (char*) query))) {
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		q = pushStr(mb, q, escaped_q);
		if (q == NULL) {
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
		q = pushStr(mb, q, getSQLoptimizer(be->mvc));
		if (q == NULL) {
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
	}

	/* announce the transaction mode */
	q = newStmt(mb, sqlRef, mvcRef);
	if (q == NULL) {
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return -1;
	}
	be->mvc_var = getDestVar(q);
	be->mb = mb;
	if (!sql_relation2stmt(be, r, top)) {
		if (querylog)
			(void) pushInt(mb, querylog, mb->stop);
		return (be->mvc->errstr[0] == '\0') ? 0 : -1;
	}

	be->mvc_var = old_mv;
	be->mb = old_mb;
	if (top && !be->depth && (m->type == Q_SCHEMA || m->type == Q_TRANS) && !GDKembedded()) {
		q = newStmt(mb, sqlRef, exportOperationRef);
		if (q == NULL) {
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			return -1;
		}
	}
	/* generate a dummy return assignment for functions */
	if (getArgType(mb, getInstrPtr(mb, 0), 0) != TYPE_void && getInstrPtr(mb, mb->stop - 1)->barrier != RETURNsymbol) {
		q = newAssignment(mb);
		if (q == NULL) {
			sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
int
backend_dumpproc(backend *be, Client c, cq *cq, sql_rel *r)
{
	mvc *m = be->mvc;
	MalBlkPtr mb = 0;
	Symbol symbackup = NULL;
	InstrPtr curInstr = 0;
	char arg[IDLENGTH];
	int argc = 1, res = 0, added_to_cache = 0;
	backend bebackup;
	const char *sql_private_module = putName(sql_private_module_name);

	symbackup = c->curprg;
	memcpy(&bebackup, be, sizeof(backend)); /* backup current backend */
	backend_reset(be);

	if (m->params)
		argc += list_length(m->params);
	if (argc < MAXARG)
		argc = MAXARG;
	assert(cq && strlen(cq->name) < IDLENGTH);
	c->curprg = newFunctionArgs(sql_private_module, putName(cq->name), FUNCTIONsymbol, argc);
	if (c->curprg == NULL) {
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		res = -1;
		goto cleanup;
	}

	mb = c->curprg->def;
	curInstr = getInstrPtr(mb, 0);
	/* we do not return anything */
	setVarType(mb, 0, TYPE_void);
	setModuleId(curInstr, sql_private_module);

	if (m->params) {	/* needed for prepare statements */
		argc = 0;
		for (node *n = m->params->h; n; n = n->next, argc++) {
			sql_arg *a = n->data;
			sql_type *tpe = a->type.type;
			int type, varid = 0;

			if (!tpe || tpe->eclass == EC_ANY) {
				sql_error(m, 10, SQLSTATE(42000) "Could not determine type for argument number %d", argc+1);
				res = -1;
				goto cleanup;
			}
			type = tpe->localtype;
			snprintf(arg, IDLENGTH, "A%d", argc);
			if ((varid = newVariable(mb, arg,strlen(arg), type)) < 0) {
				sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: variable id too long");
				res = -1;
				goto cleanup;
			}
			curInstr = pushArgument(mb, curInstr, varid);
			if (c->curprg == NULL) {
				sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				res = -1;
				goto cleanup;
			}
			if (mb->errors) {
				sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: %s", mb->errors);
				res = -1;
				goto cleanup;
			}
			setVarType(mb, varid, type);
		}
	}

	if ((res = backend_dumpstmt(be, mb, r, m->emode == m_prepare, 1, be->q ? be->q->f->query : NULL)) < 0)
		goto cleanup;

	SQLaddQueryToCache(c);
	added_to_cache = 1;
	// optimize this code the 'old' way
	if (m->emode == m_prepare && !c->curprg->def->errors)
		c->curprg->def->errors = SQLoptimizeFunction(c,c->curprg->def);
	if (c->curprg->def->errors) {
		sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: %s", c->curprg->def->errors);
		res = -1;
		goto cleanup;
	}

	// restore the context for the wrapper code
cleanup:
	if (res < 0) {
		if (!added_to_cache)
			freeSymbol(c->curprg);
		else
			SQLremoveQueryFromCache(c);
	}
	memcpy(be, &bebackup, sizeof(backend));
	c->curprg = symbackup;
	return res;
}

int
monet5_has_module(ptr M, char *module)
{
	Client c;
	int clientID = *(int*) M;
	c = MCgetClient(clientID);

	Module m = findModule(c->usermodule, putName(module));
	if (m && m != c->usermodule)
		return 1;
	return 0;
}

static MT_Lock sql_gencodeLock = MT_LOCK_INITIALIZER(sql_gencodeLock);

static str
monet5_cache_remove(Module m, const char *nme)
{
	/* Warning, this function doesn't do any locks, so be careful with concurrent symbol insert/deletes */
	Symbol s = findSymbolInModule(m, nme);
	if (s == NULL)
		throw(MAL, "cache.remove", SQLSTATE(42000) "internal error, symbol missing\n");
	deleteSymbol(m, s);
	return MAL_SUCCEED;
}

/* if 'mod' not NULL, use it otherwise get the module from the client id */
void
monet5_freecode(const char *mod, int clientid, const char *name)
{
	Module m = NULL;
	str msg = MAL_SUCCEED;

	if (mod) {
		m = getModule(putName(mod));
	} else {
		Client c = MCgetClient(clientid);
		if (c)
			m = c->usermodule;
	}
	if (m) {
		if (mod)
			MT_lock_set(&sql_gencodeLock);
		msg = monet5_cache_remove(m, name);
		if (mod)
			MT_lock_unset(&sql_gencodeLock);
		freeException(msg); /* do something with error? */
	}
}

/* the function 'f' may not have the 'imp' field set yet */
int
monet5_resolve_function(ptr M, sql_func *f, const char *fimp, bit *side_effect)
{
	Client c;
	Module m;
	int clientID = *(int*) M;
	const char *mname = putName(sql_func_mod(f)), *fname = putName(fimp);

	if (!mname || !fname)
		return 0;

	/* Some SQL functions MAL mapping such as count(*) aggregate, the number of arguments don't match */
	if (mname == calcRef && fname == getName("=")) {
		*side_effect = 0;
		return 1;
	}
	if (mname == aggrRef && (fname == countRef || fname == count_no_nilRef)) {
		*side_effect = 0;
		return 1;
	}
	if (f->type == F_ANALYTIC) {
		*side_effect = 0;
		return 1;
	}

	c = MCgetClient(clientID);
	MT_lock_set(&sql_gencodeLock);
	for (m = findModule(c->usermodule, mname); m; m = m->link) {
		for (Symbol s = findSymbolInModule(m, fname); s; s = s->peer) {
			InstrPtr sig = getSignature(s);
			int argc = sig->argc - sig->retc, nfargs = list_length(f->ops), nfres = list_length(f->res);

			if ((sig->varargs & VARARGS) == VARARGS || f->vararg || f->varres) {
				*side_effect = (bit) s->def->unsafeProp;
				MT_lock_unset(&sql_gencodeLock);
				return 1;
			} else if (nfargs == argc && (nfres == sig->retc || (sig->retc == 1 && (IS_FILT(f) || IS_PROC(f))))) {
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
				*side_effect = (bit) s->def->unsafeProp;
				MT_lock_unset(&sql_gencodeLock);
				return 1;
			}
		}
	}
	MT_lock_unset(&sql_gencodeLock);
	return 0;
}

/* Parse the SQL query from the function, and extract the MAL function from the generated abstract syntax tree */
static str
mal_function_find_implementation_address(mvc *m, sql_func *f)
{
	buffer *b = NULL;
	bstream *bs = NULL;
	stream *buf = NULL;
	char *n = NULL;
	int len = _strlen(f->query);
	dlist *l, *ext_name;
	str fimp = NULL;

	if (!(b = (buffer*)malloc(sizeof(buffer))))
		return sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (!(n = malloc(len + 2))) {
		free(b);
		return sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	snprintf(n, len + 2, "%s\n", f->query);
	len++;
	buffer_init(b, n, len);
	if (!(buf = buffer_rastream(b, "sqlstatement"))) {
		buffer_destroy(b);
		return sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (!(bs = bstream_create(buf, b->len))) {
		buffer_destroy(b);
		return sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	mvc o = *m;
	scanner_init(&m->scanner, bs, NULL);
	m->scanner.mode = LINE_1;
	bstream_next(m->scanner.rs);

	m->type = Q_PARSE;
	m->user_id = m->role_id = USER_MONETDB;
	m->params = NULL;
	m->sym = NULL;
	m->errstr[0] = '\0';
	m->session->status = 0;
	(void) sqlparse(m);
	if (m->session->status || m->errstr[0] || !m->sym || m->sym->token != SQL_CREATE_FUNC) {
		if (m->errstr[0] == '\0')
			(void) sql_error(m, 10, SQLSTATE(42000) "Could not parse CREATE SQL MAL function statement");
	} else {
		l = m->sym->data.lval;
		ext_name = l->h->next->next->next->data.lval;
		const char *imp = qname_schema_object(ext_name);

		if (strlen(imp) >= IDLENGTH)
			(void) sql_error(m, 10, SQLSTATE(42000) "MAL function name '%s' too large for the backend", imp);
		else if (!(fimp = _STRDUP(imp))) /* found the implementation, set it */
			(void) sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	buffer_destroy(b);
	bstream_destroy(m->scanner.rs);

	m->sym = NULL;
	o.frames = m->frames;	/* may have been realloc'ed */
	o.sizeframes = m->sizeframes;
	if (m->session->status || m->errstr[0]) {
		int status = m->session->status;

		strcpy(o.errstr, m->errstr);
		*m = o;
		m->session->status = status;
	} else {
		unsigned int label = m->label;

		while (m->topframes > o.topframes)
			clear_frame(m, m->frames[--m->topframes]);
		*m = o;
		m->label = label;
	}
	return fimp;
}

int
backend_create_mal_func(mvc *m, sql_func *f)
{
	char *F = NULL, *fn = NULL;
	bit old_side_effect = f->side_effect, new_side_effect = 0;
	int clientid = m->clientid;
	str fimp = NULL;

	if (f->instantiated)
		return 0;
	FUNC_TYPE_STR(f->type, F, fn)
	(void) F;
	if (strlen(f->mod) >= IDLENGTH) {
		(void) sql_error(m, 10, SQLSTATE(42000) "MAL module name '%s' too large for the backend", f->mod);
		return -1;
	}
	if (!(fimp = mal_function_find_implementation_address(m, f)))
		return -1;
	if (!backend_resolve_function(&clientid, f, fimp, &new_side_effect)) {
		(void) sql_error(m, 10, SQLSTATE(3F000) "MAL external name %s.%s not bound (%s.%s)", f->mod, fimp, f->s->base.name, f->base.name);
		return -1;
	}
	if (old_side_effect != new_side_effect) {
		(void) sql_error(m, 10, SQLSTATE(42000) "Side-effect value from the SQL %s %s.%s doesn't match the MAL definition %s.%s\n"
						 "Either re-create the %s, or fix the MAL definition and restart the database", fn, f->s->base.name, f->base.name, f->mod, fimp, fn);
		return -1;
	}
	MT_lock_set(&sql_gencodeLock);
	if (!f->instantiated) {
		f->imp = fimp;
		f->instantiated = TRUE; /* make sure 'instantiated' gets set after 'imp' */
	} else {
		_DELETE(fimp);
	}
	MT_lock_unset(&sql_gencodeLock);
	return 0;
}

static int
backend_create_sql_func(backend *be, sql_func *f, list *restypes, list *ops)
{
	mvc *m = be->mvc;
	MalBlkPtr curBlk = NULL;
	InstrPtr curInstr = NULL;
	Client c = be->client;
	Symbol symbackup = NULL;
	int res = 0, i, nargs, retseen = 0, sideeffects = 0, no_inline = 0, added_to_cache = 0, needstoclean = 0;
	str msg = MAL_SUCCEED;
	backend bebackup;
	sql_func *pf = NULL;
	str fimp = NULL;
	char befname[IDLENGTH];
	const char *sql_shared_module;
	Module mod;
	sql_rel *r;

	/* already instantiated or instantiating a recursive function */
	if (f->instantiated || (m->forward && m->forward->base.id == f->base.id))
		return res;

	sql_shared_module = putName(sql_shared_module_name);
	mod = getModule(sql_shared_module);

	r = rel_parse(m, f->s, f->query, m_instantiate);
	if (r)
		r = sql_processrelation(m, r, 1, 1, 0);
	if (!r)
		return -1;

#ifndef NDEBUG
	/* for debug builds we keep the SQL function name in the MAL function name to make it easy to debug */
	if (strlen(f->base.name) + 21 >= IDLENGTH) { /* 20 bits for u64 number + '%' */
		(void) sql_error(m, 10, SQLSTATE(42000) "MAL function name '%s' too large for the backend", f->base.name);
		return -1;
	}
	(void) snprintf(befname, IDLENGTH, "%%" LLFMT "%s", store_function_counter(m->store), f->base.name);
#else
	(void) snprintf(befname, IDLENGTH, "f_" LLFMT, store_function_counter(m->store));
#endif
	symbackup = c->curprg;
	memcpy(&bebackup, be, sizeof(backend)); /* backup current backend */
	backend_reset(be);

	nargs = (f->res && f->type == F_UNION ? list_length(f->res) : 1) + (f->vararg && ops ? list_length(ops) : f->ops ? list_length(f->ops) : 0);
	c->curprg = newFunctionArgs(sql_shared_module, putName(befname), FUNCTIONsymbol, nargs);
	if (c->curprg == NULL) {
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		res = -1;
		goto cleanup;
	}
	if (!(fimp = _STRDUP(befname))) {
		sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		res = -1;
		goto cleanup;
	}

	curBlk = c->curprg->def;
	curInstr = getInstrPtr(curBlk, 0);

	if (f->res) {
		sql_arg *fres = f->res->h->data;
		if (f->type == F_UNION) {
			curInstr = table_func_create_result(curBlk, curInstr, f, restypes);
			if( curInstr == NULL) {
				sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				res = -1;
				goto cleanup;
			}
		} else {
			setArgType(curBlk, curInstr, 0, fres->type.type->localtype);
		}
	} else {
		setArgType(curBlk, curInstr, 0, TYPE_void);
	}

	if (f->vararg && ops) {
		int argc = 0;

		for (node *n = ops->h; n; n = n->next, argc++) {
			stmt *s = n->data;
			int type = tail_type(s)->type->localtype;
			int varid = 0;
			char buf[IDLENGTH];

			(void) snprintf(buf, IDLENGTH, "A%d", argc);
			if ((varid = newVariable(curBlk, buf, strlen(buf), type)) < 0) {
				sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: variable id too long");
				res = -1;
				goto cleanup;
			}
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
		}
	} else if (f->ops) {
		int argc = 0;

		for (node *n = f->ops->h; n; n = n->next, argc++) {
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
				sql_error(m, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				res = -1;
				goto cleanup;
			}
			if ((varid = newVariable(curBlk, buf, strlen(buf), type)) < 0) {
				sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: variable id too long");
				res = -1;
				goto cleanup;
			}
			curInstr = pushArgument(curBlk, curInstr, varid);
			setVarType(curBlk, varid, type);
		}
	}
	/* for recursive functions, avoid infinite loops */
	pf = m->forward;
	m->forward = f;
	be->fimp = fimp; /* for recursive functions keep the generated name */
	res = backend_dumpstmt(be, curBlk, r, 0, 1, NULL);
	m->forward = pf;
	if (res < 0)
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
	/* optimize the code, but beforehand add it to the cache, so recursive functions will be found */
	/* 'sql' module is shared, so adquire mal context lock to avoid race conditions while adding new function symbols */
	MT_lock_set(&sql_gencodeLock);
	if (!f->instantiated) {
		insertSymbol(mod, c->curprg);
		added_to_cache = 1;
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
				freeException(msg);
			else
				c->curprg->def->errors = msg;
		}
		if (c->curprg->def->errors) {
			MT_lock_unset(&sql_gencodeLock);
			sql_error(m, 10, SQLSTATE(42000) "Internal error while compiling statement: %s", c->curprg->def->errors);
			res = -1;
			goto cleanup;
		}
		f->imp = fimp;
		f->instantiated = TRUE; /* make sure 'instantiated' gets set after 'imp' */
	} else {
		needstoclean = 1;
	}
	MT_lock_unset(&sql_gencodeLock);

cleanup:
	if (res < 0 || needstoclean) {
		if (!added_to_cache) {
			freeSymbol(c->curprg);
		} else {
			MT_lock_set(&sql_gencodeLock);
			deleteSymbol(mod, c->curprg);
			MT_lock_unset(&sql_gencodeLock);
		}
		_DELETE(fimp);
	}
	memcpy(be, &bebackup, sizeof(backend));
	c->curprg = symbackup;
	return res;
}

static int
backend_create_func(backend *be, sql_func *f, list *restypes, list *ops)
{
	switch(f->lang) {
	case FUNC_LANG_INT:
	case FUNC_LANG_R:
	case FUNC_LANG_PY:
	case FUNC_LANG_PY3:
	case FUNC_LANG_MAP_PY:
	case FUNC_LANG_MAP_PY3:
	case FUNC_LANG_C:
	case FUNC_LANG_CPP:
		return 0; /* these languages don't require internal instantiation */
	case FUNC_LANG_MAL:
		return backend_create_mal_func(be->mvc, f);
	case FUNC_LANG_SQL:
		return backend_create_sql_func(be, f, restypes, ops);
	default:
		sql_error(be->mvc, 10, SQLSTATE(42000) "Function language without a MAL backend");
		return -1;
	}
}

int
backend_create_subfunc(backend *be, sql_subfunc *f, list *ops)
{
	return backend_create_func(be, f->func, f->res, ops);
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
