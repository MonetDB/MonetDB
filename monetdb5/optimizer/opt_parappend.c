/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* author Joeri van Ruth
 * This optimizer replaces calls to sql.importTable with a series of calls to
 * sql.importColumn.
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "opt_parappend.h"
#include "wlc.h"

typedef struct parstate {
	InstrPtr prep_stmt;
	InstrPtr finish_stmt;
} parstate;

static str transform(parstate *state, MalBlkPtr mb, InstrPtr importTable, const char *execRef, const char *prepRef, int *actions);
static int setup_append_prep(parstate *state, MalBlkPtr mb, InstrPtr old, const char *prepRef);
static void flush_finish_stmt(parstate *state, MalBlkPtr mb);


str
OPTparappendImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	lng usec = GDKusec();
	int actions = 0;
	InstrPtr *old_mb_stmt = NULL;
	parstate state = { NULL };

	(void)stk;
	(void)pci;

	if (WLCused()) {
		// can of worms, bail out.
		return MAL_SUCCEED;
	}

	int found_at = -1;
	for (int i = 0; i < mb->stop; i++) {
		InstrPtr p = getInstrPtr(mb, i);
		if (p->modname == sqlRef) {
			if (p->fcnname == appendRef || p->fcnname == updateRef) {
				found_at = i;
				break;
			}
		}
	}
	if (found_at == -1)
		return MAL_SUCCEED;

	old_mb_stmt = mb->stmt;
	size_t old_ssize = mb->ssize;
	size_t old_stop = mb->stop;
	if (newMalBlkStmt(mb, mb->stop + getInstrPtr(mb, found_at)->argc) < 0) {
		msg = createException(MAL, "optimizer.parappend", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto end;
	}

	for (size_t i = 0; i < old_stop; i++) {
		InstrPtr p = old_mb_stmt[i];
		if (p->modname == sqlRef && p->fcnname == appendRef) {
			msg = transform(&state, mb, p, putName("append_exec"), putName("append_prep"), &actions);
		} else if (p->modname == sqlRef && p->fcnname == updateRef) {
			msg = transform(&state, mb, p, putName("update_exec"), putName("update_prep"), &actions);
		} else {
			flush_finish_stmt(&state, mb);
			pushInstruction(mb, p);
		}
		if (msg != MAL_SUCCEED)
			goto end;
	}
	assert(state.prep_stmt == NULL);

end:
	if (old_mb_stmt) {
		for (size_t i = old_stop; i < old_ssize; i++) {
			InstrPtr p = old_mb_stmt[i];
			if (p)
				freeInstruction(p);
		}
		GDKfree(old_mb_stmt);
	}

    /* Defense line against incorrect plans */
    if (actions > 0 && msg == MAL_SUCCEED) {
	    if (!msg)
        	msg = chkTypes(cntxt->usermodule, mb, FALSE);
	    if (!msg)
        	msg = chkFlow(mb);
	    if (!msg)
        	msg = chkDeclarations(mb);
    }
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
	char buf[256];
    snprintf(buf, sizeof(buf), "%-20s actions=%2d time=" LLFMT " usec", "parappend" ,actions, usec);
   	newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);



	return msg;
}

static str
transform(parstate *state, MalBlkPtr mb, InstrPtr old, const char *opRef, const char *prepRef, int *actions)
{
	int sname_var;
	int tname_var;
	int cname_var;
	int data_var;
	int cand_var;

	// take the old instruction apart
	assert(old->retc == 1);
	assert(old->argc == 6 || old->argc == 7);
	switch (old->argc) {
	case 6:
		sname_var = getArg(old, 2);
		tname_var = getArg(old, 3);
		cname_var = getArg(old, 4);
		data_var = getArg(old, 5);
		cand_var = 0;
		break;
	case 7:
		sname_var = getArg(old, 2);
		tname_var = getArg(old, 3);
		cname_var = getArg(old, 4);
		cand_var = getArg(old, 5);
		data_var = getArg(old, 6);
		break;
	default:
		throw(MAL, "optimizer.parappend", "internal error: append/update instr should have argc 6 or 7, not %d", old->argc);
	}

	bool sname_constant = isVarConstant(mb, sname_var);
	bool tname_constant = isVarConstant(mb, tname_var);
	bool cname_constant = isVarConstant(mb, cname_var);

	if (!sname_constant || !tname_constant || !cname_constant) {
		// cannot transform this
		flush_finish_stmt(state, mb);
		pushInstruction(mb, old);
		return MAL_SUCCEED;
	}

	*actions += 1;

	int cookie_var = setup_append_prep(state, mb, old, prepRef);

	int ret_cookie = newTmpVariable(mb, TYPE_ptr);
	InstrPtr e = newFcnCall(mb, sqlRef, opRef);
	setReturnArgument(e, ret_cookie);
	e = pushArgument(mb, e, cookie_var);
	if (cand_var)
		e = pushArgument(mb, e, cand_var);
	e = pushArgument(mb, e, data_var);

	state->finish_stmt = pushArgument(mb, state->finish_stmt, ret_cookie);

	freeInstruction(old);
	return MAL_SUCCEED;
}

static int
setup_append_prep(parstate *state, MalBlkPtr mb, InstrPtr old, const char *prepRef)
{
	// take the old instruction apart
	assert(old->retc == 1);
	assert(old->argc == 6 || old->argc == 7);
	int chain_out_var = getArg(old, 0);
	int chain_in_var = getArg(old, 1);
	int sname_var = getArg(old, 2);
	int tname_var = getArg(old, 3);
	int cname_var = getArg(old, 4);

	// check if the state refers to a sql.append_prep statement that can be
	// reused.
	InstrPtr prep_stmt = state->prep_stmt;
	do {
		if (prep_stmt == NULL)
			break;

		if (prep_stmt->fcnname != prepRef) {
			prep_stmt = NULL;
			break;
		}

		int existing_sname_var = getArg(prep_stmt, prep_stmt->retc + 1);
		int existing_tname_var = getArg(prep_stmt, prep_stmt->retc + 2);

		const char *existing_sname = getVarConstant(mb, existing_sname_var).val.sval;
		const char *incoming_sname = getVarConstant(mb, sname_var).val.sval;
		if (strcmp(existing_sname, incoming_sname) != 0) {
			prep_stmt = NULL;
			break;
		}

		const char *existing_tname = getVarConstant(mb, existing_tname_var).val.sval;
		const char *incoming_tname = getVarConstant(mb, tname_var).val.sval;
		if (strcmp(existing_tname, incoming_tname) != 0) {
			prep_stmt = NULL;
			break;
		}

		const char *incoming_cname = getVarConstant(mb, cname_var).val.sval;
		int existing_cols = prep_stmt->retc - 1;
		for (int i = prep_stmt->argc - existing_cols; i < prep_stmt->argc; i++) {
			int var = getArg(prep_stmt, i);
			const char *existing_cname = getVarConstant(mb, var).val.sval;
			if (strcmp(existing_cname, incoming_cname) == 0) {
				// We're not prepared for the complications that may arise
				// when there are multiple appends to the same column.
				// In particular we would have to track down where the previous
				// cookie was used and make sure we execute the next append
				// after that use.
				// This is unlikely to occur in practice, so instead we just start over.
				prep_stmt = NULL;
				break;
			}
		}

		// It seems there is no objection to reusing the existing sql.append_prep.
	} while (0);

	int cookie_var = newTmpVariable(mb, TYPE_ptr);
	if (prep_stmt == NULL) {
		flush_finish_stmt(state, mb);

		int chain = newTmpVariable(mb, TYPE_int);
		InstrPtr p = newFcnCall(mb, sqlRef, prepRef);
		setReturnArgument(p, chain);
		pushReturn(mb, p, cookie_var);
		p = pushArgument(mb, p, chain_in_var);
		p = pushArgument(mb, p, sname_var);
		p = pushArgument(mb, p, tname_var);
		p = pushArgument(mb, p, cname_var);
		state->prep_stmt = p;

		InstrPtr f = newInstructionArgs(mb, sqlRef, append_finishRef, 2);
		setReturnArgument(f, chain_out_var);
		f = pushArgument(mb, f, chain);
		state->finish_stmt = f;
	} else {
		// Append to existing first, to ensure there is room
		prep_stmt = pushArgument(mb, prep_stmt, cname_var);
		prep_stmt = pushArgument(mb, prep_stmt, cookie_var);
		// Now move the cookie_var to its proper location
		for (int i = prep_stmt->argc - 1; i > prep_stmt->retc; i--)
			setArg(prep_stmt, i, getArg(prep_stmt, i - 1));
		setArg(prep_stmt, prep_stmt->retc, cookie_var);
		prep_stmt->retc += 1;

		// Always use the chain_out of the latest sql_append:
		setArg(state->finish_stmt, 0, chain_out_var);

		state->prep_stmt = prep_stmt;
	}

	return cookie_var;
}


static void
flush_finish_stmt(parstate *state, MalBlkPtr mb)
{
	if (state->finish_stmt) {
		pushInstruction(mb, state->finish_stmt);
	}
	state->prep_stmt = NULL;
	state->finish_stmt = NULL;
}
