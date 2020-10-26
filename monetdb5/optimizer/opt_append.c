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
#include "opt_append.h"

static str transform(MalBlkPtr mb, InstrPtr importTable);

str
OPTparappendImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	InstrPtr *old_mb_stmt = NULL;
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;

	int found_at = -1;
	for (int i = 0; i < mb->stop; i++) {
		InstrPtr p = getInstrPtr(mb, i);
		if (p->modname == sqlRef && p->fcnname == appendRef) {
			found_at = i;
			break;
		}
	}
	if (found_at == -1)
		return MAL_SUCCEED;

	old_mb_stmt = mb->stmt;
	int old_stop = mb->stop;
	if (newMalBlkStmt(mb, mb->stop + getInstrPtr(mb, found_at)->argc) < 0) {
		msg = createException(MAL, "optimizer.parappend", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto end;
	}

	for (int i = 0; i < old_stop; i++) {
		InstrPtr p = old_mb_stmt[i];
		if (p->modname == sqlRef && p->fcnname == appendRef && isaBatType(getArgType(mb, p, 5))) {
				msg = transform(mb, p);
			} else {
			pushInstruction(mb, p);
		}
		if (msg != MAL_SUCCEED)
			return msg;
	}

end:
	if (old_mb_stmt)
		GDKfree(old_mb_stmt);
	return msg;
}

static str
transform(MalBlkPtr mb, InstrPtr old)
{

	// take the old instruction apart
	assert(old->retc == 1);
	assert(old->argc == 1 + 5);
	int chain_out_var = getArg(old, 0);
	int chain_in_var = getArg(old, 1);
	int sname_var = getArg(old, 2);
	int tname_var = getArg(old, 3);
	int cname_var = getArg(old, 4);
	int data_var = getArg(old, 5);

	bool sname_constant = isVarConstant(mb, sname_var);
	bool tname_constant = isVarConstant(mb, tname_var);
	bool cname_constant = isVarConstant(mb, cname_var);

	if (!sname_constant || !tname_constant || !cname_constant) {
		// cannot transform this
		pushInstruction(mb, old);
		return MAL_SUCCEED;
	}

	int cookie_var = newTmpVariable(mb, TYPE_ptr);

	str append_prepRef = putName("append_prep");
	InstrPtr p = newFcnCall(mb, sqlRef, append_prepRef);
	setReturnArgument(p, chain_out_var);
	pushReturn(mb, p, cookie_var);
	pushArgument(mb, p, chain_in_var);
	pushArgument(mb, p, sname_var);
	pushArgument(mb, p, tname_var);
	pushArgument(mb, p, cname_var);

	str append_execRef = putName("append_exec");
	InstrPtr e = newFcnCall(mb, sqlRef, append_execRef);
	pushArgument(mb, e, cookie_var);
	pushArgument(mb, e, data_var);

	// fprintf(stderr, "TRIGGER\n");

	return MAL_SUCCEED;
}
