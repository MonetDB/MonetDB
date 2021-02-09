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
#include "opt_bincopyfrom.h"

static str transform(MalBlkPtr mb, InstrPtr importTable);
static int extract_column(MalBlkPtr mb, InstrPtr old, int idx, str proto_path, int proto_bat_var, int count_var, bool byteswap);

str
OPTbincopyfromImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	InstrPtr *old_mb_stmt = NULL;
	lng usec = GDKusec();
	int actions = 0;

	(void)stk;
	(void)pci;

	const char *importTableRef = putName("importTable");

	int found_at = -1;
	for (int i = 0; i < mb->stop; i++) {
		InstrPtr p = getInstrPtr(mb, i);
		if (p->modname == sqlRef && p->fcnname == importTableRef) {
			found_at = i;
			break;
		}
	}
	if (found_at == -1)
		return MAL_SUCCEED;

	old_mb_stmt = mb->stmt;
	size_t old_ssize = mb->ssize;
	size_t old_stop = mb->stop;
	if (newMalBlkStmt(mb, mb->stop + getInstrPtr(mb, found_at)->argc) < 0) {
		msg = createException(MAL, "optimizer.bincopyfrom", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto end;
	}

	for (size_t i = 0; i < old_stop; i++) {
		InstrPtr p = old_mb_stmt[i];
		if (p->modname == sqlRef && p->fcnname == importTableRef) {
			msg = transform(mb, p);
			actions++;
		} else {
			pushInstruction(mb, p);
		}
		if (msg != MAL_SUCCEED)
			goto end;
	}

end:
	if (old_mb_stmt) {
		for (size_t i = old_stop; i < old_ssize; i++)
			if (old_mb_stmt[i])
				freeInstruction(old_mb_stmt[i]);
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
    snprintf(buf, sizeof(buf), "%-20s actions=%2d time=" LLFMT " usec","bincopyfrom",actions, usec);
   	newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);

	return msg;
}

static str
transform(MalBlkPtr mb, InstrPtr old)
{
	// prototype: (bat1, .., batN) := sql.importTable(schema, table, onclient, path1 , .. ,pathN);
	int onclient_arg = *(int*)getVarValue(mb, getArg(old, old->retc + 2));
	bool onserver = !onclient_arg;
	bool onclient = !onserver;
	bool byteswap = *(bit*)getVarValue(mb, getArg(old, old->retc + 3));

	// In the following loop we pick a "prototype column".
	// This is always a column with a non-nil path and will be the first column for
	// which we emit code. We prefer a prototype column that is quick to import
	// because ON SERVER, all other columns can be loaded in parallel once we've
	// loaded the first one.
	//
	// Both ON SERVER and ON CLIENT, the prototype column is also used when emitting the
	// columns with a nil path, by projecting nil values over it.
	int prototype_idx = -1;
	int prototype_type = TYPE_any;
	str prototype_path = NULL;
	for (int i = 0; i < old->retc; i++) {
		int var = getArg(old, i);
		int var_type = getVarType(mb, var);
		int tail_type = ATOMstorage(getBatType(var_type));
		if (tail_type >= prototype_type)
			continue;
		int path_idx = old->retc + 4 + i;
		int path_var = getArg(old, path_idx);
		if (VALisnil(&getVarConstant(mb, path_var)))
			continue;
		// this is the best so far
		prototype_idx = i;
		prototype_type = tail_type;
		prototype_path = (str)getVarValue(mb, path_var);
	}
	if (prototype_idx < 0)
		return createException(MAL, "optimizer.bincopyfrom", SQLSTATE(42000) "all paths are nil");

	// Always emit the prototype column first
	int prototype_count_var = extract_column(mb, old, prototype_idx, NULL, -1, -1, byteswap);
	assert(mb->stop > 0);
	int prototype_bat_var = getArg(getInstrPtr(mb, mb->stop - 1), 0);
	assert(prototype_count_var == getArg(getInstrPtr(mb, mb->stop - 1), 1));

	// Then emit the rest of the columns

	int row_count_var = prototype_count_var;
	for (int i = 0; i < old->retc; i++) {
		if (i == prototype_idx)
			continue;
		int new_row_count_var = extract_column(mb, old, i, prototype_path, prototype_bat_var, row_count_var, byteswap);
		if (onclient)
			row_count_var = new_row_count_var; // chain the importColumn statements
	}

	freeInstruction(old);

	return MAL_SUCCEED;
}

static int
extract_column(MalBlkPtr mb, InstrPtr old, int idx, str proto_path, int proto_bat_var, int count_var, bool byteswap)
{
	int var = getArg(old, idx);
	int var_type = getVarType(mb, var);

	// The sql.importColumn operator takes a 'method' string to determine how to
	// load the data. This leaves the door open to have multiple loaders for the
	// same backend type, for example nul- and newline terminated strings.
	// For the time being we just use the name of the storage type as the method
	// name.
	str method = ATOMname(getBatType(var_type));

	int onclient = *(int*)getVarValue(mb, getArg(old, old->retc + 2));

	int path_idx = old->retc + 4 + idx;
	int path_var = getArg(old, path_idx);
	str path = (str)getVarValue(mb, path_var);

	if (!strNil(path)) {
		if (proto_path != NULL && strcmp(proto_path, path) == 0) {
			// Same data as in the prototype column so reuse that var
			InstrPtr p = newInstructionArgs(mb, NULL, NULL, 2);
			p = pushArgument(mb, p, proto_bat_var);
			setReturnArgument(p, old->argv[idx]);
			pushInstruction(mb, p);
			return count_var;
		} else {
			// Emit a new importColumn call
			InstrPtr p = newFcnCall(mb, sqlRef, importColumnRef);
			setReturnArgument(p, old->argv[idx]);
			int new_count_var = newTmpVariable(mb, TYPE_oid);
			pushReturn(mb, p, new_count_var);
			pushStr(mb, p, method);
			pushBit(mb, p, byteswap);
			pushStr(mb, p, path);
			pushInt(mb, p, onclient);
			if (count_var < 0)
				pushOid(mb, p, 0);
			else
				p = pushArgument(mb, p, count_var);
			return new_count_var;
		}
	} else {
		// create an empty column by projecting the prototype
		InstrPtr p = newFcnCall(mb, algebraRef, projectRef);
		setReturnArgument(p, old->argv[idx]);
		p = pushArgument(mb, p, proto_bat_var);
		int proto_bat_type = getVarType(mb, var);
		int proto_elem_type = getBatType(proto_bat_type);
		p = pushNil(mb, p, proto_elem_type);
		return count_var;
	}
}
