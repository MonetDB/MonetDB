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
static int extract_column(MalBlkPtr mb, InstrPtr old, int idx, int proto_bat_var, int count_var);

str
OPTbincopyfromImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	InstrPtr *old_mb_stmt = NULL;
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;

	str importTableRef = putName("importTable");

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
	int old_stop = mb->stop;
	if (newMalBlkStmt(mb, mb->stop + getInstrPtr(mb, found_at)->argc) < 0) {
		msg = createException(MAL, "optimizer.bincopyfrom", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto end;
	}

	for (int i = 0; i < old_stop; i++) {
		InstrPtr p = old_mb_stmt[i];
		if (p->modname != sqlRef || p->fcnname != importTableRef) {
			pushInstruction(mb, p);
			continue;
		}
		msg = transform(mb, p);
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
	// prototype: (bat1, .., batN) := sql.importTable(schema, table, onclient, path1 , .. ,pathN);
	int onclient_arg = *(int*)getVarValue(mb, getArg(old, old->retc + 2));
	bool onserver = !onclient_arg;
	bool onclient = !onserver;

	// In the following loop we pick a "prototype column".
	// This is always a column with a non-nil path and will be the first column for
	// which we emit code. We prefer a prototype column that is quick to import
	// because ON SERVER, all other columns can be loaded in parallel once we've
	// loaded the first one.
	//
	// Both ON SERVER and ON CLIENT, the prototype column is also used when emitting the
	// columns with a nil path.
	int prototype_idx = -1;
	int prototype_type = TYPE_any;
	for (int i = 0; i < old->retc; i++) {
		int var = getArg(old, i);
		int var_type = getVarType(mb, var);
		int tail_type = ATOMstorage(getBatType(var_type));
		if (tail_type >= prototype_type)
			continue;
		int path_idx = old->retc + 3 + i;
		int path_var = getArg(old, path_idx);
		if (VALisnil(&getVarConstant(mb, path_var)))
			continue;
		// this is the best so far
		prototype_idx = i;
		prototype_type = tail_type;
	}
	if (prototype_idx < 0)
		return createException(MAL, "optimizer.bincopyfrom", SQLSTATE(42000) "all paths are nil");

	// Always emit the prototype column first
	int prototype_count_var = extract_column(mb, old, prototype_idx, -1, -1);
	assert(mb->stop > 0);
	int prototype_bat_var = getArg(getInstrPtr(mb, mb->stop - 1), 0);
	assert(prototype_count_var == getArg(getInstrPtr(mb, mb->stop - 1), 1));

	// Then emit the rest of the columns

	int row_count_var = prototype_count_var;
	for (int i = 0; i < old->retc; i++) {
		if (i == prototype_idx)
			continue;
		int new_row_count_var = extract_column(mb, old, i, prototype_bat_var, row_count_var);
		if (onclient)
			row_count_var = new_row_count_var; // chain the importColumn statements
	}

	return MAL_SUCCEED;
}

static int
extract_column(MalBlkPtr mb, InstrPtr old, int idx, int proto_bat_var, int count_var)
{
	int var = getArg(old, idx);
	int var_type = getVarType(mb, var);
	str var_type_name = ATOMname(getBatType(var_type));

	int onclient = *(int*)getVarValue(mb, getArg(old, old->retc + 2));

	int path_idx = old->retc + 3 + idx;
	int path_var = getArg(old, path_idx);
	str path = (str)getVarValue(mb, path_var);

	if (!strNil(path)) {
		InstrPtr p = newFcnCall(mb, sqlRef, importColumnRef);
		setReturnArgument(p, old->argv[idx]);
		int new_count_var = newTmpVariable(mb, TYPE_lng);
		pushReturn(mb, p, new_count_var);
		pushStr(mb, p, var_type_name);
		pushStr(mb, p, path);
		pushInt(mb, p, onclient);
		if (count_var < 0)
			pushLng(mb, p, 0);
		else
			pushArgument(mb, p, count_var);
		return new_count_var;
	} else {
		InstrPtr p = newFcnCall(mb, algebraRef, projectRef);
		setReturnArgument(p, old->argv[idx]);
		pushArgument(mb, p, proto_bat_var);
		int proto_bat_type = getVarType(mb, var);
		int proto_elem_type = getBatType(proto_bat_type);
		pushNil(mb, p, proto_elem_type);
		return count_var;
	}
}
