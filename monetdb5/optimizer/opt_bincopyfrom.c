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
static int extract_column(MalBlkPtr mb, InstrPtr old, int idx, int count_var);

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
	int onclient_parm = *(int*)getVarValue(mb, getArg(old, old->retc + 2));
	bool onserver = !onclient_parm;
	bool onclient = !onserver;

	// In the following loop, we (a) verify there is at least one non-nil column,
	// and (b) look for the column with the smallest type.
	int narrowest_idx = -1;
	int narrowest_type = TYPE_any;
	for (int i = 0; i < old->retc; i++) {
		int var = getArg(old, i);
		int var_type = ATOMstorage(getVarType(mb, var) & 0xFFFF);  // <=== what's the proper way to do this?
		if (var_type >= narrowest_type)
			continue;
		int path_idx = old->retc + 3 + i;
		int path_var = getArg(old, path_idx);
		if (VALisnil(&getVarConstant(mb, path_var)))
			continue;
		// this is the best so far
		narrowest_idx = i;
		narrowest_type = var_type;
	}
	if (narrowest_idx < 0)
		return createException(MAL, "optimizer.bincopyfrom", SQLSTATE(42000) "all paths are nil");

	int row_count_var;
	if (onserver) {
		// First import the narrowest column, which we assume will load quickest.
		// Pas its row count to the other imports.
		row_count_var = extract_column(mb, old, narrowest_idx, -1);
	} else {
		// with ON CLIENT, parallellism is harmful because there is only a single
		// connection to the client anyway. Every import will use the row count
		// of the previous import, which will serialize them.
		row_count_var = -1;
	}

	// Then emit the rest of the columns
	for (int i = 0; i < old->retc; i++) {
		if (i != narrowest_idx) {
			int new_row_count_var = extract_column(mb, old, i, row_count_var);
			if (onclient)
				row_count_var = new_row_count_var;
		}
	}

	return MAL_SUCCEED;
}

static int
extract_column(MalBlkPtr mb, InstrPtr old, int idx, int count_var)
{
	int var = getArg(old, idx);
	int var_type = getVarType(mb, var) & 0xFFFF;  // <=== what's the proper way to do this?
	str var_type_name = ATOMname(var_type);

	int onclient = *(int*)getVarValue(mb, getArg(old, old->retc + 2));

	int path_idx = old->retc + 3 + idx;
	int path_var = getArg(old, path_idx);
	str path = (str)getVarValue(mb, path_var);

	InstrPtr p = newFcnCall(mb, sqlRef, importColumnRef);
	setReturnArgument(p, old->argv[idx]);
	int row_count_var = newTmpVariable(mb, TYPE_lng);
	pushReturn(mb, p, row_count_var);
	pushStr(mb, p, var_type_name);
	pushStr(mb, p, path);
	pushInt(mb, p, onclient);
	if (count_var < 0)
		pushLng(mb, p, 0);
	else
		pushArgument(mb, p, count_var);

	return row_count_var;
}
