/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "opt_aliases.h"

/* an alias is recognized by a simple assignment */
#define OPTisAlias(X) (X->argc == 2 && X->token == ASSIGNsymbol && X->barrier == 0 )

str
OPTaliasesImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k = 1, limit, actions = 0;
	int *alias = 0;
	str msg = MAL_SUCCEED;
	InstrPtr p;

	(void) stk;
	(void) cntxt;

	limit = mb->stop;
	for (i = 1; i < limit; i++) {
		p = getInstrPtr(mb, i);
		if (OPTisAlias(p))
			break;
	}
	if (i == limit) {
		// we didn't found a simple assignment that warrants a rewrite
		goto wrapup;
	}
	k = i;
	if (i < limit) {
		alias = GDKzalloc(sizeof(int) * mb->vtop);
		if (alias == NULL)
			throw(MAL, "optimizer.aliases", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		setVariableScope(mb);
		for (j = 1; j < mb->vtop; j++)
			alias[j] = j;
	}
	for (; i < limit; i++) {
		p = getInstrPtr(mb, i);
		mb->stmt[k++] = p;
		if (OPTisAlias(p) && getLastUpdate(mb, getArg(p, 0)) == i
			&& getBeginScope(mb, getArg(p, 0)) == i
			&& getLastUpdate(mb, getArg(p, 1)) <= i) {
			alias[getArg(p, 0)] = alias[getArg(p, 1)];
			freeInstruction(p);
			actions++;
			k--;
			mb->stmt[k] = 0;
		} else {
			for (int i = 0; i < p->argc; i++)
				getArg(p, i) = alias[getArg(p, i)];
		}
	}

	for (i = k; i < limit; i++)
		mb->stmt[i] = NULL;

	mb->stop = k;
	GDKfree(alias);

	/* Defense line against incorrect plans */
	/* Plan is unaffected */
	// msg = chkTypes(cntxt->usermodule, mb, FALSE);
	// if ( msg == MAL_SUCCEED)
	//      msg = chkFlow(mb);
	// if ( msg == MAL_SUCCEED)
	//      msg = chkDeclarations(mb);
  wrapup:
	/* keep actions taken as a fake argument */
	(void) pushInt(mb, pci, actions);
	return msg;
}
