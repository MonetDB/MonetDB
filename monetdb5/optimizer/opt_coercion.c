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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
*/

#include "monetdb_config.h"
#include "opt_coercion.h"

static int
coercionOptimizerStep(MalBlkPtr mb, int i, InstrPtr p)
{
	int t, k, a, b;

	a = getArg(p, 0);
	b = getArg(p, 1);
	t = getVarType(mb, b);
	if (getVarType(mb, a) != t)
		return 0;
	if (strcmp(getFunctionId(p), ATOMname(t)) == 0) {
		removeInstruction(mb, p); /* dead code */
		for (; i < mb->stop; i++) {
			p = getInstrPtr(mb, i);
			for (k = p->retc; k < p->argc; k++)
				if (p->argv[k] == a)
					p->argv[k] = b;
		}
		return 1;
	}
	return 0;
}
int
OPTcoercionImplementation(Client cntxt,MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k;
	InstrPtr p;
	int actions = 0;
	str calcRef= putName("calc",4);

	(void) cntxt;
	(void) pci;
	(void) stk;		/* to fool compilers */

	for (i = 1; i < mb->stop; i++) {
		p = getInstrPtr(mb, i);
		if (getModuleId(p) == NULL)
			continue;
		if (getModuleId(p)==calcRef && p->argc == 2) {
			k= coercionOptimizerStep(mb, i, p);
			actions += k;
			if( k) i--;
		}
	}
	/*
	 * This optimizer affects the flow, but not the type and declaration
	 * structure. A cheaper optimizer is sufficient.
	 */
	return actions;
}
