/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "opt_generator.h"
#include "mal_builder.h"

/*
 * (c) Martin Kersten, Sjoerd Mullender
 * Series generating module for integer, decimal, real, double and timestamps.
 */

#define errorCheck(P,IDX,MOD,I)										\
	do {															\
		setModuleId(P, generatorRef);								\
		typeChecker(cntxt->usermodule, mb, P, IDX, TRUE);			\
		if(P->typechk == TYPE_UNKNOWN){								\
			setModuleId(P, MOD);									\
			typeChecker(cntxt->usermodule, mb, P, IDX, TRUE);		\
			setModuleId(series[I], generatorRef);					\
			setFunctionId(series[I], seriesRef);					\
			typeChecker(cntxt->usermodule, mb, series[I], I, TRUE);	\
		}															\
		pushInstruction(mb,P);										\
	} while (0)

#define casting(TPE)													\
	do {																\
		k = getArg(p, 1);												\
		p->argc = p->retc;												\
		q = newInstruction(0, calcRef, TPE##Ref);						\
		if (q == NULL) {												\
			msg = createException(MAL, "optimizer.generator", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			goto bailout;												\
		}																\
		if (setDestVar(q, newTmpVariable(mb, TYPE_##TPE)) < 0) {		\
			freeInstruction(q);											\
			msg = createException(MAL, "optimizer.generator", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			goto bailout;												\
		}																\
		q = pushArgument(mb, q, getArg(series[k], 1));					\
		typeChecker(cntxt->usermodule, mb, q, 0, TRUE);					\
		p = pushArgument(mb, p, getArg(q, 0));							\
		pushInstruction(mb, q);											\
		q = newInstruction(0, calcRef, TPE##Ref);						\
		if (q == NULL) {												\
			msg = createException(MAL, "optimizer.generator", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			goto bailout;												\
		}																\
		if (setDestVar(q, newTmpVariable(mb, TYPE_##TPE)) < 0) {		\
			freeInstruction(q);											\
			msg = createException(MAL, "optimizer.generator", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			goto bailout;												\
		}																\
		q = pushArgument(mb, q, getArg(series[k], 2));					\
		pushInstruction(mb, q);											\
		typeChecker(cntxt->usermodule,  mb,  q,  0, TRUE);				\
		p = pushArgument(mb, p, getArg(q, 0));							\
		if( p->argc == 4){												\
			q = newInstruction(0, calcRef, TPE##Ref);					\
			if (q == NULL) {											\
				msg = createException(MAL, "optimizer.generator", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
				goto bailout;											\
			}															\
			if (setDestVar(q, newTmpVariable(mb, TYPE_##TPE)) < 0) {	\
				freeInstruction(q);										\
				msg = createException(MAL, "optimizer.generator", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
				goto bailout;											\
			}															\
			q = pushArgument(mb, q, getArg(series[k], 3));				\
			typeChecker(cntxt->usermodule, mb, q, 0, TRUE);				\
			p = pushArgument(mb, p, getArg(q, 0));						\
			pushInstruction(mb, q);										\
		}																\
		setModuleId(p, generatorRef);									\
		setFunctionId(p, parametersRef);								\
		series[getArg(p, 0)] = p;										\
		pushInstruction(mb, p);											\
		old[i] = NULL;													\
	} while (0)

str
OPTgeneratorImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
						   InstrPtr pci)
{
	InstrPtr p, q, *old, *series;
	int i, k, limit, slimit, actions = 0;
	const char *bteRef = getName("bte");
	const char *shtRef = getName("sht");
	const char *intRef = getName("int");
	const char *lngRef = getName("lng");
	const char *fltRef = getName("flt");
	const char *dblRef = getName("dbl");
	str msg = MAL_SUCCEED;
	int needed = 0;

	(void) stk;

	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;

	// check applicability first
	for (i = 0; i < limit; i++) {
		p = old[i];
		if (getModuleId(p) == generatorRef && getFunctionId(p) == seriesRef)
			needed = 1;
		/* avoid error in table-udf-column-descriptor */
		if (p->token == RETURNsymbol || p->barrier == RETURNsymbol) {
			old = NULL;
			goto wrapup;
		}
	}
	if (!needed)
		goto wrapup;

	series = (InstrPtr *) GDKzalloc(sizeof(InstrPtr) * mb->vtop);
	if (series == NULL)
		throw(MAL, "optimizer.generator", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (newMalBlkStmt(mb, mb->ssize) < 0) {
		GDKfree(series);
		throw(MAL, "optimizer.generator", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i = 0; mb->errors == NULL && i < limit; i++) {
		p = old[i];
		if (p->token == ENDsymbol) {
			break;
		}
		if (getModuleId(p) == generatorRef && getFunctionId(p) == seriesRef) {
			series[getArg(p, 0)] = p;
			setModuleId(p, generatorRef);
			setFunctionId(p, parametersRef);
			typeChecker(cntxt->usermodule, mb, p, i, TRUE);
			pushInstruction(mb, p);
			old[i] = NULL;
		} else if (getModuleId(p) == algebraRef && getFunctionId(p) == selectRef
				   && series[getArg(p, 1)]) {
			errorCheck(p, i, algebraRef, getArg(p, 1));
		} else if (getModuleId(p) == algebraRef
				   && getFunctionId(p) == thetaselectRef
				   && series[getArg(p, 1)]) {
			errorCheck(p, i, algebraRef, getArg(p, 1));
		} else if (getModuleId(p) == algebraRef
				   && getFunctionId(p) == projectionRef
				   && series[getArg(p, 2)]) {
			errorCheck(p, i, algebraRef, getArg(p, 2));
		} else if (getModuleId(p) == sqlRef
				   && getFunctionId(p) == putName("exportValue")
				   && isaBatType(getArgType(mb, p, 0))) {
			// interface expects scalar type only, not expressable in MAL signature
			mb->errors = createException(MAL, "generate_series",
										 SQLSTATE(42000)
										 "internal error, generate_series is a table producing function");
		} else if (getModuleId(p) == batcalcRef && getFunctionId(p) == bteRef
				   && series[getArg(p, 1)] && p->argc == 2) {
			casting(bte);
		} else if (getModuleId(p) == batcalcRef && getFunctionId(p) == shtRef
				   && series[getArg(p, 1)] && p->argc == 2) {
			casting(sht);
		} else if (getModuleId(p) == batcalcRef && getFunctionId(p) == intRef
				   && series[getArg(p, 1)] && p->argc == 2) {
			casting(int);
		} else if (getModuleId(p) == batcalcRef && getFunctionId(p) == lngRef
				   && series[getArg(p, 1)] && p->argc == 2) {
			casting(lng);
		} else if (getModuleId(p) == batcalcRef && getFunctionId(p) == fltRef
				   && series[getArg(p, 1)] && p->argc == 2) {
			casting(flt);
		} else if (getModuleId(p) == batcalcRef && getFunctionId(p) == dblRef
				   && series[getArg(p, 1)] && p->argc == 2) {
			casting(dbl);
		} else if (getModuleId(p) == languageRef && getFunctionId(p) == passRef) {
			pushInstruction(mb, p);
			old[i] = NULL;
		} else {
			// check for use without conversion
			for (k = p->retc; k < p->argc; k++) {
				if (series[getArg(p, k)]) {
					const char *m = getModuleId(p);
					setModuleId(p, generatorRef);
					typeChecker(cntxt->usermodule, mb, p, i, TRUE);
					if (p->typechk == TYPE_UNKNOWN) {
						setModuleId(p, m);
						typeChecker(cntxt->usermodule, mb, p, i, TRUE);
						InstrPtr r = series[getArg(p, k)];
						setModuleId(r, generatorRef);
						setFunctionId(r, seriesRef);
						typeChecker(cntxt->usermodule, mb, r, getPC(mb, r),
									TRUE);
					}
				}
			}
			pushInstruction(mb, p);
			old[i] = NULL;
		}
	}
	for (; i < limit; i++)
		pushInstruction(mb, old[i]);
  bailout:
	for (; i < slimit; i++) {
		if (old[i])
			pushInstruction(mb, old[i]);
	}
	GDKfree(old);
	GDKfree(series);

	/* Defense line against incorrect plans */
	/* all new/modified statements are already checked */
	// msg = chkTypes(cntxt->usermodule, mb, FALSE);
	// if (!msg)
	//      msg = chkFlow(mb);
	// if (!msg)
	//      msg = chkDeclarations(mb);
	/* keep all actions taken as a post block comment */
  wrapup:
	/* keep actions taken as a fake argument */
	(void) pushInt(mb, pci, actions);
	return msg;
}
