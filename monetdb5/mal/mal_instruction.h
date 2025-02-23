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

#ifndef _MAL_INSTR_H
#define _MAL_INSTR_H

#include "mal_type.h"
#include "mal_stack.h"
#include "mal_namespace.h"

#define isaSignature(P)  ((P)->token >=COMMANDsymbol || (P)->token == PATTERNsymbol)

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

#define DEBUG_MAL_INSTR
#define MAXARG 8				/* was 4 BEWARE the code depends on this knowledge, where? */
#define STMT_INCREMENT 4
#define MAL_VAR_WINDOW  16		// was 32
#define MAXLISTING (64*1024)

/* Allocation of space assumes a rather exotic number of
 * arguments. Access to module and function name are cast in macros to
 * prepare for separate name space management. */
#define getModuleId(P)		((P)->modname)
#ifdef NDEBUG
#define setModuleId(P,S)	do { (P)->modname = (S); } while (0)
#else
static inline void
setModuleId(InstrPtr p, const char *s)
{
	assert(s == getName(s));
	p->modname = s;
}
#endif
#define setModuleScope(P,S)	do {(P)->modname= (S)==NULL?NULL: (S)->name;} while (0)

#define getFunctionId(P)	((P)->fcnname)
#ifdef NDEBUG
#define setFunctionId(P,S)	do { (P)->fcnname = (S); } while (0)
#else
static inline void
setFunctionId(InstrPtr p, const char *s)
{
	assert(s == getName(s));
	p->fcnname = s;
}
#endif
#define garbageControl(P)	((P)->gc)

#define getInstrPtr(M,I)	((M)->stmt[I])
#define putInstrPtr(M,I,P)	(M)->stmt[I] = P
#define getSignature(S)		getInstrPtr((S)->def,0)
#define getFcnName(M)		getFunctionId(getInstrPtr(M,0))
#define getArgCount(M)		getInstrPtr(M,0)->argc
#define getModName(M)		getModuleId(getInstrPtr(M,0))
#define getPrgSize(M)		(M)->stop

#define getVar(M,I)			(&(M)->var[I])
#define getVarType(M,I)		((M)->var[I].type)
mal_export char *getVarNameIntoBuffer(MalBlkPtr mb, int idx, char *buf)
	__attribute__((__access__(write_only, 3)));

#define getVarKind(M,I)		((M)->var[I].kind)
#define getVarGDKType(M,I)	getGDKType((M)->var[I].type)
#define setVarType(M,I,V)	((M)->var[I].type = (V))
#define setVarKind(M,I,V)	((M)->var[I].kind = (V))	/* either _, X, or C */

#define clrVarFixed(M,I)	((M)->var[I].fixedtype = 0)
#define setVarFixed(M,I)	((M)->var[I].fixedtype = 1)
#define isVarFixed(M,I)		((M)->var[I].fixedtype)

#define clrVarCleanup(M,I)	((M)->var[I].cleanup = 0)
#define setVarCleanup(M,I)	((M)->var[I].cleanup = 1)
#define isVarCleanup(M,I)	((M)->var[I].cleanup)

#define isTmpVar(M,I)		(getVarKind(M,I) == REFMARKER)

#define clrVarUsed(M,I)		((M)->var[I].used = 0)
#define setVarUsed(M,I)		((M)->var[I].used = 1)
#define isVarUsed(M,I)		((M)->var[I].used)

#define clrVarDisabled(M,I)	((M)->var[I].disabled = 0)
#define setVarDisabled(M,I)	((M)->var[I].disabled = 1)
#define isVarDisabled(M,I)	((M)->var[I].disabled)

#define clrVarInit(M,I)		((M)->var[I].initialized = 0)
#define setVarInit(M,I)		((M)->var[I].initialized = 1)
#define isVarInit(M,I)		((M)->var[I].initialized)

#define clrVarTypedef(M,I)	((M)->var[I].typevar = 0)
#define setVarTypedef(M,I)	((M)->var[I].typevar = 1)
#define isVarTypedef(M,I)	((M)->var[I].typevar)

#define clrVarConstant(M,I)	((M)->var[I].constant = 0)
#define setVarConstant(M,I)	((M)->var[I].constant = 1)
#define isVarConstant(M,I)	((M)->var[I].constant)

#define setVarDeclared(M,I,X)	((M)->var[I].declared = (X))
#define getVarDeclared(M,I)	((M)->var[I].declared)

#define setVarUpdated(M,I,X)	((M)->var[I].updated = (X))
#define getVarUpdated(M,I)	((M)->var[I].updated)

#define setVarEolife(M,I,X)	((M)->var[I].eolife = (X))
#define getVarEolife(M,I)	((M)->var[I].eolife)

#define setVarScope(M,I,S)	((M)->var[I].depth = (S))
#define getVarScope(M,I)	((M)->var[I].depth)

#define clrVarCList(M,I)	((M)->var[I].kind = REFMARKER)
#define setVarCList(M,I)	((M)->var[I].kind = REFMARKERC)
#define isVarCList(M,I)		((M)->var[I].kind == REFMARKERC)

#define getVarConstant(M,I)	((M)->var[I].value)
#define getVarValue(M,I)	VALget(&(M)->var[I].value)

#define setRowCnt(M,I,C)	((M)->var[I].rowcnt = (C))
#define getRowCnt(M,I)		((M)->var[I].rowcnt)

#define getVarSTC(M,I)		((M)->var[I].stc)

#define getDestVar(P)		((P)->argv[0])
#define setDestVar(P,X)		((P)->argv[0] = (X))
#define setDestType(M,P,V)	setVarType((M),getDestVar(P),V)
#define getDestType(M,P)	destinationType(M,P)
#define getArg(P,I)			(P)->argv[I]
#define setArg(P,I,R)		((P)->argv[I] = (R))
#define getArgName(M,P,I)	(M)->var[(P)->argv[I]].name
#define getArgNameIntoBuffer(M,P,I,B)	getVarNameIntoBuffer((M),(P)->argv[I], B)
#define getArgType(M,P,I)	getVarType((M),(P)->argv[I])
#define getArgGDKType(M,P,I) getVarGDKType((M),(P)->argv[I])
#define getGDKType(T)		((T) <= TYPE_str ? (T) : ((T) == TYPE_any ? TYPE_void : findGDKtype(T)))

mal_export void addMalException(MalBlkPtr mb, str msg);
mal_export void mal_instruction_reset(void);
mal_export InstrPtr newInstruction(MalBlkPtr mb, const char *modnme,
								   const char *fcnnme);
mal_export InstrPtr newInstructionArgs(MalBlkPtr mb, const char *modnme,
									   const char *fcnnme, int args);
mal_export InstrPtr copyInstruction(const InstrRecord *p);
mal_export InstrPtr copyInstructionArgs(const InstrRecord *p, int args);
mal_export void clrInstruction(InstrPtr p);
mal_export void freeInstruction(InstrPtr p);
mal_export void clrFunction(InstrPtr p);
mal_export Symbol newSymbol(const char *nme, int kind);
mal_export void freeSymbol(Symbol s);
mal_export void freeSymbolList(Symbol s);
mal_export void printSignature(stream *fd, Symbol s, int flg);

mal_export MalBlkPtr newMalBlk(int elements);
mal_export void resetMalBlk(MalBlkPtr mb);
mal_export void resetMalTypes(MalBlkPtr mb, int stop);
mal_export int newMalBlkStmt(MalBlkPtr mb, int elements);
mal_export int resizeMalBlk(MalBlkPtr mb, int elements);
mal_export void freeMalBlk(MalBlkPtr mb);
mal_export MalBlkPtr copyMalBlk(MalBlkPtr mb);
mal_export void trimMalVariables(MalBlkPtr mb, MalStkPtr stk);
mal_export void trimMalVariables_(MalBlkPtr mb, MalStkPtr glb);
mal_export void moveInstruction(MalBlkPtr mb, int pc, int target);
mal_export void removeInstruction(MalBlkPtr mb, InstrPtr p);
mal_export void removeInstructionBlock(MalBlkPtr mb, int pc, int cnt);
mal_export str operatorName(int i);

mal_export int findVariable(MalBlkPtr mb, const char *name);
mal_export int findVariableLength(MalBlkPtr mb, const char *name, int len);
mal_export str getArgDefault(MalBlkPtr mb, InstrPtr p, int idx);
mal_export int newVariable(MalBlkPtr mb, const char *name, size_t len,
						   malType type);
mal_export int cloneVariable(MalBlkPtr dst, MalBlkPtr src, int varid);
mal_export void setVariableType(MalBlkPtr mb, const int idx, malType type);
mal_export int newTmpVariable(MalBlkPtr mb, malType type);
mal_export int newTypeVariable(MalBlkPtr mb, malType type);
mal_export void freeVariable(MalBlkPtr mb, int varid);
mal_export void clearVariable(MalBlkPtr mb, int varid);
mal_export int cpyConstant(MalBlkPtr mb, VarPtr vr);
mal_export int defConstant(MalBlkPtr mb, int type, ValPtr cst);
mal_export int fndConstant(MalBlkPtr mb, const ValRecord *cst, int depth);
mal_export str convertConstant(malType type, ValPtr vr);

mal_export void pushInstruction(MalBlkPtr mb, InstrPtr p);
mal_export InstrPtr pushArgument(MalBlkPtr mb, InstrPtr p, int varid);
mal_export InstrPtr setArgument(MalBlkPtr mb, InstrPtr p, int idx, int varid);
mal_export InstrPtr pushReturn(MalBlkPtr mb, InstrPtr p, int varid);
mal_export InstrPtr pushArgumentId(MalBlkPtr mb, InstrPtr p, const char *name);
mal_export void delArgument(InstrPtr p, int varid);
mal_export void setArgType(MalBlkPtr mb, InstrPtr p, int i, int tpe);
mal_export void setReturnArgument(InstrPtr p, int varid);
mal_export malType destinationType(MalBlkPtr mb, InstrPtr p);
mal_export void setPolymorphic(InstrPtr p, int tpe, int force);
/* Utility macros to inspect an instruction */
#define functionStart(X) ((X)->token == FUNCTIONsymbol || \
						  (X)->token == COMMANDsymbol)
#define patternStart(X)  ((X)->token == PATTERNsymbol)
#define functionExit(X)  ((X)->token == ENDsymbol)

#define blockStart(X)   ((X)->barrier && (((X)->barrier == BARRIERsymbol || \
										   (X)->barrier == CATCHsymbol)))
#define blockExit(X) ((X)->barrier == EXITsymbol)
#define blockReturn(X) ((X)->barrier == RETURNsymbol)
#define blockCntrl(X) ((X)->barrier== LEAVEsymbol ||	\
					   (X)->barrier== REDOsymbol ||		\
					   (X)->barrier== RETURNsymbol)
#define isLinearFlow(X)  (!(blockStart(X) || blockExit(X) || \
				(X)->barrier== LEAVEsymbol ||  (X)->barrier== REDOsymbol))

mal_export void strBeforeCall(ValPtr v, ValPtr bak);
mal_export void strAfterCall(ValPtr v, ValPtr bak);
mal_export void batBeforeCall(ValPtr v, ValPtr bak);
mal_export void batAfterCall(ValPtr v, ValPtr bak);

#endif /*  _MAL_INSTR_H */
