/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _MAL_INSTR_H
#define _MAL_INSTR_H

#include "mal_type.h"
#include "mal_stack.h"
#include "mal_namespace.h"

#define isaSignature(P)  ((P)->token >=COMMANDsymbol)

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

#define DEBUG_MAL_INSTR
/* #define DEBUG_REDUCE */
#define MAXARG 4				/* BEWARE the code depends on this knowledge */
#define STMT_INCREMENT 32
#define MAL_VAR_WINDOW  32
#define MAXVARS 32
#define MAXLISTING 64*1024
#define SMALLBUFSIZ 64

/* Allocation of space assumes a rather exotic number of
 * arguments. Access to module and function name are cast in macros to
 * prepare for separate name space management. */
#define getModuleId(P)		(P)->modname
#define setModuleId(P,S)	(P)->modname= S
#define setModuleScope(P,S)	do {(P)->modname= (S)==NULL?NULL: (S)->name;} while (0)

#define getFunctionId(P)	(P)->fcnname
#define setFunctionId(P,S)	(P)->fcnname= S
#define garbageControl(P)	((P)->gc & GARBAGECONTROL)

#define getInstrPtr(M,I)	(M)->stmt[I]
#define getSignature(S)		getInstrPtr((S)->def,0)
#define isMain(M)			((getInstrPtr(M,0))->fcnname== putName("main",4))
#define getFcnName(M)		getFunctionId(getInstrPtr(M,0))
#define getArgCount(M)		getInstrPtr(M,0)->argc
#define getModName(M)		getModuleId(getInstrPtr(M,0))
#define getPrgSize(M)		(M)->stop

#define getVar(M,I)			(M)->var[I]
#define getVarTmp(M,I)		(M)->var[I]->tmpindex
#define isTmpVar(M,I)		((M)->var[I]->tmpindex)
#define getVarType(M,I)		((M)->var[I]->type)
#define getVarGDKType(M,I)	getGDKType((M)->var[I]->type)
#define getGDKType(T) 		( T <= TYPE_str ? T : (T == TYPE_any ? TYPE_void : findGDKtype(T)))

#define clrVarFixed(M,I)		((M)->var[I]->flags &= ~VAR_FIXTYPE)
#define setVarFixed(M,I)		((M)->var[I]->flags |= VAR_FIXTYPE)
#define isVarFixed(M,I)		((M)->var[I]->flags & VAR_FIXTYPE)

#define clrVarCleanup(M,I)		((M)->var[I]->flags &= ~VAR_CLEANUP)
#define setVarCleanup(M,I)		((M)->var[I]->flags |= VAR_CLEANUP)
#define isVarCleanup(M,I)		((M)->var[I]->flags & VAR_CLEANUP)

#define clrVarUsed(M,I)		((M)->var[I]->flags &= ~VAR_USED)
#define setVarUsed(M,I)		((M)->var[I]->flags |= VAR_USED)
#define isVarUsed(M,I)		((M)->var[I]->flags & VAR_USED)

#define clrVarDisabled(M,I)		((M)->var[I]->flags &= ~VAR_DISABLED)
#define setVarDisabled(M,I)		((M)->var[I]->flags |= VAR_DISABLED)
#define isVarDisabled(M,I)		((M)->var[I]->flags & VAR_DISABLED)

#define clrVarInit(M,I)		((M)->var[I]->flags &= ~VAR_INIT)
#define setVarInit(M,I)		((M)->var[I]->flags |= VAR_INIT)
#define isVarInit(M,I)		((M)->var[I]->flags & VAR_INIT)

#define clrVarTypedef(M,I)		((M)->var[I]->flags &= ~VAR_TYPEVAR)
#define setVarTypedef(M,I)		((M)->var[I]->flags |= VAR_TYPEVAR)
#define isVarTypedef(M,I)		((M)->var[I]->flags & VAR_TYPEVAR)

#define clrVarUDFtype(M,I)		((M)->var[I]->flags &= ~VAR_UDFTYPE)
#define setVarUDFtype(M,I)		((M)->var[I]->flags |= VAR_UDFTYPE)
#define isVarUDFtype(M,I)		((M)->var[I]->flags & VAR_UDFTYPE)

#define clrVarConstant(M,I)		((M)->var[I]->flags &= ~VAR_CONSTANT)
#define setVarConstant(M,I)		((M)->var[I]->flags |= VAR_CONSTANT)
#define isVarConstant(M,I)		((M)->var[I]->flags & VAR_CONSTANT)

#define clrVarCList(M,I)		((M)->var[I]->flags &= ~VAR_CLIST)
#define setVarCList(M,I)		((M)->var[I]->flags |= VAR_CLIST)
#define isVarCList(M,I)		((M)->var[I]->flags & VAR_CLIST)

#define getVarConstant(M,I)	((M)->var[I]->value)
#define getVarValue(M,I)	VALget(&(M)->var[I]->value)

#define setRowCnt(M,I,C)	(M)->var[I]->rowcnt = C
#define getRowCnt(M,I)		((M)->var[I]->rowcnt)

#define setMitosisPartition(P,C)	(P)->mitosis = C
#define getMitosisPartition(P)		((P)->mitosis)

#define getSTC(M,I)		((M)->var[I]->stc)
#define setSTC(M,I,X)	(M)->var[I]->stc =X

#define getDestVar(P)		(P)->argv[0]
#define setDestVar(P,X)		(P)->argv[0]  =X
#define setDestType(M,P,V)	setVarType((M),getDestVar(P),V)
#define getDestType(M,P)	destinationType((M),(P))
#define getArg(P,I)			(P)->argv[I]
#define setArg(P,I,R)		(P)->argv[I]= R
#define getArgName(M,P,I)	getVarName((M),(P)->argv[I])
#define getArgType(M,P,I)	getVarType((M),(P)->argv[I])
#define getArgGDKType(M,P,I) getVarGDKType((M),(P)->argv[I])

#define getEndOfLife(X,Y)	(X)->var[Y]->eolife

mal_export InstrPtr newInstruction(MalBlkPtr mb, int kind);
mal_export InstrPtr copyInstruction(InstrPtr p);
mal_export void oldmoveInstruction(InstrPtr dst, InstrPtr src);
mal_export void clrInstruction(InstrPtr p);
mal_export void freeInstruction(InstrPtr p);
mal_export void clrFunction(InstrPtr p);
mal_export Symbol newSymbol(str nme, int kind);
mal_export void freeSymbol(Symbol s);
mal_export void freeSymbolList(Symbol s);
mal_export void printSignature(stream *fd, Symbol s, int flg);

mal_export MalBlkPtr newMalBlk(int maxvars, int maxstmts);
mal_export void resetMalBlk(MalBlkPtr mb, int stop);
mal_export int newMalBlkStmt(MalBlkPtr mb, int maxstmts);
mal_export void resizeMalBlk(MalBlkPtr mb, int maxstmt, int maxvar);
mal_export void prepareMalBlk(MalBlkPtr mb, str s);
mal_export void freeMalBlk(MalBlkPtr mb);
mal_export MalBlkPtr copyMalBlk(MalBlkPtr mb);
mal_export void addtoMalBlkHistory(MalBlkPtr mb, str marker);
mal_export MalBlkPtr getMalBlkHistory(MalBlkPtr mb, int idx);
mal_export MalBlkPtr gotoMalBlkMarker(MalBlkPtr mb, str marker);
mal_export MalBlkPtr getMalBlkMarker(MalBlkPtr mb, str marker);
mal_export void expandMalBlk(MalBlkPtr mb, int lines);
mal_export void trimMalBlk(MalBlkPtr mb);
mal_export void trimMalVariables(MalBlkPtr mb, MalStkPtr stk);
mal_export void trimMalVariables_(MalBlkPtr mb, bit *used, MalStkPtr glb);
mal_export void moveInstruction(MalBlkPtr mb, int pc, int target);
mal_export void insertInstruction(MalBlkPtr mb, InstrPtr p, int pc);
mal_export void removeInstruction(MalBlkPtr mb, InstrPtr p);
mal_export void removeInstructionBlock(MalBlkPtr mb, int pc, int cnt);
mal_export str operatorName(int i);

mal_export int findVariable(MalBlkPtr mb, const char *name);
mal_export int findTmpVariable(MalBlkPtr mb, int type);
mal_export int findVariableLength(MalBlkPtr mb, str name, int len);
mal_export malType getType(MalBlkPtr mb, str nme);
mal_export str getArgDefault(MalBlkPtr mb, InstrPtr p, int idx);
mal_export str getVarName(MalBlkPtr mb, int i);
mal_export void setVarName(MalBlkPtr mb, int i, str nme);
mal_export int newVariable(MalBlkPtr mb, str name, malType type);
mal_export int cloneVariable(MalBlkPtr dst, MalBlkPtr src, int varid);
mal_export void renameVariable(MalBlkPtr mb, int i, str pattern, int newid);
mal_export void resetVarName(MalBlkPtr mb, int i);
mal_export int copyVariable(MalBlkPtr dst, VarPtr v);
mal_export void removeVariable(MalBlkPtr mb, int varid);
mal_export int newTmpVariable(MalBlkPtr mb, malType type);
mal_export int newTmpSink(MalBlkPtr mb, malType type);
mal_export int newTypeVariable(MalBlkPtr mb, malType type);
mal_export void delVariable(MalBlkPtr mb, int varid);
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
mal_export InstrPtr pushArgumentId(MalBlkPtr mb, InstrPtr p, str name);
mal_export void delArgument(InstrPtr p, int varid);
mal_export void setVarType(MalBlkPtr mb, int i, int tpe);
mal_export void clrAllTypes(MalBlkPtr mb);
mal_export void setArgType(MalBlkPtr mb, InstrPtr p, int i, int tpe);
mal_export void setReturnArgument(InstrPtr p, int varid);
mal_export malType destinationType(MalBlkPtr mb, InstrPtr p);
mal_export void setPolymorphic(InstrPtr p, int tpe, int force);
mal_export void pushEndInstruction(MalBlkPtr mb);	/* used in src/mal/mal_parser.c */
/* Utility macros to inspect an instruction */
#define functionStart(X) ((X)->token == FUNCTIONsymbol || \
              (X)->token == COMMANDsymbol || \
              (X)->token == FACTORYsymbol )
#define patternStart(X)  ((X)->token == PATTERNsymbol)
#define functionExit(X)  ((X)->token == ENDsymbol)

#define blockStart(X)   ((X)->barrier && (((X)->barrier == BARRIERsymbol || \
             (X)->barrier == CATCHsymbol )))
#define blockExit(X) ((X)->barrier == EXITsymbol)
#define blockCntrl(X) ( (X)->barrier== LEAVEsymbol ||  \
             (X)->barrier== REDOsymbol || (X)->barrier== RETURNsymbol )
#define isLinearFlow(X)  (!(blockStart(X) || blockExit(X) || \
				(X)->barrier== LEAVEsymbol ||  (X)->barrier== REDOsymbol ))

mal_export void strBeforeCall(ValPtr v, ValPtr bak);
mal_export void strAfterCall(ValPtr v, ValPtr bak);
mal_export void batBeforeCall(ValPtr v, ValPtr bak);
mal_export void batAfterCall(ValPtr v, ValPtr bak);
#endif /*  _MAL_INSTR_H */
