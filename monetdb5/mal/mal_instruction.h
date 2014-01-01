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

#ifndef _MAL_INSTR_H
#define _MAL_INSTR_H

#include "mal_type.h"
#include "mal_stack.h"
#include "mal_properties.h"

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

typedef struct SYMDEF {
	struct SYMDEF *peer;		/* where to look next */
	struct SYMDEF *skip;		/* skip to next different symbol */
	str name;
	int kind;
	struct MALBLK *def;			/* the details of the MAL fcn */
} *Symbol, SymRecord;

typedef struct VARRECORD {
	str name;					/* argname or lexical value repr */
	malType type;				/* internal type signature */
	int flags;					/* see below, reserve some space */
	int tmpindex;				/* temporary variable */
	ValRecord value;
	int eolife;					/* pc index when it should be garbage collected */
	int propc, maxprop;			/* proc count and max number of properties */
	int prps[];					/* property array */
} *VarPtr, VarRecord;

/* Variable properties */
#define VAR_CONSTANT 	1
#define VAR_TYPEVAR	2
#define VAR_FIXTYPE	4
#define VAR_UDFTYPE	8
#define VAR_CLEANUP	16
#define VAR_INIT	32
#define VAR_USED	64
#define VAR_DISABLED	128		/* used for comments and scheduler */

/* type check status is kept around to improve type checking efficiency */
#define TYPE_ERROR      -1
#define TYPE_UNKNOWN     0
#define TYPE_RESOLVED    2

#define GARBAGECONTROL   3

#define VARARGS 1				/* deal with variable arguments */
#define VARRETS 2

/* all functions return a string */

typedef struct {
	bit token;					/* instruction type */
	bit barrier;				/* flow of control modifier takes:
								   BARRIER, LEAVE, REDO, EXIT, CATCH, RAISE */
	bit typechk;				/* type check status */
	bit gc;						/* garbage control flags */
	bit polymorphic;			/* complex type analysis */
	bit varargs;				/* variable number of arguments */
	int recycle;				/* <0 or index into recycle cache */
	int jump;					/* controlflow program counter */
	MALfcn fcn;					/* resolved function address */
	struct MALBLK *blk;			/* resolved MAL function address */
	str modname;				/* module context */
	str fcnname;				/* function name */
	int argc, retc, maxarg;		/* total and result argument count */
	int argv[];					/* at least a few entries */
} *InstrPtr, InstrRecord;

/* For performance analysis we keep track of the number of calls and
 * the total time spent while executing the instruction. (See
 * mal_profiler.mx) The performance structures are separately
 * administered, because they are only used in limited
 * curcumstances. */
typedef struct PERF {
#ifdef HAVE_TIMES
	struct tms timer;			/* timing information */
#endif
	struct timeval clock;		/* clock */
	lng clk;					/* time when instruction started */
	lng ticks;					/* micro seconds spent on last call */
	lng totalticks;				/* accumulate micro seconds send on this instruction */
	int calls;					/* number of calls seen */
	bit trace;					/* facilitate filter-based profiling */
	lng rbytes;					/* bytes read by an instruction */
	lng wbytes;					/* bytes written by an instruction */
} *ProfPtr, ProfRecord;

typedef struct MALBLK {
	str binding;				/* related C-function */
	str help;					/* supportive commentary */
	oid tag;					/* unique block tag */
	struct MALBLK *alternative;
	int vtop;					/* next free slot */
	int vsize;					/* size of variable arena */
	VarRecord **var;			/* Variable table */
	int stop;					/* next free slot */
	int ssize;					/* byte size of arena */
	InstrPtr *stmt;				/* Instruction location */
	int ptop;					/* next free slot */
	int psize;					/* byte size of arena */
	MalProp *prps;				/* property table */
	int errors;					/* left over errors */
	int typefixed;				/* no undetermined instruction */
	int flowfixed;				/* all flow instructions are fixed */
	ProfPtr profiler;
	struct MALBLK *history;		/* of optimizer actions */
	short keephistory;			/* do we need the history at all */
	short dotfile;				/* send dot file to stethoscope? */
	str marker;					/* history points are marked for backtracking */
	int maxarg;					/* keep track on the maximal arguments used */
	ptr replica;				/* for the replicator tests */
	sht recycle;				/* execution subject to recycler control */
	lng recid;					/* Recycler identifier */
	lng legid;					/* Octopus control */
	sht trap;					/* call debugger when called */
	lng starttime;				/* track when the query started, for resource management */
	lng runtime;				/* average execution time of block in ticks */
	int calls;					/* number of calls */
	lng optimize;				/* total optimizer time */
} *MalBlkPtr, MalBlkRecord;

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
#define ignoreVar(M,I)		((M)->var[I]->type == TYPE_ptr)
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

#define getVarConstant(M,I)	((M)->var[I]->value)
#define getVarValue(M,I)	VALget(&(M)->var[I]->value)

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

mal_export int findVariable(MalBlkPtr mb, str name);
mal_export int findTmpVariable(MalBlkPtr mb, int type);
mal_export int findVariableLength(MalBlkPtr mb, str name, int len);
mal_export malType getType(MalBlkPtr mb, str nme);
mal_export str getArgDefault(MalBlkPtr mb, InstrPtr p, int idx);
mal_export str getVarName(MalBlkPtr mb, int i);
mal_export void setVarName(MalBlkPtr mb, int i, str nme);
mal_export str getRefName(MalBlkPtr mb, int i);
mal_export int newVariable(MalBlkPtr mb, str name, malType type);
mal_export int cloneVariable(MalBlkPtr dst, MalBlkPtr src, int varid);
mal_export void renameVariable(MalBlkPtr mb, int i, str pattern, int newid);
mal_export void resetVarName(MalBlkPtr mb, int i);
mal_export int copyVariable(MalBlkPtr dst, VarPtr v);
mal_export void copyProperties(MalBlkPtr mb, int src, int dst);
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

mal_export int newProperty(MalBlkPtr mb);
#define varSetProperty(mb, var, name, opname, cst) \
	varSetProp(mb, var, PropertyIndex(name), PropertyOperator(opname), cst)
mal_export void varSetProp(MalBlkPtr mb, int var, int prop, int op, ValPtr cst);
mal_export str varGetPropStr(MalBlkPtr mb, int var);
mal_export VarPtr varGetProp(MalBlkPtr mb, int var, int prop);

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
