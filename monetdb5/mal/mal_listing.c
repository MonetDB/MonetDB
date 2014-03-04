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

/*
 * (author) M. Kersten
*/

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "mal_function.h"   /* for getPC() */
#include "mal_utils.h"
#include "mal_exception.h"
#include "mal_listing.h"

/* Reverse programming
 * Since MAL programs can be created on the fly by linked-in query
 * compilers, or transformed by optimizers, it becomes
 * mandatory to be able to produce textual correct MAL programs
 * from its internal representation.
 * 
 * No guarantee is given to produce the exact input for a reversed
 * MAL program, except that the output can be fed back for
 * interpretation. Provided, the code did not produce an error.
 * 
 * The hiddenInstruction operator assumes a sufficiently large block
 * to leave information on the signature behind.
 * 
 * The protection against overflow is not tight.
*/
#define advance(X,B,L)  while(*(X) && B+L>X)(X)++;

str
instructionCall(MalBlkPtr mb, InstrPtr p, str s, str base, size_t len)
{
	int i, closing=0, margin = 2 * PATHLENGTH;
	str tpe;
	if (p->retc > 1) {
		*s++ = '(';
		*s = 0;
	}
	for (i = 0; i <= p->argc; i++) {
		if (i == p->retc) {
			if (i > 1 && p->retc > 1) {
				*s++ = ')';
				*s = 0;
			}
			if (p->blk && p->blk->binding) {
				snprintf(s, (len - (s - base)), " := %s(", p->blk->binding);
				closing = 1;
			} else if (getFunctionId(p)) {
				snprintf(s, (len - (s - base)), " := %s.%s(", getModuleId(p), getFunctionId(p));
				closing = 1;
			} else if (p->argc > p->retc)
				sprintf(s, " := ");
			advance(s, base, len);
		}
		if (i < p->argc && p->argv[i] >= 0) {
			str nme;
			char nmebuf[PATHLENGTH];

			tpe = getTypeName(getArgType(mb, p, i));
			if (isTmpVar(mb, getArg(p, i)) || isVarTypedef(mb, getArg(p, i))) {
				snprintf(nmebuf, PATHLENGTH, "%c%d", TMPMARKER, getVarTmp(mb, getArg(p, i)));
				nme = nmebuf;
			} else
				nme = getArgName(mb, p, i);
			snprintf(s, (len - margin - (s - base)), "%s:%s", (nme ? nme : "nil"), tpe);
			advance(s, base, len);
			if (i != p->retc - 1 && i < p->argc - 1)
				sprintf(s, ", ");

			GDKfree(tpe);
			advance(s, base, len);
		}
	}
	if (closing)
		*s++ = ')';
	*s = 0;
	return s;
}

static str
hiddenInstructionArgs(MalBlkPtr mb, InstrPtr p, str s, str start, int flg, str base, size_t len)
{
	int i;

	i= (int)(s-start);
	while (i++< 40) *s++= ' ';	/* to give a better look to most programs */
	*s = 0;
	snprintf(s, (len-(s-base)), "#%3d ", getPC(mb, p));
	advance(s,base,len);
	if (p->token == REMsymbol )
		return s;
	s = instructionCall(mb,p,s, base, len);
/*
 * The instruction is complemented with simple flags to ease debugging.
 * To limit the impact on test output, we only display them when they
 * are set.
 * 
 * D = debug the function call
 * U = types not yet resolved
 * P = polymorphic instruction
 * G = subject to garbage control
 * R = subject to recycler control
 * J = jump towards other statement
*/
	if (flg & LIST_MAL_STMT){
		*s++ =' ';
		*s++ ='{';
		switch( p->typechk){
		case TYPE_UNKNOWN:
			*s++ ='U'; break;
		case TYPE_RESOLVED:
			/* implicit *s++ =' '; */ break;
		}
		if ( mb->trap && mb->stmt[0] == p)
			*s++ ='D';
		if (p->polymorphic)
			*s++ ='P';
		if (p->gc)
			*s++ ='G';
		if (p->recycle)
			*s++ ='R';
		if (p->jump){
			sprintf(s, "J%d", p->jump);
			advance(s,base,len);
		}
		if( *(s-1) != '{')
			*s++ ='}';
		else s--;
		*s=0;
	}
	advance(s,base,len);
	return s;
}

/*
It receives the space to store the definition
The MAL profiler dumps some performance data at the
beginning of each line.
*/

str
fcnDefinition(MalBlkPtr mb, InstrPtr p, str s, int flg, str base, size_t len)
{
	int i;
	str t, tpe, pstring= NULL;

	t = s;
	snprintf(t,(len-(t-base)), "%s%s ", (flg ? "" : "#"), operatorName(p->token));

	advance(t,base,len);
	if (getModuleId(p))
		snprintf(t,(len-(t-base)), "%s.", getModuleId(p));
	else
		snprintf(t,(len-(t-base)),"user.");
	advance(t,base,len);

	pstring = varGetPropStr(mb, getArg(p,0));
	if( pstring ) {
		snprintf(t,(len-(t-base)),"%s%s(", getFunctionId(p),pstring);
		GDKfree(pstring);
	} else 
		snprintf(t,(len-(t-base)),"%s(", getFunctionId(p));
	for (i = p->retc; i < p->argc; i++) {
		tpe= getTypeName(getArgType(mb,p,i));
		if (flg & LIST_MAL_PROPS)
			pstring = varGetPropStr(mb, getArg(p,i));
		else pstring = 0;
		advance(t,base,len);
		snprintf(t,(len-(t-base)),"%s%s", (*getArgName(mb,p,i) == TMPMARKER?"X":""), getArgName(mb, p, i));
		advance(t,base,len);
		snprintf(t,(len-(t-base)),":%s%s",tpe, (pstring?pstring:""));
		advance(t,base,len);
		if( i<p->argc-1) sprintf(t,",");
		if(pstring) { GDKfree(pstring); pstring=0;}
		GDKfree(tpe);
	}

	advance(t,base,len);
	if (p->varargs & VARARGS)
		sprintf(t, "...");
	advance(t,base,len);
	if (p->retc == 1) {
		tpe = getTypeName(getArgType(mb, p, 0));
		snprintf(t,(len-(t-base)),"):%s", tpe);
		advance(t,base,len);
		if (p->varargs & VARRETS)
			sprintf(t, "...");
		GDKfree(tpe);
		advance(t,base,len);
	} else {
		sprintf(t, ") (");
		t += 3;
		for (i = 0; i < p->retc; i++) {
			tpe= getTypeName(getArgType(mb,p,i));
			if (flg & LIST_MAL_PROPS)
				pstring = varGetPropStr(mb, getArg(p,i));
			else pstring = 0;
			advance(t,base,len);
			snprintf(t,(len-(t-base)),"%s%s", (*getArgName(mb,p,i) == TMPMARKER?"X":""), getArgName(mb, p, i));
			advance(t,base,len);
			snprintf(t,(len-(t-base)),":%s%s",tpe, (pstring?pstring:""));
			advance(t,base,len);
			if( i<p->retc-1) sprintf(t,",");
			if(pstring) { GDKfree(pstring); pstring=0;}
			GDKfree(tpe);

		}
		if (p->varargs & VARRETS)
			sprintf(t, "...");
		advance(t,base,len);
		*t++ = ')';
	}
	if (mb->binding)
		snprintf(t,(len-(t-base))," address %s;", mb->binding);
	else
		sprintf(t, ";");
	if (flg & LIST_MAL_DETAIL) {
		advance(t,base,len);
		hiddenInstructionArgs(mb, p, t,s, flg,base,len);
	}
	return s;
}

str
operatorName(int i)
{
	switch (i) {
	case ASSIGNsymbol:
		return ":=";
	case BARRIERsymbol:
		return "barrier";
	case REDOsymbol:
		return "redo";
	case LEAVEsymbol:
		return "leave";
	case EXITsymbol:
		return "exit";
	case RETURNsymbol:
		return "return";
	case YIELDsymbol:
		return "yield";
	case CATCHsymbol:
		return "catch";
	case RAISEsymbol:
		return "raise";
	case ENDsymbol:
		return "end";
	case FUNCTIONsymbol:
		return "function";
	case FACTORYsymbol:
		return "factory";
	case COMMANDsymbol:
		return "command";
	case PATTERNsymbol:
		return "pattern";
	}
	return "Undefined";
}

str
instruction2str(MalBlkPtr mb, MalStkPtr stk,  InstrPtr p, int flg)
{
	int i, tab = 4;
	str base, s, t;
	size_t len=  (mb->stop < 1000? 1000: mb->stop) * 128 /* max realistic line length estimate */;
	int low, high;
	char nmebuf[PATHLENGTH];
	str pstring = NULL;
	str cv = NULL;

	base = s = GDKmalloc(len);
	if ( s == NULL)
		return s;
	if (flg) {
		if( p->token<0){
			s[0] = '#';
			s[1] = 0;
			t = s+1;
		}else{
			s[0] = 0;
			t = s;
		}
	} else {
		s[0] = '#';
		if (p->typechk == TYPE_UNKNOWN) {
			s[1] = '!';	/* error */
			s[2] = 0;
			t = s + 2;
		} else {
			s[1] = 0;
			t = s + 1;
		}
	}
	if (flg & LIST_MAL_LNR){
		snprintf(t,len-1,"#%3d ",getPC(mb,p));
		advance(t,base,len);
	}
	if (p->argc > 0 && isTmpVar(mb, getArg(p, 0))) {
		if (isVarUsed(mb, getDestVar(p))) {
			snprintf(nmebuf, PATHLENGTH, "%c%d", TMPMARKER, getVarTmp(mb, getArg(p, 0)));
		} else
			nmebuf[0] = 0;
	} 
	advance(t,base,len);
	if (p->token == REMsymbol) {
		/* do nothing */
	} else if (p->barrier) {
		if (p->barrier == LEAVEsymbol || 
			p->barrier == REDOsymbol || 
			p->barrier == RETURNsymbol || 
			p->barrier == YIELDsymbol || 
			p->barrier == RAISEsymbol) {
    			for(;tab>0;tab--) 
				*t++= ' ';
    			*t= 0;
    			advance(t,base,len);
		}
		snprintf(t,(len-(t-base)), "%s ", operatorName(p->barrier));
		advance(t,base,len);
	} else if (!functionStart(p) && !functionExit(p) && flg!=LIST_MAL_CALL) {
    		for(;tab>0;tab--) 
			*t++= ' ';
    		*t= 0;
    		advance(t,base,len);
	}
	switch (p->token<0?-p->token:p->token) {
	case FCNcall:
	case FACcall:
	case PATcall:
	case CMDcall:
	case ASSIGNsymbol :
		for (i = 0; i < p->retc; i++)
			if (!getVarTmp(mb, getArg(p, i)) || isVarUsed(mb, getArg(p, i)) || isVarUDFtype(mb,getArg(p,i)))
				break;

		if (i == p->retc)
			break;

		/* display multi-assignment */
		if (p->retc > 1)
			*t++ = '(';

		for (i = 0; i < p->retc; i++) {
			if (flg & LIST_MAL_STMT) {
				snprintf(t,(len-(t-base)),"%s%s", (*getArgName(mb,p,i) == TMPMARKER?"X":""), getArgName(mb, p, i));
				advance(t,base,len);
				if ( flg & LIST_MAL_TYPE ){
					str tpe = getTypeName(getVarType(mb, getArg(p, i)));
					snprintf(t,(len-(t-base)), ":%s ", tpe);
					GDKfree(tpe);
					advance(t,base,len);
				} else
					if ( flg & (LIST_MAL_UDF | LIST_MAL_STMT | LIST_MAL_VALUE)  && i < p->retc) {
					if ( isVarUDFtype(mb, getArg(p, i))) {
						str tpe = getTypeName(getVarType(mb, getArg(p, i)));
						snprintf(t,(len-(t-base)), ":%s ", tpe);
						GDKfree(tpe);
						advance(t,base,len);
					} 
				}
				if (flg & LIST_MAL_PROPS ){
					pstring = varGetPropStr(mb, getArg(p, i));
					if (pstring){
						snprintf(t,(len-(t-base)),"%s", pstring);
						advance(t,base,len);
						GDKfree(pstring);
					}
				}
			}

			if (stk && (flg & LIST_MAL_VALUE) && isaBatType(getArgType(mb,p,i))) {
				BAT *d = 0;
				cv = NULL;
				VALformat(&cv, &stk->stk[getArg(p, i)]);
				if ( cv && strlen(cv) > len - (t - s)) {
					char *ns = (char *) GDKmalloc(len = strlen(cv) + len + 5);
					if ( ns == NULL){
						GDKerror(MAL_MALLOC_FAIL);
					} else {
						*t = 0;
						strcpy(ns, s);
						t = ns + (t - s);
						GDKfree(s);
						s = ns;
					}
				}
				if ( cv && strcmp(cv,"nil") ){
					strcat(t, "=");
					strcat(t, cv);
					advance(t,base,len);
					if (cv)
						GDKfree(cv);
					if ( abs(stk->stk[getArg(p,i)].val.ival) ){
						d= BBPquickdesc(abs(stk->stk[getArg(p,i)].val.ival),TRUE);
						if( d){
							snprintf(t,(len-(t-base)),"[" BUNFMT "]", BATcount(d));
							advance(t,base,len);
						}
					} else {
						snprintf(t,(len-(t-base)),"[ nil ]");
						advance(t,base,len);
					}
				} else strcat(t, "=nil");
			}
			if (i < p->retc - 1)
				*t++ = ',';
		}
		if (p->retc > 1)
			*t++ = ')';

		if (p->argc > p->retc || getFunctionId(p)) {
			sprintf(t, " := ");
			t += 4;
		}
		*t = 0;
		break;
	case ENDsymbol:
		snprintf(t,(len-(t-base)), "end %s", getFunctionId(getInstrPtr(mb, 0)));
		advance(t,base,len);
		break;
	case COMMANDsymbol:
	case FUNCTIONsymbol:
	case FACTORYsymbol:
	case PATTERNsymbol:
		if (flg & LIST_MAL_VALUE) {
			snprintf(t,(len-(t-base)), "%s ", operatorName(p->token));
			advance(t,base,len);
			break;
		}
		return fcnDefinition(mb, p, s, flg, base, len);
	case REMsymbol:
	case NOOPsymbol:
		if(getVar(mb, getArg(p, 0))->value.val.sval) 
			snprintf(t,(len-(t-base)), "#%s ", getVar(mb, getArg(p, 0))->value.val.sval);
		else
			snprintf(t, (len-(t-base)), "# ");
		break;
	default:
		snprintf(t,  (len-(t-base))," unknown symbol ?%d? ", p->token);
	}
	advance(t,base,len);
	low = p->retc;
	high = p->argc;
	if (getModuleId(p))
		snprintf(t,  (len-(t-base)),"%s.", getModuleId(p));
	advance(t,base,len);
	if (getFunctionId(p)) {
		snprintf(t, (len-(t-base)), "%s(", getFunctionId(p));
	} else if (p->argc > p->retc + 1)
		snprintf(t, (len-(t-base)), "(");
	advance(t,base,len);

	for (i = low; i < high; i++) {
		advance(t,base,len);
		if (i >low){
			snprintf(t, (len-(t-base)), ",");
			advance(t,base,len);
		}
		if (i + 1 == high && p->varargs & VARARGS) {
			snprintf(t, (len-(t-base)), "...");
			advance(t,base,len);
			break;
		}
		/* show the value if availabe */
		if ( (isVarConstant(mb, getArg(p, i)) || stk) && !isVarTypedef(mb,getArg(p,i)) ){

			if (stk && flg & LIST_MAL_VALUE){
				if ( !isVarConstant(mb, getArg(p,i)) && flg & LIST_MAL_ARG)
					snprintf(t,(len-(t-base)),"%s%s", (*getArgName(mb,p,i)== TMPMARKER?"X":""), getArgName(mb, p, i));
				advance(t,base,len);
				if( getColumnType(getArgType(mb,p,i)) > TYPE_str )
				{ 	char *ct;
					VALformat(&cv, &stk->stk[getArg(p, i)]);
					ct= (char*) GDKmalloc(1024+strlen(cv));
					if ( ct == NULL){
						GDKerror("instruction2str"MAL_MALLOC_FAIL);
						break;
					}
					if (isVarUDFtype(mb, getArg(p, i)) ) {
						if ( strcmp(cv,"nil") == 0)
							snprintf(ct, 1024+strlen(cv), "=%s", cv);
						else
							snprintf(ct, 1024+strlen(cv), "=\"%s\"", cv);
					} else
					if ( strcmp(cv,"nil") == 0)
						snprintf(ct, 1024+strlen(cv), "=%s:%s", cv, getTypeName(getColumnType(getArgType(mb,p,i))));
					else
						snprintf(ct, 1024+strlen(cv), "=\"%s\":%s", cv, getTypeName(getColumnType(getArgType(mb,p,i))));
					if( cv) GDKfree(cv);
					cv= ct;
				} else
					VALformat(&cv, &stk->stk[getArg(p, i)]);
			} else {
				if ( p->recycle && flg & LIST_MAL_ARG)
					snprintf(t,(len-(t-base)),"%s%s=", (*getArgName(mb,p,i)== TMPMARKER?"X":""), getArgName(mb, p, i));
				advance(t,base,len);
				if( getColumnType(getArgType(mb,p,i)) > TYPE_str )
				{ char *ct=cv;
					VALformat(&cv, &getVar(mb, getArg(p, i))->value);
					ct= (char*) GDKmalloc(1024+strlen(cv));
					if ( ct == NULL){
						GDKerror("instruction2str"MAL_MALLOC_FAIL);
						break;
					}
					if (isVarUDFtype(mb, getArg(p, i)) ) {
						if ( strcmp(cv,"nil") == 0)
							snprintf(ct, 1024+strlen(cv), "%s", cv);
						else
							snprintf(ct, 1024+strlen(cv), "\"%s\"", cv);
					} else
					if ( strcmp(cv,"nil") == 0)
						snprintf(ct, 1024+strlen(cv), "%s:%s", cv, getTypeName(getColumnType(getArgType(mb,p,i))));
					else
						snprintf(ct, 1024+strlen(cv), "\"%s\":%s", cv, getTypeName(getColumnType(getArgType(mb,p,i))));
					if( cv) GDKfree(cv);
					cv= ct;
				} else
					VALformat(&cv, &getVar(mb, getArg(p, i))->value);
			}
			if ( cv && strlen(cv) > len - (t - s)) {
				char *ns = (char *) GDKmalloc(len = strlen(cv) + len + 5);
				if ( ns == NULL){
					GDKerror("instruction2str"MAL_MALLOC_FAIL);
					return s;
				}

				*t = 0;
				strcpy(ns, s);
				t = ns + (t - s);
				GDKfree(s);
				s = ns;
			}
			if ( cv )  {
				if ( !isVarConstant(mb, getArg(p, i)) )
					strcat(t,"=");
				strcat(t, cv);
			}
			
			advance(t,base,len);
			if ( (cv && strlen(cv)==0) || isVarUDFtype(mb, getArg(p, i)) ||
				isAmbiguousType(getArgType(mb,p,i)) ){
				str tpe = getTypeName(getVarType(mb, getArg(p, i)));
				snprintf(t,(len-(t-base)), ":%s", tpe);
				GDKfree(tpe);
				advance(t,base,len);
			}
			if (cv)
				GDKfree(cv);
			if( flg & LIST_MAL_VALUE && isaBatType(getVarType(mb,getArg(p,i)) ) ){
				BAT *d = 0;
				if (stk && stk->stk[getArg(p,i)].vtype== TYPE_bat){
					d= BBPquickdesc(abs(stk->stk[getArg(p,i)].val.ival),TRUE);
				} else
					d= BBPquickdesc(abs(getVarConstant(mb,getArg(p,i)).val.ival),TRUE);
				if( d){
					snprintf(t,(len-(t-base)),"[" BUNFMT "]", BATcount(d));
					advance(t,base,len);
				}
			}
		} else {
			if( ! isVarTypedef(mb,getArg(p,i))  ){
				snprintf(t,(len-(t-base)),"%s%s", (*getArgName(mb,p,i) == TMPMARKER?"X":""), getArgName(mb, p, i));
				advance(t,base,len);
				if ( flg & LIST_MAL_TYPE ){
					str tpe = getTypeName(getVarType(mb, getArg(p, i)));
					snprintf(t,(len-(t-base)), ":%s ", tpe);
					GDKfree(tpe);
					advance(t,base,len);
				} else
					if ( flg & (LIST_MAL_UDF | LIST_MAL_STMT | LIST_MAL_VALUE)  && i < p->retc) {
					if ( isVarUDFtype(mb, getArg(p, i))) {
						str tpe = getTypeName(getVarType(mb, getArg(p, i)));
						snprintf(t,(len-(t-base)), ":%s ", tpe);
						GDKfree(tpe);
						advance(t,base,len);
					} 
				}
				if (flg & LIST_MAL_PROPS ){
					pstring = varGetPropStr(mb, getArg(p, i));
					if (pstring){
						snprintf(t,(len-(t-base)),"%s", pstring);
						advance(t,base,len);
						GDKfree(pstring);
					}
				}
			} else {
				str tpe = getTypeName(getVarType(mb, getArg(p, i)));
				snprintf(t,(len-(t-base)), ":%s", tpe);
				GDKfree(tpe);
				advance(t,base,len);
				if (flg & LIST_MAL_PROPS){
					pstring = varGetPropStr(mb, getArg(p, i));
					if (pstring){
						snprintf(t,(len-(t-base)),"%s", pstring);
						advance(t,base,len);
						GDKfree(pstring);
					}
				}
			} 
		} 
	} 
	if (getFunctionId(p) || p->argc > p->retc + 1)
		snprintf(t,(len-(t-base)), ")");
	advance(t,base,len);
	if (p->token != REMsymbol){
		snprintf(t,(len-(t-base)), ";");
		advance(t,base,len);
		if (flg & LIST_MAL_DETAIL) {
			advance(t,base,len);
			t = hiddenInstructionArgs(mb, p, t,s, flg,base,len);
		}
	}
	/* we may accidentally overwrite */
	if (t > s + len)
		GDKfatal("instruction2str:");
	return s;
}

str
mal2str(MalBlkPtr mb, int flg, int first, int last)
{
	str ps, *txt;
	int i, *len, totlen = 0;

	txt = GDKmalloc(sizeof(str) * mb->stop);
	len = GDKmalloc(sizeof(int) * mb->stop);

	if( txt == NULL || len == NULL){
		GDKerror("mal2str"MAL_MALLOC_FAIL);
		if( txt ) GDKfree(txt);
		if( len ) GDKfree(len);
		return NULL;
	}
	for (i = first; i < last; i++) {
		txt[i] = instruction2str(mb, 0, getInstrPtr(mb, i), flg);
		if ( txt[i])
			totlen += len[i] = (int)strlen(txt[i]);
	}
	ps = GDKmalloc(totlen + mb->stop + 1);
	if( ps == NULL)
		GDKerror("mal2str"MAL_MALLOC_FAIL);

	totlen = 0;
	for (i = first; i < last; i++) 
	if( txt[i]){
		if( ps){
			strncpy(ps + totlen, txt[i], len[i]);
			ps[totlen + len[i]] = '\n';
			ps[totlen + len[i] + 1] = 0;
			totlen += len[i] + 1;
		}
		GDKfree(txt[i]);
	}
	GDKfree(len);
	GDKfree(txt);
	return ps;
}

str
function2str(MalBlkPtr mb, int flg){
	return mal2str(mb,flg,0,mb->stop);
}

void
promptInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg)
{
	str ps;

	if (fd == 0)
		return;
	ps = instruction2str(mb, stk, p, flg);
	/* ps[strlen(ps)-1] = 0; remove '\n' */
	if ( ps ){
		mnstr_printf(fd, "%s%s", (flg & LIST_MAPI ? "=" : ""), ps);
		GDKfree(ps);
	}
}

void
printInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg)
{
	promptInstruction(fd, mb, stk, p, flg);
	mnstr_printf(fd, "\n");
}

void
printSignature(stream *fd, Symbol s, int flg)
{
	InstrPtr p;
	str txt;

	if ( s->def == 0 ){
		mnstr_printf(fd, "missing definition of %s\n", s->name);
		return;
	}
	txt = GDKzalloc(MAXLISTING); /* some slack for large blocks */
	if( txt){
		p = getSignature(s);
		(void) fcnDefinition(s->def, p, txt, flg, txt, MAXLISTING);
		mnstr_printf(fd, "%s\n", txt);
		GDKfree(txt);
	} else GDKerror("printSignature"MAL_MALLOC_FAIL);
}

/*
 * For clarity we show the last optimizer applied
 * also as the last of the list, although it is linked with mb.
*/
void showMalBlkHistory(stream *out, MalBlkPtr mb)
{
	MalBlkPtr m=mb;
	InstrPtr p,sig;
	int j=0;
	str msg;

	sig = getInstrPtr(mb,0);
	m= m->history;
	while(m){
		p= getInstrPtr(m,m->stop-1);
		if( p->token == REMsymbol){
			msg= instruction2str(m, 0, p, FALSE);
			if (msg ) {
				mnstr_printf(out,"%s.%s[%2d] %s\n", 
					getModuleId(sig), getFunctionId(sig),j++,msg+3);
				GDKfree(msg);
			}
		} 
		m= m->history;
	}
	p=getInstrPtr(mb,mb->stop-1);
	if( p->token == REMsymbol){
		msg= instruction2str(mb, 0, p, FALSE);
		if (msg) {
			mnstr_printf(out,"%s.%s[%2d] %s\n", 
				getModuleId(sig), getFunctionId(sig),j++,msg+3);
				GDKfree(msg);
		}
		} 
}
