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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (author) M. Kersten 2015
*/

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "mal_function.h"   /* for getPC() */
#include "mal_utils.h"
#include "mal_exception.h"
#include "mal_listing.h"

/* 
 * Since MAL programs can be created on the fly by linked-in query
 * compilers, or transformed by optimizers, it becomes
 * mandatory to be able to produce textual correct MAL programs
 * from its internal representation for several purposes.
 * 
 * Whenever there is an overriding property it is applied.
 * 
 * The hiddenInstruction operator assumes a sufficiently large block
 * to leave information on the signature behind.
 * 
 * The protection against overflow is not tight.
*/
#define advance(X,B,L)  while(*(X) && B+L>X)(X)++;

static str
renderTerm(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int idx, int flg)
{
	char *buf;
	char *nme;
	int nameused= 0;
	size_t len = 0, maxlen = BUFSIZ;
	str pstring;
	ValRecord *val = 0;
	char *cv =0;
	str tpe;
	int showtype = 0, closequote=0;
	int varid = getArg(p,idx);

	buf = GDKzalloc(maxlen);
	// show the name when required or is used
	if ((flg & LIST_MAL_NAME) && !isVarConstant(mb,varid) && !isVarTypedef(mb,varid)) {
		nme = getVarName(mb,varid);
		snprintf(buf, maxlen, "%s", nme);
		len = strlen(buf);
		nameused =1;
	} 
	// show the value when required or being a constant
	if( ((flg & LIST_MAL_VALUE) && stk != 0) || isVarConstant(mb,varid) ){
		if (nameused){
			strcat(buf + len,"=");
			len++;
		}

		// locate value record
		if (isVarConstant(mb,varid))
			val = &getVarConstant(mb, varid);
		else if( stk)
			val = &stk->stk[varid];

		VALformat(&cv, val);
		if (strlen(cv) >= len)
			buf= GDKrealloc(buf, maxlen =len + strlen(cv) + BUFSIZ);

		if( buf == 0){
			GDKerror("renderTerm:Failed to allocate");
			return 0;
		}
		if( strcmp(cv,"nil") == 0){
			strcat(buf+len,cv);
			len += strlen(buf+len);
			if( cv) GDKfree(cv);
			showtype =getColumnType(getVarType(mb,varid)) > TYPE_str || ((isVarUDFtype(mb,varid) || isVarTypedef(mb,varid)) && isVarConstant(mb,varid)) || isaBatType(getVarType(mb,varid));
		} else{
			if ( !isaBatType(getVarType(mb,varid)) && getColumnType(getVarType(mb,varid)) > TYPE_str ){
				closequote = 1;
				strcat(buf+len,"\"");
				len++;
			}
			strcat(buf+len,cv);
			len += strlen(buf+len);
			if( cv) GDKfree(cv);

			if( closequote ){
				strcat(buf+len,"\"");
				len++;
			}
			showtype =getColumnType(getVarType(mb,varid)) > TYPE_str || ((isVarUDFtype(mb,varid) || 
					isVarTypedef(mb,varid)) && isVarConstant(mb,varid)) || isaBatType(getVarType(mb,varid));

			if (stk && isaBatType(getVarType(mb,varid)) && abs(stk->stk[varid].val.ival) ){
				BAT *d= BBPquickdesc(abs(stk->stk[varid].val.ival),TRUE);
				if( d)
					snprintf(buf+len,maxlen-len,"[" BUNFMT "]", BATcount(d));
				len += strlen(buf+len);
			}
		}
	}

	// show the type when required or frozen by the user
	// special care should be taken with constants, they may have been casted
	if ((flg & LIST_MAL_TYPE ) || (isVarUDFtype(mb, varid) && idx < p->retc) || isVarTypedef(mb,varid) || showtype){
		strcat(buf + len,":");
		len++;
		tpe = getTypeName(getVarType(mb, varid));
		snprintf(buf+len,maxlen-len,"%s",tpe);
		len += strlen(buf+len);
		GDKfree(tpe);
	}

	// show the properties when required 
	if (flg & LIST_MAL_PROPS){
		pstring = varGetPropStr(mb,varid);
		if( pstring)
			snprintf(buf+len,maxlen-len,"%s",pstring);
		len += strlen(buf + len);
		GDKfree(pstring);
	}
	if( len >= maxlen)
		GDKerror("renderTerm:Value representation too large");
	return buf;
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
	str arg, t, tpe, pstring= NULL;

	t = s;
	snprintf(t,(len-(t-base)), "%s%s ", (flg ? "" : "#"), operatorName(p->token));

	advance(t, base, len);
	snprintf(t, (len-(t-base)),  "%s.",  getModuleId(p)?getModuleId(p):"user");
	advance(t, base, len);

	pstring = varGetPropStr(mb,  getArg(p, 0));
	snprintf(t, (len-(t-base)), "%s%s(",  getFunctionId(p), pstring?pstring:"");
	if( pstring ) 
		GDKfree(pstring);
	advance(t, base, len);

	for (i = p->retc; i < p->argc; i++) {
		arg = renderTerm(mb, 0, p, i, (LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_PROPS)); 
		snprintf(t, (len-(t-base)),"%s", arg);
		advance(t,  base, len);
		GDKfree(arg);
		if( i<p->argc-1) {
			sprintf(t,",");
			advance(t,base,len);
		}
	}

	advance(t,base,len);
	if (p->varargs & VARARGS)
		sprintf(t, "...");
	advance(t,base,len);

	if (p->retc == 1) {
		*t++ = ')';
		tpe = getTypeName(getVarType(mb, getArg(p,0)));
		snprintf(t,(len-(t-base)),":%s", tpe);
		advance(t,base,len);
		GDKfree(tpe);
		if (p->varargs & VARRETS)
			sprintf(t, "...");
		advance(t,base,len);
	} else {
		sprintf(t, ") (");
		t += 3;
		for (i = 0; i < p->retc; i++) {
			arg = renderTerm(mb, 0, p, i, (LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_PROPS));
			snprintf(t,(len-(t-base)),"%s", arg);
			advance(t,base,len);
			GDKfree(arg);
			if( i<p->retc-1) {
				sprintf(t,",");
				advance(t,base,len);
			}
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
	return s;
}

str
operatorName(int i)
{
	switch (i) {
	case ASSIGNsymbol: return ":=";
	case BARRIERsymbol: return "barrier";
	case REDOsymbol: return "redo";
	case LEAVEsymbol: return "leave";
	case EXITsymbol: return "exit";
	case RETURNsymbol: return "return";
	case YIELDsymbol: return "yield";
	case CATCHsymbol: return "catch";
	case RAISEsymbol: return "raise";
	case ENDsymbol: return "end";
	case FUNCTIONsymbol: return "function";
	case FACTORYsymbol: return "factory";
	case COMMANDsymbol: return "command";
	case PATTERNsymbol: return "pattern";
	}
	return "Undefined";
}

str
instruction2str(MalBlkPtr mb, MalStkPtr stk,  InstrPtr p, int flg)
{
	int i, tab = 4;
	str base, s, t;
	size_t len=  (mb->stop < 1000? 1000: mb->stop) * 128 /* max realistic line length estimate */;
	str arg;

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
	} else
	if( functionStart(p) && flg != LIST_MAL_CALL ){
		return fcnDefinition(mb, p, s, flg, base, len);
	} else if (!functionExit(p) && flg!=LIST_MAL_CALL) {
			// beautify with tabs
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
		// is any variable explicit or used
		for (i = 0; i < p->retc; i++)
			if (!getVarTmp(mb, getArg(p, i)) || isVarUsed(mb, getArg(p, i)) || isVarUDFtype(mb,getArg(p,i)))
				break;

		if (i == p->retc)
			break;

		/* display multi-assignment list */
		if (p->retc > 1)
			*t++ = '(';

		for (i = 0; i < p->retc; i++) {
			arg= renderTerm(mb, stk, p, i, flg);
			snprintf(t,(len-(t-base)), "%s", arg);
			GDKfree(arg);
			advance(t,base,len);
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
		snprintf(t,(len-(t-base)), "end %s.%s", getModuleId(getInstrPtr(mb,0)), getFunctionId(getInstrPtr(mb, 0)));
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
	if (getModuleId(p))
		snprintf(t,  (len-(t-base)),"%s.", getModuleId(p));
	advance(t,base,len);
	if (getFunctionId(p)) {
		snprintf(t, (len-(t-base)), "%s(", getFunctionId(p));
	} else if (p->argc > p->retc + 1)
		snprintf(t, (len-(t-base)), "(");
	advance(t,base,len);

	for (i = p->retc; i < p->argc; i++) {
		arg= renderTerm(mb, stk, p, i, flg);
		snprintf(t,(len-(t-base)), "%s", arg);
		GDKfree(arg);
		advance(t,base,len);

		if (i < p->argc -1){
			snprintf(t, (len-(t-base)), ",");
			advance(t,base,len);
		}
	} 
	if (getFunctionId(p) || p->argc > p->retc + 1)
		snprintf(t,(len-(t-base)), ")");
	advance(t,base,len);
	if (p->token != REMsymbol){
		snprintf(t,(len-(t-base)), ";");
		advance(t,base,len);
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
		GDKerror("mal2str: " MAL_MALLOC_FAIL);
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
		GDKerror("mal2str: " MAL_MALLOC_FAIL);

	totlen = 0;
	for (i = first; i < last; i++) {
		if( txt[i]){
			if( ps){
				strncpy(ps + totlen, txt[i], len[i]);
				ps[totlen + len[i]] = '\n';
				ps[totlen + len[i] + 1] = 0;
				totlen += len[i] + 1;
			}
			GDKfree(txt[i]);
		}
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
		mnstr_printf(fd, "%s%s", (flg & LIST_MAL_MAPI ? "=" : ""), ps);
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
