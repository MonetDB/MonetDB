/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
	char *buf =0;
	char *nme =0;
	int nameused = 0;
	size_t len = 0, maxlen = BUFSIZ;
	ValRecord *val = 0;
	char *cv =0;
	str tpe;
	int showtype = 0, closequote=0;
	int varid = getArg(p,idx);

	buf = GDKzalloc(maxlen);
	if( buf == NULL) {
		return NULL;
	}
	// show the name when required or is used
	if ((flg & LIST_MAL_NAME) && !isVarConstant(mb,varid) && !isVarTypedef(mb,varid)) {
		nme = getVarName(mb,varid);
		len +=snprintf(buf, maxlen, "%s", nme);
		nameused =1;
	} 
	// show the value when required or being a constant
	if( ((flg & LIST_MAL_VALUE) && stk != 0) || isVarConstant(mb,varid) ){
		if (nameused){
			strcat(buf + len,"=");
			len++;
		}

		// locate value record
		if (isVarConstant(mb,varid)){
			val = &getVarConstant(mb, varid);
			showtype= getVarType(mb,varid) != TYPE_str && getVarType(mb,varid) != TYPE_bit;
		} else if( stk)
			val = &stk->stk[varid];

		if (VALformat(&cv, val) <= 0) {
			GDKfree(buf);
			GDKerror("renderTerm:Failed to allocate");
			return NULL;
		}
		if (len + strlen(cv) >= maxlen) {
			char *nbuf = GDKrealloc(buf, maxlen =len + strlen(cv) + BUFSIZ);
			if (nbuf == NULL) {
				GDKfree(buf);
				return NULL;
			}
			buf = nbuf;
		}

		if( strcmp(cv,"nil") == 0){
			strcat(buf+len,cv);
			len += strlen(buf+len);
			GDKfree(cv);
			showtype = showtype || getBatType(getVarType(mb,varid)) > TYPE_str || 
				((isVarUDFtype(mb,varid) || isVarTypedef(mb,varid)) && isVarConstant(mb,varid)) || isaBatType(getVarType(mb,varid)); 
		} else{
			if ( !isaBatType(getVarType(mb,varid)) && getBatType(getVarType(mb,varid)) > TYPE_str ){
				closequote = 1;
				strcat(buf+len,"\"");
				len++;
			}
			strcat(buf+len,cv);
			len += strlen(buf+len);
			GDKfree(cv);

			if( closequote ){
				strcat(buf+len,"\"");
				len++;
			}
			showtype = showtype || closequote > TYPE_str || ((isVarUDFtype(mb,varid) || isVarTypedef(mb,varid) || (flg & LIST_MAL_REMOTE)) && isVarConstant(mb,varid)) ||
				(isaBatType(getVarType(mb,varid)) && idx < p->retc);

			if (stk && isaBatType(getVarType(mb,varid)) && stk->stk[varid].val.bval ){
				BAT *d= BBPquickdesc(stk->stk[varid].val.bval,TRUE);
				if( d)
					len += snprintf(buf+len,maxlen-len,"[" BUNFMT "]", BATcount(d));
			}
		}
	}

	// show the type when required or frozen by the user
	// special care should be taken with constants, they may have been casted
	if ((flg & LIST_MAL_TYPE) || (isVarUDFtype(mb, varid) && idx < p->retc) || isVarTypedef(mb,varid) || showtype){
		strcat(buf + len,":");
		len++;
		tpe = getTypeName(getVarType(mb, varid));
		if (tpe == NULL) {
			GDKfree(buf);
			GDKerror("renderTerm:Failed to allocate");
			return NULL;
		}
		len += snprintf(buf+len,maxlen-len,"%s",tpe);
		GDKfree(tpe);
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
	str arg, t, tpe;

	t = s;
	snprintf(t,(len-(t-base)), "%s", (flg ? "" : "#") );
	advance(t, base, len);
	if( mb->inlineProp){
		snprintf(t,(len-(t-base)), "inline ");
		advance(t, base, len);
	}
	if( mb->unsafeProp){
		snprintf(t,(len-(t-base)), "unsafe ");
		advance(t, base, len);
	}
	if( mb->sealedProp){
		snprintf(t,(len-(t-base)), "sealed ");
		advance(t, base, len);
	}
	snprintf(t,(len-(t-base)), "%s ",  operatorName(p->token));

	advance(t, base, len);
	snprintf(t, (len-(t-base)),  "%s.",  getModuleId(p)?getModuleId(p):"user");
	advance(t, base, len);
	snprintf(t, (len-(t-base)), "%s(",  getFunctionId(p));
	advance(t, base, len);

	for (i = p->retc; i < p->argc; i++) {
		arg = renderTerm(mb, 0, p, i, (LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_PROPS));
		if (arg) {
			snprintf(t, (len-(t-base)),"%s", arg);
			GDKfree(arg);
		}
		advance(t,  base, len);
		if( i<p->argc-1) {
			sprintf(t,", ");
			advance(t,base,len);
		}
	}

	advance(t,base,len);
	if (p->varargs & VARARGS && t < base + len -3)
		sprintf(t, "...");
	advance(t,base,len);

	if (p->retc == 1 && t < base +len) {
		*t++ = ')';
		tpe = getTypeName(getVarType(mb, getArg(p,0)));
		snprintf(t,(len-(t-base)),":%s", tpe);
		advance(t,base,len);
		GDKfree(tpe);
		if (p->varargs & VARRETS && t < base + len -3)
			sprintf(t, "...");
		advance(t,base,len);
	} else {
		if( t < base +len -3) sprintf(t, ") (");
		t += 3;
		for (i = 0; i < p->retc; i++) {
			arg = renderTerm(mb, 0, p, i, (LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_PROPS));
			if (arg) {
				snprintf(t,(len-(t-base)),"%s", arg);
				GDKfree(arg);
			}
			advance(t,base,len);
			if( i<p->retc-1 && t < base + len) {
				sprintf(t,", ");
				advance(t,base,len);
			}
		}
		if (p->varargs & VARRETS && t < base +len -3)
			sprintf(t, "...");
		advance(t,base,len);
		if(t < base + len) *t++ = ')';
	}

	if (mb->binding[0])
		snprintf(t,(len-(t-base))," address %s;", mb->binding);
	else
		if( t <base + len) sprintf(t, ";");
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
		s[0] = 0;
		t = s;
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
	if (p->token == REMsymbol && !( getModuleId(p) && strcmp(getModuleId(p),"querylog") == 0  && getFunctionId(p) && strcmp(getFunctionId(p),"define") == 0)) {
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
			if ( !isTmpVar(mb,getArg(p,i)) || isVarUsed(mb, getArg(p, i)) || isVarUDFtype(mb,getArg(p,i)))
				break;

		if (i == p->retc)
			break;

		/* display multi-assignment list */
		if (p->retc > 1)
			if( t< base+len) *t++ = '(';

		for (i = 0; i < p->retc; i++) {
			arg= renderTerm(mb, stk, p, i, flg);
			if (arg) {
				snprintf(t,(len-(t-base)), "%s", arg);
				GDKfree(arg);
			}
			advance(t,base,len);
			if ( t < base+len && i < p->retc - 1){
				*t++ = ',';
				*t++ = ' ';
			}
		}
		if (p->retc > 1)
			if( t< base+len) *t++ = ')';

		if (p->argc > p->retc || getFunctionId(p)) {
			if( t< base+len-4) {
				sprintf(t, " := ");
				t += 4;
			}
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
		if(getVar(mb, getArg(p, 0))->value.val.sval && getVar(mb, getArg(p, 0))->value.len > 0) 
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
		if (arg) {
			snprintf(t,(len-(t-base)), "%s", arg);
			GDKfree(arg);
		}
		advance(t,base,len);

		if (i < p->argc -1 && t < base + len){
			snprintf(t, (len-(t-base)), ", ");
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
	return base;
}

/* the MAL beautifier is meant to simplify correlation of MAL variables and
 * the columns in the underlying database.
 * If the status is set, then we consider the instruction DONE and the result variables 
 * should be shown as well.
 */
static str
shortRenderingTerm(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int idx)
{
	str s, nme;
	BAT *b;
	ValRecord *val;
	char *cv =0;
	int varid = getArg(p,idx);
	size_t len = BUFSIZ;

	s= GDKmalloc(len);
	if( s == NULL)
		return NULL;
	*s = 0;

	if( isVarConstant(mb,varid) ){
		val =&getVarConstant(mb, varid);
		if(VALformat(&cv, val) < 0) {
			GDKfree(s);
			return NULL;
		}
		if (strlen(cv) >= len) {
			char *nbuf;
			len = strlen(cv);
			nbuf = GDKrealloc(s, len + 1);
			if (nbuf == NULL) {
				GDKfree(s);
				return NULL;
			}
			s = nbuf;
		}
		snprintf(s,len + 1,"%s",cv);
	} else {
		val = &stk->stk[varid];
		if(VALformat(&cv, val) < 0) {
			GDKfree(s);
			return NULL;
		}
		nme = getVarName(mb, varid);
		if ( isaBatType(getArgType(mb,p,idx))){
			b = BBPquickdesc(stk->stk[varid].val.bval,TRUE);
			snprintf(s,BUFSIZ,"%s["BUNFMT"]" ,nme, b?BATcount(b):0);
		} else
		if( cv)
			snprintf(s,BUFSIZ,"%s=%s ",nme,cv);
		else
			snprintf(s,BUFSIZ,"%s ",nme);
	}
	GDKfree(cv);
	return s;
}

str
shortStmtRendering(MalBlkPtr mb, MalStkPtr stk,  InstrPtr p)
{
	int i;
	str base, s, t, nme;
	size_t len=  (mb->stop < 1000? 1000: mb->stop) * 128 /* max realistic line length estimate */;

	base = s = GDKmalloc(len);
	if ( s == NULL)
		return s;
	*s =0;
	t=s;
	if (p->token == REMsymbol && !( getModuleId(p) && strcmp(getModuleId(p),"querylog") == 0  && getFunctionId(p) && strcmp(getFunctionId(p),"define") == 0)) 
		return base;
	if (p->barrier == LEAVEsymbol || 
		p->barrier == REDOsymbol || 
		p->barrier == RETURNsymbol || 
		p->barrier == YIELDsymbol || 
		p->barrier == EXITsymbol || 
		p->barrier == RAISEsymbol) {
			snprintf(t,(len-(t-base)), "%s ", operatorName(p->barrier));
			advance(t,base,len);
		}
	if( p->token == FUNCTIONsymbol) {
			snprintf(t,(len-(t-base)), "function %s.", getModuleId(p));
			advance(t,base,len);
		}
	if (p->token == ENDsymbol ){
		snprintf(t,(len-(t-base)), "end %s.%s", getModuleId(getInstrPtr(mb,0)), getFunctionId(getInstrPtr(mb,0)));
		return base;
	}
	// handle the result variables
	for (i = 0; i < p->retc; i++)
		if ( !isTmpVar(mb,getArg(p,i)) || isVarUsed(mb, getArg(p, i)) || isVarUDFtype(mb,getArg(p,i)))
			break;

	if (i == p->retc) // no result arguments
		goto short_end;

	/* display optional multi-assignment list */
	if( getArgType(mb,p,0) != TYPE_void){
		if (p->retc > 1 && t < base + len-1){
			*t++ = '(';
			*t=0;
		}

		for (i = 0; i < p->retc; i++) {
			nme = shortRenderingTerm(mb, stk, p,i);
			snprintf(t,(len-(t-base)), "%s%s", (i?", ":""), nme);
			GDKfree(nme);
			advance(t,base,len);
		}
		if (p->retc > 1 && t< base+len)
			*t++ = ')';
		if( t < base +len) *t++ = ':';
		if( t < base +len) *t++ = '=';
		if( t < base +len) *t++ = ' ';
	}
	*t =0;

	short_end:
	advance(t,base,len);

	// handle the instruction mapping
	snprintf(t,  (len-(t-base)),"%s", (getFunctionId(p)?getFunctionId(p):""));
	advance(t,base,len);

	// handle the arguments, constants should  be shown including their non-default type
	/* display optional multi-assignment list */
	if( t< base + len) *t++ = '(';
	for (i = p->retc; i < p->argc; i++) {
		nme = shortRenderingTerm(mb, stk, p,i);
		snprintf(t,(len-(t-base)), "%s%s", (i!= p->retc? ", ":" "), nme);
		GDKfree(nme);
		advance(t,base,len);
		if (i < p->retc - 1 && t < base+len){
			*t++ = ',';
			*t++ = ' ';
		}
	}
	if( t < base + len) *t++ = ' ';
	if( t < base + len) *t++ = ')';
	*t=0;

	if (t >= s + len)
		throw(MAL,"instruction2str:","instruction too long");
	return base;
}

/* Remote execution of MAL calls for more type/property information to be exchanged */
str
mal2str(MalBlkPtr mb, int first, int last)
{
	str ps = NULL, *txt;
	int i, *len, totlen = 0;

	txt = GDKmalloc(sizeof(str) * mb->stop);
	len = GDKmalloc(sizeof(int) * mb->stop);

	if( txt == NULL || len == NULL){
		GDKfree(txt);
		GDKfree(len);
		return NULL;
	}
	for (i = first; i < last; i++) {
		if( i == 0)
			txt[i] = instruction2str(mb, 0, getInstrPtr(mb, i), LIST_MAL_NAME | LIST_MAL_TYPE  | LIST_MAL_PROPS);
		else
			txt[i] = instruction2str(mb, 0, getInstrPtr(mb, i), LIST_MAL_CALL | LIST_MAL_PROPS | LIST_MAL_REMOTE);
#ifdef _DEBUG_LISTING_
		fprintf(stderr,"%s\n",txt[i]);
#endif

		if ( txt[i])
			totlen += len[i] = (int)strlen(txt[i]);
	}
	ps = GDKmalloc(totlen + mb->stop + 1);
	if( ps == NULL){
		GDKfree(len);
		GDKfree(txt);
		return NULL;
	}

	totlen = 0;
	for (i = first; i < last; i++) {
		if( txt[i]){
			strncpy(ps + totlen, txt[i], len[i]);
			ps[totlen + len[i]] = '\n';
			ps[totlen + len[i] + 1] = 0;
			totlen += len[i] + 1;
			GDKfree(txt[i]);
		}
	}
	GDKfree(len);
	GDKfree(txt);
	return ps;
}

void
printInstruction(stream *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg)
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
	mnstr_printf(fd, "\n");
}

void
fprintInstruction(FILE *fd, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg)
{
	str ps;

	if (fd == 0)
		return;
	ps = instruction2str(mb, stk, p, flg);
	/* ps[strlen(ps)-1] = 0; remove '\n' */
	if ( ps ){
		fprintf(fd, "%s%s", (flg & LIST_MAL_MAPI ? "=" : ""), ps);
		GDKfree(ps);
	}
	fprintf(fd, "\n");
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
	}
}

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
}
