/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

/* Copy string in src to *dstp which has *lenp space available and
 * terminate with a NULL byte.  *dstp and *lenp are adjusted for the
 * used space.  If there is not enough space to copy all of src,
 * return false, otherwise return true.  The resulting string is
 * always NULL-terminated. */
static inline bool
copystring(char **dstp, const char *src, size_t *lenp)
{
	size_t len = *lenp;
	char *dst = *dstp;

	if (src == NULL)
		return true;
	if (len > 0) {
		while (*src && len > 1) {
			*dst++ = *src++;
			len--;
		}
		*dst = 0;
		*dstp = dst;
		*lenp = len;
	}
	return *src == 0;
}

static str
renderTerm(MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int idx, int flg)
{
	char *buf =0;
	char *nme =0;
	int nameused = 0;
	size_t len = 0, maxlen = BUFSIZ;
	ValRecord *val = 0;
	char *cv =0, *c;
	str tpe;
	int showtype = 0, closequote=0;
	int varid = getArg(p,idx);

	buf = GDKzalloc(maxlen);
	if( buf == NULL) {
		addMalException(mb, "renderTerm:Failed to allocate");
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

		if ((cv = VALformat(val)) == NULL) {
			addMalException(mb, "renderTerm:Failed to allocate");
			GDKfree(buf);
			return NULL;
		}
		if (len + strlen(cv) >= maxlen) {
			char *nbuf= GDKrealloc(buf, maxlen =len + strlen(cv) + BUFSIZ);

			if( nbuf == 0){
				GDKfree(buf);
				GDKfree(cv);
				addMalException(mb,"renderTerm:Failed to allocate");
				return NULL;
			}
			buf = nbuf;
		}

		if( strcmp(cv,"nil") == 0){
			strcat(buf+len,cv);
			len += strlen(buf+len);
			GDKfree(cv);
			showtype = showtype || (getBatType(getVarType(mb,varid)) >= TYPE_date && getBatType(getVarType(mb,varid)) != TYPE_str) ||
				((isVarTypedef(mb,varid)) && isVarConstant(mb,varid)) || isaBatType(getVarType(mb,varid));
		} else{
			if ( !isaBatType(getVarType(mb,varid)) && getBatType(getVarType(mb,varid)) >= TYPE_date && getBatType(getVarType(mb,varid)) != TYPE_str ){
				closequote = 1;
				strcat(buf+len,"\"");
				len++;
			}
			if ( isaBatType(getVarType(mb,varid))){
				c = strchr(cv, '>');
				strcat(buf+len,c+1);
				len += strlen(buf+len);
			} else {
				strcat(buf+len,cv);
				len += strlen(buf+len);
			}
			GDKfree(cv);

			if( closequote ){
				strcat(buf+len,"\"");
				len++;
			}
			showtype = showtype || closequote > TYPE_str || ((isVarTypedef(mb,varid) || (flg & (LIST_MAL_REMOTE | LIST_MAL_TYPE))) && isVarConstant(mb,varid)) ||
				(isaBatType(getVarType(mb,varid)) && idx < p->retc);

			if (stk && isaBatType(getVarType(mb,varid)) && stk->stk[varid].val.bval ){
				BAT *d= BBPquickdesc(stk->stk[varid].val.bval, false);
				if( d)
					len += snprintf(buf+len,maxlen-len,"[" BUNFMT "]", BATcount(d));
			}
		}
	}

	// show the type when required or frozen by the user
	// special care should be taken with constants, they may have been casted
	if ((flg & LIST_MAL_TYPE) || (idx < p->retc) || isVarTypedef(mb,varid) || showtype){
		strcat(buf + len,":");
		len++;
		tpe = getTypeName(getVarType(mb, varid));
		len += snprintf(buf+len,maxlen-len,"%s",tpe);
		GDKfree(tpe);
	}

	if( len >= maxlen)
		addMalException(mb,"renderTerm:Value representation too large");
	return buf;
}

/*
It receives the space to store the definition
The MAL profiler dumps some performance data at the
beginning of each line.
*/

str
fcnDefinition(MalBlkPtr mb, InstrPtr p, str t, int flg, str base, size_t len)
{
	int i, j;
	str arg, tpe;

	len -= t - base;
	if (!flg && !copystring(&t, "#", &len))
		return base;
	if( mb->inlineProp && !copystring(&t, "inline ", &len))
		return base;
	if( mb->unsafeProp && !copystring(&t, "unsafe ", &len))
		return base;
	if (!copystring(&t, operatorName(p->token), &len) ||
		!copystring(&t, " ", &len) ||
		!copystring(&t, getModuleId(p) ? getModuleId(p) : "user", &len) ||
		!copystring(&t, ".", &len) ||
		!copystring(&t, getFunctionId(p), &len) ||
		!copystring(&t, "(", &len))
		return base;

	for (i = p->retc; i < p->argc; i++) {
		arg = renderTerm(mb, 0, p, i, (LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_PROPS));
		if (arg && !copystring(&t, arg, &len)) {
			GDKfree(arg);
			return base;
		}
		GDKfree(arg);
		if( i<p->argc-1 && !copystring(&t, ", ", &len))
			return base;
	}

	advance(t,base,len);
	if (p->varargs & VARARGS && !copystring(&t, "...", &len))
		return base;

	if (p->retc == 1) {
		if (!copystring(&t, "):", &len))
			return base;
		tpe = getTypeName(getVarType(mb, getArg(p,0)));
		if (!copystring(&t, tpe, &len)) {
			GDKfree(tpe);
			return base;
		}
		GDKfree(tpe);
		if (p->varargs & VARRETS && !copystring(&t, "...", &len))
			return base;
	} else {
		if (!copystring(&t, ") (", &len))
			return base;
		for (i = 0; i < p->retc; i++) {
			arg = renderTerm(mb, 0, p, i, (LIST_MAL_NAME | LIST_MAL_TYPE | LIST_MAL_PROPS));
			if (arg && !copystring(&t, arg, &len)) {
				GDKfree(arg);
				return base;
			}
			GDKfree(arg);
			if( i<p->retc-1 && !copystring(&t, ", ", &len))
				return base;
		}
		if (p->varargs & VARRETS && !copystring(&t, "...", &len))
			return base;
		if (!copystring(&t, ")", &len))
			return base;
	}

	if (mb->binding[0]) {
		if (!copystring(&t, " address ", &len) ||
			!copystring(&t, mb->binding, &len))
			return base;
	}
	(void) copystring(&t, ";", &len);
	/* add the extra properties for debugging */
	if( flg & LIST_MAL_PROPS){
		char extra[256];
		if (p->token == REMsymbol){
		} else{
			snprintf(extra, 256, "\t#[%d] ("BUNFMT") %s ", getPC(mb,p), getRowCnt(mb,getArg(p,0)), (p->blk? p->blk->binding:""));
			if (!copystring(&t, extra, &len))
				return base;
			for(j =0; j < p->retc; j++){
				snprintf(extra, 256, "%d ", getArg(p,j));
				if (!copystring(&t, extra, &len))
					return base;
			}
			if( p->argc - p->retc > 0){
				if (!copystring(&t, "<- ", &len))
					return base;
			}
			for(; j < p->argc; j++){
				snprintf(extra, 256, "%d ", getArg(p,j));
				if (!copystring(&t, extra, &len))
					return base;
			}
			if( p->typechk == TYPE_UNKNOWN){
				if (!copystring(&t, " type check needed" , &len))
					return base;
			}
		}
	}
	return base;
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

	/* internal symbols */
	case FCNcall:	return "FCNcall";
	case FACcall:     return "FACcall";
	case CMDcall:     return "CMDcall";
	case THRDcall:    return "THRcall";
	case PATcall:     return "PATcall";
	}
	return "";
}

str
instruction2str(MalBlkPtr mb, MalStkPtr stk,  InstrPtr p, int flg)
{
	int i,j;
	str base, t;
	size_t len = 512 + (p->argc * 128);		 /* max realistic line length estimate */
	str arg;

	t = base = GDKmalloc(len);
	if ( base == NULL)
		return NULL;
	if (!flg) {
		*t++ = '#';
		len--;
		if (p->typechk == TYPE_UNKNOWN) {
			*t++ = '!';	/* error */
			len--;
		}
	}
	*t = 0;
	if (p->token == REMsymbol && !( getModuleId(p) && strcmp(getModuleId(p),"querylog") == 0  && getFunctionId(p) && strcmp(getFunctionId(p),"define") == 0)) {
		/* do nothing */
	} else if (p->barrier) {
		if (p->barrier == LEAVEsymbol ||
			p->barrier == REDOsymbol ||
			p->barrier == RETURNsymbol ||
			p->barrier == YIELDsymbol ||
			p->barrier == RAISEsymbol) {
			if (!copystring(&t, "    ", &len))
				return base;
		}
		arg = operatorName(p->barrier);
		if (!copystring(&t, arg, &len) ||
			!copystring(&t, " ", &len))
			return base;
	} else if( functionStart(p) && flg != LIST_MAL_CALL ){
		return fcnDefinition(mb, p, t, flg, base, len + (t - base));
	} else if (!functionExit(p) && flg!=LIST_MAL_CALL) {
		// beautify with tabs
		if (!copystring(&t, "    ", &len))
			return base;
	}
	switch (p->token<0?-p->token:p->token) {
	case FCNcall:
	case FACcall:
	case PATcall:
	case CMDcall:
	case ASSIGNsymbol :
		// is any variable explicit or used
		/* this code was meant to make it easy to detect functions whose
		 * result variable was not used anywhere.
		 * It is not essential
		for (i = 0; i < p->retc; i++)
			if ( !isTmpVar(mb,getArg(p,i)) || isVarUsed(mb, getArg(p, i)))
				break;

		if (i == p->retc)
			break;
		*/

		/* display multi-assignment list */
		if (p->retc > 1 && !copystring(&t, "(", &len))
			return base;

		for (i = 0; i < p->retc; i++) {
			arg= renderTerm(mb, stk, p, i, flg);
			if (arg) {
				if (!copystring(&t, arg, &len)) {
					GDKfree(arg);
					return base;
				}
				GDKfree(arg);
			}
			if (i < p->retc - 1 && !copystring(&t, ", ", &len))
				return base;
		}
		if (p->retc > 1 && !copystring(&t, ")", &len))
			return base;

		if (p->argc > p->retc || getFunctionId(p)) {
			if (!copystring(&t, " := ", &len))
				return base;
		}
		break;
	case ENDsymbol:
		if (!copystring(&t, "end ", &len) ||
			!copystring(&t, getModuleId(getInstrPtr(mb,0)), &len) ||
			!copystring(&t, ".", &len) ||
			!copystring(&t, getFunctionId(getInstrPtr(mb, 0)), &len))
			return base;
		break;
	case COMMANDsymbol:
	case FUNCTIONsymbol:
	case FACTORYsymbol:
	case PATTERNsymbol:
		if (flg & LIST_MAL_VALUE) {
			if (!copystring(&t, operatorName(p->token), &len) ||
				!copystring(&t, " ", &len))
				return base;
		}
		return fcnDefinition(mb, p, t, flg, base, len + (t - base));
	case REMsymbol:
	case NOOPsymbol:
		if (!copystring(&t, "#", &len))
			return base;
		if (getVar(mb, getArg(p, 0))->value.val.sval && getVar(mb, getArg(p, 0))->value.len > 0 &&
			!copystring(&t, getVar(mb, getArg(p, 0))->value.val.sval, &len))
			return base;
		if (!copystring(&t, " ", &len))
			return base;
		break;
	default:
		i = snprintf(t, len, " unknown symbol ?%d? ", p->token);
		if (i < 0 || (size_t) i >= len)
			return base;
		len -= (size_t) i;
		t += i;
		break;
	}

	if (getModuleId(p)) {
		if (!copystring(&t, getModuleId(p), &len) ||
			!copystring(&t, ".", &len))
			return base;
	}
	if (getFunctionId(p)) {
		if (!copystring(&t, getFunctionId(p), &len) ||
			!copystring(&t, "(", &len))
			return base;
	} else if (p->argc > p->retc + 1) {
		if (!copystring(&t, "(", &len))
			return base;
	}
	for (i = p->retc; i < p->argc; i++) {
		arg= renderTerm(mb, stk, p, i, flg);
		if (arg) {
			if (!copystring(&t, arg, &len)) {
				GDKfree(arg);
				return base;
			}
			GDKfree(arg);
		}

		if (i < p->argc -1 && !copystring(&t, ", ", &len))
			return base;
	}
	if (getFunctionId(p) || p->argc > p->retc + 1) {
		if (!copystring(&t, ")", &len))
			return base;
	}
	if (p->token != REMsymbol){
		if (!copystring(&t, ";", &len))
			return base;
	}
	/* add the extra properties for debugging */
	if( flg & LIST_MAL_PROPS){
		char extra[256];
		if (p->token == REMsymbol){
		} else{
			snprintf(extra, 256, "\t#[%d] ("BUNFMT") %s ", p->pc, getRowCnt(mb,getArg(p,0)), (p->blk? p->blk->binding:""));
			if (!copystring(&t, extra, &len))
				return base;
			for(j =0; j < p->retc; j++){
				snprintf(extra, 256, "%d ", getArg(p,j));
				if (!copystring(&t, extra, &len))
					return base;
			}
			if( p->argc - p->retc > 0){
				if (!copystring(&t, "<- ", &len))
					return base;
			}
			for(; j < p->argc; j++){
				snprintf(extra, 256, "%d ", getArg(p,j));
				if (!copystring(&t, extra, &len))
					return base;
			}
			if( p->typechk == TYPE_UNKNOWN){
				if (!copystring(&t, " type check needed" , &len))
					return base;
			}
		}
	}
	if (flg & LIST_MAL_ALGO) {
		const char *algo = MT_thread_getalgorithm();
		if (algo) {
			if (!copystring(&t, " # ", &len))
				return base;
			if (!copystring(&t, algo, &len))
				return base;
		}
	}
	return base;
}

/* the MAL beautifier is meant to simplify correlation of MAL variables and
 * the columns in the underlying database.
 * If the status is set, then we consider the instruction DONE and the result variables
 * should be shown as well.
 */

/* Remote execution of MAL calls for more type/property information to be exchanged */
str
mal2str(MalBlkPtr mb, int first, int last)
{
	str ps = NULL, *txt;
	int i, j;
	size_t *len, totlen = 0;

	txt = GDKmalloc(sizeof(str) * mb->stop);
	len = GDKmalloc(sizeof(size_t) * mb->stop);

	if( txt == NULL || len == NULL){
		addMalException(mb,"mal2str: " MAL_MALLOC_FAIL);
		GDKfree(txt);
		GDKfree(len);
		return NULL;
	}
	for (i = first; i < last; i++) {
		if( i == 0)
			txt[i] = instruction2str(mb, 0, getInstrPtr(mb, i), LIST_MAL_NAME | LIST_MAL_TYPE  | LIST_MAL_PROPS);
		else
			txt[i] = instruction2str(mb, 0, getInstrPtr(mb, i), LIST_MAL_CALL | LIST_MAL_PROPS | LIST_MAL_REMOTE);

		if ( txt[i])
			totlen += len[i] = strlen(txt[i]);
		else {
			addMalException(mb,"mal2str: " MAL_MALLOC_FAIL);
			GDKfree(len);
			for (j = first; j < i; j++)
				GDKfree(txt[j]);
			GDKfree(txt);
			return NULL;
		}
	}
	ps = GDKmalloc(totlen + mb->stop + 1);
	if( ps == NULL){
		addMalException(mb,"mal2str: " MAL_MALLOC_FAIL);
		GDKfree(len);
		for (i = first; i < last; i++)
			GDKfree(txt[i]);
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
	} else {
		mnstr_printf(fd,"#failed instruction2str()");
	}
	mnstr_printf(fd, "\n");
}

void
traceInstruction(component_t comp, MalBlkPtr mb, MalStkPtr stk, InstrPtr p, int flg)
{
	str ps;
	TRC_DEBUG_IF(comp){
		ps = instruction2str(mb, stk, p, flg);
		/* ps[strlen(ps)-1] = 0; remove '\n' */
		if ( ps ){
			TRC_DEBUG_ENDIF(comp, "%s%s\n", (flg & LIST_MAL_MAPI ? "=" : ""), ps);
			GDKfree(ps);
		} else {
			TRC_DEBUG_ENDIF(comp, "Failed instruction2str()\n");
		}
	}
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
	} else mnstr_printf(fd, "printSignature: " MAL_MALLOC_FAIL);
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
			} else {
				mnstr_printf(out,"#failed instruction2str()\n");
			}
		}
		m= m->history;
	}
}
