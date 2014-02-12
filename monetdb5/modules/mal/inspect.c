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
 * author Martin Kersten
 * Inspection
 * This module introduces a series of commands that provide access
 * to information stored within the interpreter data structures.
 * It's primary use is debugging.
 * In all cases, the pseudo BAT operation is returned that
 * should be garbage collected after being used.
 *
 * The main performance drain would be to use a pseudo BAT directly to
 * successively access it components. This can be avoided by first assigning
 * the pseudo BAT to a variable.
 */
#include "monetdb_config.h"
#include "inspect.h"

static void
pseudo(int *ret, BAT *b, str X1,str X2, str X3) {
	char buf[BUFSIZ];
	snprintf(buf,BUFSIZ,"%s_%s_%s", X1,X2,X3);
	if (BBPindex(buf) <= 0)
		BATname(b,buf);
	BATroles(b,X1,X2);
	BATmode(b,TRANSIENT);
	BATfakeCommit(b);
	*ret = b->batCacheid;
	BBPkeepref(*ret);
}

/*
 * Symbol table
 * Mal symbol table and environment analysis.
 *
 * Collect symbol table information in a series of BATs for analysis
 * and display. Note, the elements are aligned using a counter,
 * which makes it susceptable for intermediate updates
 */

str
INSPECTgetAllFunctions(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i;
	oid k = 0;
	BAT *b = BATnew(TYPE_oid, TYPE_str, 256);
	int *ret = (int *) getArgReference(stk,pci,0);

	(void) mb;
	if (b == 0)
		throw(MAL, "inspect.getgetFunctionId", MAL_MALLOC_FAIL );
	BATseqbase(b, k);
	s = cntxt->nspace;
	while (s) {
		for (i = 0; s && i < MAXSCOPE; i++)
			if (s->subscope[i]) {
				for (t = s->subscope[i]; t; t = t->peer) {
					InstrPtr sig = getSignature(t);

					BUNins(b, &k, getFunctionId(sig), FALSE);
					k++;
				}
			}
		s = s->outer;
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","symbol","function");

	return MAL_SUCCEED;
}

str
INSPECTgetAllModules(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i;
	oid k = 0;
	BAT *b = BATnew(TYPE_oid, TYPE_str, 256);
	int *ret = (int *) getArgReference(stk,pci,0);

	(void) mb;
	if (b == 0)
		throw(MAL, "inspect.getmodule", MAL_MALLOC_FAIL);
	BATseqbase(b, k);
	s = cntxt->nspace;
	while (s) {
		for (i = 0; s && i < MAXSCOPE; i++)
			if (s->subscope[i]) {
				for (t = s->subscope[i]; t; t = t->peer) {
					InstrPtr sig = getSignature(t);

					BUNins(b, &k, getModuleId(sig), FALSE);
					k++;
				}
			}
		s = s->outer;
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","symbol","module");

	return MAL_SUCCEED;
}

str
INSPECTgetkind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i;
	oid k = 0;
	BAT *b = BATnew(TYPE_oid, TYPE_str, 256);
	int *ret = (int *) getArgReference(stk,pci,0);

	(void)mb;
	if (b == 0)
		throw(MAL, "inspect.get", MAL_MALLOC_FAIL);
	BATseqbase(b, k);
	s = cntxt->nspace;
	while (s) {
		for (i = 0; s && i < MAXSCOPE; i++)
			if (s->subscope[i]) {
				for (t = s->subscope[i]; t; t = t->peer) {
					InstrPtr sig = getSignature(t);
					str kind = operatorName(sig->token);

					BUNins(b, &k, kind, FALSE);
					k++;
				}
			}
		s = s->outer;
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","symbol","kind");

	return MAL_SUCCEED;
}


str
INSPECTgetAllSignatures(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i;
	oid k = 0;
	BAT *b = BATnew(TYPE_oid, TYPE_str, 256);
	char sig[BLOCK],*a;
	int *ret = (int *) getArgReference(stk,pci,0);

	(void)mb;

	if (b == 0)
		throw(MAL, "inspect.get", MAL_MALLOC_FAIL);
	BATseqbase(b, k);
	s = cntxt->nspace;
	while (s) {
		for (i = 0; s && i < MAXSCOPE; i++)
			if (s->subscope[i]) {
				for (t = s->subscope[i]; t; t = t->peer) {
					fcnDefinition(t->def, getSignature(t), sig, 0,sig,BLOCK);
					a= strstr(sig,"address");
					if(a) *a = 0;
					BUNins(b, &k, strchr(sig, '('), FALSE);
					k++;
				}
			}
		s = s->outer;
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view"," symbol","address");

	return MAL_SUCCEED;
}
str
INSPECTgetAllAddresses(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i;
	oid k = 0;
	BAT *b = BATnew(TYPE_oid, TYPE_str, 256);
	char sig[BLOCK],*a;
	int *ret = (int *) getArgReference(stk,pci,0);

	(void)mb;

	if (b == 0)
		throw(MAL, "inspect.get", MAL_MALLOC_FAIL);
	BATseqbase(b, k);
	s = cntxt->nspace;
	while (s) {
		for (i = 0; s && i < MAXSCOPE; i++)
			if (s->subscope[i]) {
				for (t = s->subscope[i]; t; t = t->peer) {
					fcnDefinition(t->def, getSignature(t), sig, 0,sig,BLOCK);
					a= strstr(sig,"address");
					if( a)
						for( a=a+7; isspace((int) *a); a++)
							;
					BUNins(b, &k, (a? a: "nil"), FALSE);
					k++;
				}
			}
		s = s->outer;
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view"," symbol","address");

	return MAL_SUCCEED;
}

str
INSPECTgetDefinition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	Symbol s;
	BAT *b;
	(void)mb;

	s = findSymbol(cntxt->nspace, putName(*mod,strlen(*mod)), putName(*fcn, strlen(*fcn)));
	if (s == 0)
		throw(MAL, "inspect.getDefinition", RUNTIME_SIGNATURE_MISSING);

	b = BATnew(TYPE_void, TYPE_str, 256);
	if (b == 0)
		throw(MAL, "inspect.getDefinition", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	while (s) {
		int i;
		str ps;

		for (i = 0; i < s->def->stop; i++) {
			ps = instruction2str(s->def,0, getInstrPtr(s->def, i), 0);
			BUNappend(b, ps + 1, FALSE);
			GDKfree(ps);
		}
		s = s->peer;
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","fcn","stmt");

	return MAL_SUCCEED;
}

str
INSPECTgetSignature(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	Symbol s;
	str ps, tail;
	BAT *b;
	(void) mb;

	s = findSymbol(cntxt->nspace, getName(*mod,strlen(*mod)), putName(*fcn, strlen(*fcn)));
	if (s == 0)
		throw(MAL, "inspect.getSignature", RUNTIME_SIGNATURE_MISSING);
	b = BATnew(TYPE_void, TYPE_str, 12);
	if (b == 0)
		throw(MAL, "inspect.getSignature", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	while (s != NULL) {
		if (idcmp(s->name, *fcn) == 0) {
			char *c, *w;

			ps = instruction2str(s->def, 0, getSignature(s), 0);
			c = strchr(ps, '(');
			if (c == 0)
				continue;
			tail= strstr(c,"address");
			if( tail)
				*tail = 0;
			if (tail && (w=strchr(tail, ';')) )
				*w = 0;
			BUNappend(b, c, FALSE);
			GDKfree(ps);
		}
		s = s->peer;
	}

	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","input","result");
	return MAL_SUCCEED;
}

str
INSPECTgetAddress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	Symbol s;
	str ps, tail;
	BAT *b;
	(void) mb;

	s = findSymbol(cntxt->nspace, getName(*mod,strlen(*mod)), putName(*fcn, strlen(*fcn)));
	if (s == 0)
		throw(MAL, "inspect.getAddress", RUNTIME_SIGNATURE_MISSING);
	b = BATnew(TYPE_void, TYPE_str, 12);
	if (b == 0)
		throw(MAL, "inspect.getAddress", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	while (s != NULL) {
		if (idcmp(s->name, *fcn) == 0) {
			char *c,*w;

			ps = instruction2str(s->def, 0, getSignature(s), 0);
			c = strchr(ps, '(');
			if (c == 0)
				continue;
			tail= strstr(c,"address");
			if( tail){
				*tail = 0;
				for( tail=tail+7; isspace((int) *tail); tail++)  ;
			}
			if (tail && (w=strchr(tail, ';')) )
				*w = 0;
			BUNappend(b, (tail? tail: "nil"), FALSE);
			GDKfree(ps);
		}
		s = s->peer;
	}

	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","input","result");
	return MAL_SUCCEED;
}
str
INSPECTgetComment(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	Symbol s;
	BAT *b;
	(void) mb;

	s = findSymbol(cntxt->nspace, getName(*mod,strlen(*mod)), putName(*fcn, strlen(*fcn)));
	if (s == 0)
		throw(MAL, "inspect.getComment", RUNTIME_SIGNATURE_MISSING);
	b = BATnew(TYPE_void, TYPE_str, 12);
	if (b == 0)
		throw(MAL, "inspect.getComment", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	while (s != NULL) {
		if (idcmp(s->name, *fcn) == 0) {
			BUNappend(b, s->def->help, FALSE);
		}
		s = s->peer;
	}

	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","input","result");
	return MAL_SUCCEED;
}

str
INSPECTgetSource(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = (str*) getArgReference(stk,pci,0);
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	Symbol s;
	char *buf;
	size_t len,lim;
	(void) mb;

	s = findSymbol( cntxt->nspace, getName(*mod,strlen(*mod)), putName(*fcn, strlen(*fcn)));
	if (s == 0)
		throw(MAL, "inspect.getSource", RUNTIME_SIGNATURE_MISSING);

	buf= (char*) GDKmalloc(BUFSIZ);
	if ( buf == NULL)
		throw(MAL, "inspect.getSource", MAL_MALLOC_FAIL);
	snprintf(buf,BUFSIZ,"%s.%s",*mod,*fcn);
	buf[0]=0;
	len= 0;
	lim= BUFSIZ;

	while (s) {
		int i;
		str ps;

		for (i = 0; i < s->def->stop; i++) {
			ps = instruction2str(s->def, 0, getInstrPtr(s->def, i), LIST_MAL_STMT);
			if( strlen(ps) >= lim-len){
				/* expand the buffer */
				char *bn;
				bn= GDKmalloc(lim+BUFSIZ);
				if ( bn == NULL) {
					GDKfree(ps);
					throw(MAL, "inspect.getSource", MAL_MALLOC_FAIL);
				}
				strcpy(bn,buf);
				GDKfree(buf);
				buf=bn;
				lim+= BUFSIZ;
			}
			strcat(buf+len,ps);
			len+= strlen(ps);
			buf[len++]='\n';
			buf[len]=0;
			GDKfree(ps);
		}
		s = s->peer;
	}
	*ret= buf;
	return MAL_SUCCEED;
}

str
INSPECTsymbolType(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int*) getArgReference(stk,pci,0);
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	Symbol s;
	BAT *b;
	(void) mb;

	s = findSymbol( cntxt->nspace, getName(*mod,strlen(*mod)), putName(*fcn, strlen(*fcn)));
	if (s == 0)
		throw(MAL, "inspect.getSignature", RUNTIME_SIGNATURE_MISSING);
	b = BATnew(TYPE_str, TYPE_str, 256);
	if (b == 0)
		throw(MAL, "inspect.getType", MAL_MALLOC_FAIL);
	while (s != NULL) {
		if (idcmp(s->name, *fcn) == 0) {
			str t = getTypeName(getDestType(s->def, getSignature(s)));

			BUNins(b, s->name, t, FALSE);
			GDKfree(t);
		}
		s = s->peer;
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","fcn","type");
	return MAL_SUCCEED;
}

str
INSPECTatom_names(int *ret)
{
	int i;
	BAT *b = BATnew(TYPE_void, TYPE_str, 256);

	if (b == 0)
		throw(MAL, "inspect.getAtomNames", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	for (i = 0; i < GDKatomcnt; i++)
		BUNappend(b, ATOMname(i), FALSE);

	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","atom","name");

	return MAL_SUCCEED;
}
str
INSPECTgetEnvironment(int *ret, int *ret2)
{
	BAT *b, *bn;

	b= VIEWhead(BATmirror(GDKkey));
	if (b == 0)
		throw(MAL, "inspect.getEnvironment", MAL_MALLOC_FAIL);
	bn= VIEWhead(BATmirror(GDKval));
	if (bn == 0){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "inspect.getEnvironment", MAL_MALLOC_FAIL);
	}
	b = BATmirror(b);
	BATseqbase(b,0);
	bn = BATmirror(bn);
	BATseqbase(bn,0);

	BBPkeepref(*ret = b->batCacheid);
	BBPkeepref(*ret2 = bn->batCacheid);
	return MAL_SUCCEED;
}

str
INSPECTgetEnvironmentKey(str *ret, str *key)
{
	str s = GDKgetenv(*key);
	if (s == 0)
		throw(MAL, "inspect.getEnvironment", "environment variable '%s' not found", *key);
	*ret = GDKstrdup(s);
	return MAL_SUCCEED;
}

str
INSPECTatom_sup_names(int *ret)
{
	int i, k;
	BAT *b = BATnew(TYPE_oid, TYPE_str, 256);

	if (b == 0)
		throw(MAL, "inspect.getAtomSuper", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	for (i = 0; i < GDKatomcnt; i++) {
		for (k = BATatoms[i].storage; k > TYPE_str; k = BATatoms[k].storage)
			;
		BUNappend(b, ATOMname(k), FALSE);
	}

	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","atom","sup_name");

	return MAL_SUCCEED;
}

str
INSPECTatom_sizes(int *ret)
{
	int i;
	int s;
	BAT *b = BATnew(TYPE_void, TYPE_int, 256);

	if (b == 0)
		throw(MAL, "inspect.getAtomSizes", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	for (i = 0; i < GDKatomcnt; i++) {
		s = ATOMsize(i);
		BUNappend(b, &s, FALSE);
	}

	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"view","atom","size");

	return MAL_SUCCEED;
}

/* calculate to trimmed storage space */
static lng
INSPECTcalcSize(MalBlkPtr mb){
	lng size,args=0,i;
	InstrPtr p;

	for(i=0;i<mb->stop; i++){
		p= getInstrPtr(mb,i);
		args += (p->argc-1)* sizeof(*p->argv);
	}
	size = (sizeof(InstrRecord) +sizeof(InstrPtr)) * mb->stop;
	size += (sizeof(VarRecord)+ sizeof(InstrPtr)) * mb->vtop;
	size += args;
	return size;
}

str
INSPECTgetSize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	lng *ret = (lng*) getArgReference(stk,p,0);


	*ret= INSPECTcalcSize(mb);
	(void) cntxt;
	(void) mb;
	return MAL_SUCCEED;
}

str
INSPECTgetFunctionSize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *ret = (lng*) getArgReference(stk,pci,0);
	str *mod = (str*) getArgReference(stk,pci,1);
	str *fcn = (str*) getArgReference(stk,pci,2);
	Symbol s;
	(void) mb;

	s = findSymbol(cntxt->nspace, getName(*mod,strlen(*mod)), putName(*fcn, strlen(*fcn)));
	if (s == 0)
		throw(MAL, "inspect.getSize", RUNTIME_SIGNATURE_MISSING);
	*ret= INSPECTcalcSize(s->def);
	return MAL_SUCCEED;
}
/*
 * Display routines
 */
str
INSPECTshowFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) p;
	printFunction(cntxt->fdout, mb, stk, LIST_INPUT);
	return MAL_SUCCEED;
}

str
INSPECTshowFunction3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str modnme = getArgName(mb, p, 1);
	str fcnnme = getArgName(mb, p, 2);
	Symbol s = NULL;

	s = findSymbol(cntxt->nspace,getName(modnme,strlen(modnme)), putName(fcnnme, strlen(fcnnme)));

	if (s == NULL){
		char buf[BUFSIZ];
		snprintf(buf,BUFSIZ,"%s.%s", modnme, fcnnme);
		throw(MAL, "inspect.showSource",RUNTIME_SIGNATURE_MISSING "%s",buf);
	} else
		printFunction(cntxt->fdout, s->def, stk, LIST_INPUT);
	return MAL_SUCCEED;
}

str
INSPECTtypename(str *ret, int *tpe)
{
	*ret = getTypeName(*tpe);
	return MAL_SUCCEED;
}
str
INSPECTtypeIndex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret;

	(void) cntxt;
	if( pci->retc== 2){
		ret = (int *) getArgReference(stk, pci, 0);
		*ret = getHeadType(getArgType(mb, pci, 2));
		ret = (int *) getArgReference(stk, pci, 1);
		*ret = getTailType(getArgType(mb, pci, 2));
	}else {
		ret = (int *) getArgReference(stk, pci, 0);
		*ret = getTailType(getArgType(mb, pci, 1));
	}
	return MAL_SUCCEED;
}
str
INSPECTequalType(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *ret;
	(void) stk;
	(void) cntxt;
	ret = (bit *) getArgReference(stk, pci, 0);
	*ret = getArgType(mb,pci,1)== getArgType(mb,pci,2);
	return MAL_SUCCEED;
}

str
INSPECTtypeName(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *hn, *tn =0;

	hn = (str *) getArgReference(stk, pci, 0);

	(void) cntxt;
	if( pci->retc== 2){
		tn = (str *) getArgReference(stk, pci, 1);
		*hn = getTypeName(getHeadType(getArgType(mb, pci, 2)));
		*tn = getTypeName(getTailType(getArgType(mb, pci, 2)));
	} else if (isaBatType(getArgType(mb,pci,1) ) ){
		int *bid= (int*) getArgReference(stk,pci,1);
		BAT *b;
		if ((b = BATdescriptor(*bid)) ) {
			*hn = getTypeName(newBatType((b->htype==TYPE_void?TYPE_oid:b->htype),b->ttype));
			BBPunfix(b->batCacheid);
		} else
			*hn = getTypeName(getArgType(mb, pci, 1));
	} else
		*hn = getTypeName(getArgType(mb, pci, 1));
	return MAL_SUCCEED;
}


