/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "gdk.h"
#include <time.h>
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_interpreter.h"
#include "mal_listing.h"
#include "mal_namespace.h"

static int
pseudo(bat *ret, BAT *b, str X1,str X2, str X3) {
	char buf[BUFSIZ];
	snprintf(buf,BUFSIZ,"%s_%s_%s", X1,X2,X3);
	if (BBPindex(buf) <= 0 && BBPrename(b->batCacheid, buf) != 0)
		return -1;
	if (BATroles(b,X2) != GDK_SUCCEED)
		return -1;
	*ret = b->batCacheid;
	BBPkeepref(*ret);
	return 0;
}

/*
 * Symbol table
 * Mal symbol table and environment analysis.
 *
 * Collect symbol table information in a series of BATs for analysis
 * and display. Note, the elements are aligned using a counter,
 * which makes it susceptable for intermediate updates
 */

static str
INSPECTgetAllFunctions(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i, j;
	Module* moduleList;
	int length;
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);
	bat *ret = getArgReference_bat(stk,pci,0);

	(void) mb;
	if (b == 0)
		throw(MAL, "inspect.getgetFunctionId", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	getModuleList(&moduleList, &length);
	if (moduleList == NULL)
		goto bailout;
	for(j = -1; j < length; j++) {
		s = j < 0 ? cntxt->usermodule : moduleList[j];
		for (i = 0; s && i < MAXSCOPE; i++) {
			if (s->space[i]) {
				for (t = s->space[i]; t; t = t->peer) {
					InstrPtr sig = getSignature(t);
					if (BUNappend(b, getFunctionId(sig), false) != GDK_SUCCEED)
						goto bailout;
				}
			}
		}
	}
	if (pseudo(ret,b,"view","symbol","function"))
		goto bailout;
	freeModuleList(moduleList);

	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	freeModuleList(moduleList);
	throw(MAL, "inspect.getgetFunctionId", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
INSPECTgetAllModules(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i, j;
	Module* moduleList;
	int length;
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);
	bat *ret = getArgReference_bat(stk,pci,0);

	(void) mb;
	if (b == 0)
		throw(MAL, "inspect.getmodule", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	getModuleList(&moduleList, &length);
	if (moduleList == NULL)
		goto bailout;
	for(j = -1; j < length; j++) {
		s = j < 0 ? cntxt->usermodule : moduleList[j];
		for (i = 0; s && i < MAXSCOPE; i++) {
			if (s->space[i]) {
				for (t = s->space[i]; t; t = t->peer) {
					InstrPtr sig = getSignature(t);

					if (BUNappend(b, getModuleId(sig), false) != GDK_SUCCEED)
						goto bailout;
				}
			}
		}
	}
	if (pseudo(ret,b,"view","symbol","module"))
		goto bailout;
	freeModuleList(moduleList);

	return MAL_SUCCEED;
  bailout:
	freeModuleList(moduleList);
	BBPreclaim(b);
	throw(MAL, "inspect.getmodule", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
INSPECTgetkind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i, j;
	Module* moduleList;
	int length;
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);
	bat *ret = getArgReference_bat(stk,pci,0);

	(void)mb;
	if (b == 0)
		throw(MAL, "inspect.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	getModuleList(&moduleList, &length);
	if (moduleList == NULL)
		goto bailout;
	for(j = -1; j < length; j++) {
		s = j < 0 ? cntxt->usermodule : moduleList[j];
		for (i = 0; s && i < MAXSCOPE; i++) {
			if (s->space[i]) {
				for (t = s->space[i]; t; t = t->peer) {
					InstrPtr sig = getSignature(t);
					str kind = operatorName(sig->token);
					if (BUNappend(b, kind, false) != GDK_SUCCEED)
						goto bailout;
				}
			}
		}
	}
	if (pseudo(ret,b,"view","symbol","kind"))
		goto bailout;
	freeModuleList(moduleList);

	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	freeModuleList(moduleList);
	throw(MAL, "inspect.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
INSPECTgetAllSignatures(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i, j;
	Module* moduleList;
	int length;
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);
	char sig[BLOCK],*a;
	bat *ret = getArgReference_bat(stk,pci,0);

	(void)mb;
	if (b == 0)
		throw(MAL, "inspect.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	getModuleList(&moduleList, &length);
	if (moduleList == NULL)
		goto bailout;
	for(j = -1; j < length; j++) {
		s = j < 0 ? cntxt->usermodule : moduleList[j];
		for (i = 0; s && i < MAXSCOPE; i++)
			if (s->space[i]) {
				for (t = s->space[i]; t; t = t->peer) {
					fcnDefinition(t->def, getSignature(t), sig, 0,sig,BLOCK);
					a= strstr(sig,"address");
					if(a) *a = 0;
					if (BUNappend(b, (a = strchr(sig, '(')) ? a : "", false) != GDK_SUCCEED)
						goto bailout;
				}
			}
	}
	if (pseudo(ret,b,"view"," symbol","address"))
		goto bailout;
	freeModuleList(moduleList);

	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	freeModuleList(moduleList);
	throw(MAL, "inspect.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

#if 0
static str
INSPECTgetAllAddresses(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Module s;
	Symbol t;
	int i, j;
	Module* moduleList;
	int length;
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);
	char sig[BLOCK],*a;
	bat *ret = getArgReference_bat(stk,pci,0);

	(void)mb;

	if (b == 0)
		throw(MAL, "inspect.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	getModuleList(&moduleList, &length);
	if (moduleList == NULL)
		goto bailout;
	for(j = -1; j < length; j++) {
		s = j < 0 ? cntxt->usermodule : moduleList[j];
		for (i = 0; s && i < MAXSCOPE; i++)
			if (s->space[i]) {
				for (t = s->space[i]; t; t = t->peer) {
					fcnDefinition(t->def, getSignature(t), sig, 0,sig,BLOCK);
					a= strstr(sig,"address");
					if( a)
						for( a=a+7; isspace((unsigned char) *a); a++)
							;
					if (BUNappend(b, (a? a: "nil"), false) != GDK_SUCCEED)
						goto bailout;
				}
			}
	}
	if (pseudo(ret,b,"view"," symbol","address"))
		goto bailout;
	freeModuleList(moduleList);

	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	freeModuleList(moduleList);
	throw(MAL, "inspect.get", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}
#endif

static str
INSPECTgetDefinition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	Symbol s;
	BAT *b;
	(void)mb;

	s = findSymbol(cntxt->usermodule, putName(*mod), putName(*fcn));
	if (s == 0)
		throw(MAL, "inspect.getDefinition", RUNTIME_SIGNATURE_MISSING);

	b = COLnew(0, TYPE_str, 256, TRANSIENT);
	if (b == 0)
		throw(MAL, "inspect.getDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	while (s) {
		int i;
		str ps;

		for (i = 0; i < s->def->stop; i++) {
			if((ps = instruction2str(s->def,0, getInstrPtr(s->def, i), 0)) == NULL)
				goto bailout;
			if (BUNappend(b, ps + 1, false) != GDK_SUCCEED) {
				GDKfree(ps);
				goto bailout;
			}
			GDKfree(ps);
		}
		s = s->peer;
	}
	if (pseudo(ret,b,"view","fcn","stmt"))
		goto bailout;

	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	throw(MAL, "inspect.getDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
INSPECTgetExistence(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;

	bit *ret = getArgReference_bit(stk,pci,0);
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);

	Symbol s = findSymbol(cntxt->usermodule, getName(*mod), putName(*fcn));

	*ret = (s != NULL);

	return MAL_SUCCEED;
}

static str
INSPECTgetSignature(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	Symbol s;
	str ps, tail;
	BAT *b;
	(void) mb;

	s = findSymbol(cntxt->usermodule, getName(*mod), putName(*fcn));
	if (s == 0)
		throw(MAL, "inspect.getSignature", RUNTIME_SIGNATURE_MISSING);
	b = COLnew(0, TYPE_str, 12, TRANSIENT);
	if (b == 0)
		throw(MAL, "inspect.getSignature", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	while (s != NULL) {
		if (idcmp(s->name, *fcn) == 0) {
			InstrPtr p = getSignature(s);
			char *c, *w;

			ps = instruction2str(s->def, 0, p, 0);
			if (ps == 0) {
				continue;
			}
			c = strchr(ps, '(');
			if (c == 0) {
				GDKfree(ps);
				continue;
			}
			tail= strstr(c,"address");
			if( tail)
				*tail = 0;
			if (tail && (w=strchr(tail, ';')) )
				*w = 0;
			if (BUNappend(b, c, false) != GDK_SUCCEED) {
				GDKfree(ps);
				goto bailout;
			}
			GDKfree(ps);
		}
		s = s->peer;
	}

	if (pseudo(ret,b,"view","input","result"))
		goto bailout;
	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	throw(MAL, "inspect.getSignature", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
INSPECTgetComment(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	Symbol s;
	BAT *b;
	(void) mb;

	s = findSymbol(cntxt->usermodule, getName(*mod), putName(*fcn));
	if (s == 0)
		throw(MAL, "inspect.getComment", RUNTIME_SIGNATURE_MISSING);
	b = COLnew(0, TYPE_str, 12, TRANSIENT);
	if (b == 0)
		throw(MAL, "inspect.getComment", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	while (s != NULL) {
		if (idcmp(s->name, *fcn) == 0 &&
			BUNappend(b, s->def->help, false) != GDK_SUCCEED)
			goto bailout;
		s = s->peer;
	}

	if (pseudo(ret,b,"view","input","result"))
		goto bailout;
	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	throw(MAL, "inspect.getComment", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
INSPECTgetSource(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = getArgReference_str(stk,pci,0);
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	Symbol s;
	char *buf;
	size_t len,lim;
	(void) mb;

	s = findSymbol( cntxt->usermodule, getName(*mod), putName(*fcn));
	if (s == 0)
		throw(MAL, "inspect.getSource", RUNTIME_SIGNATURE_MISSING);

	buf= (char*) GDKmalloc(BUFSIZ);
	if ( buf == NULL)
		throw(MAL, "inspect.getSource", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	snprintf(buf,BUFSIZ,"%s.%s",*mod,*fcn);
	buf[0]=0;
	len= 0;
	lim= BUFSIZ;

	while (s) {
		int i;
		str ps;

		for (i = 0; i < s->def->stop; i++) {
			if((ps = instruction2str(s->def, 0, getInstrPtr(s->def, i), LIST_MAL_NAME )) == NULL) {
				GDKfree(buf);
				throw(MAL, "inspect.getSource", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			if( strlen(ps) >= lim-len){
				/* expand the buffer */
				char *bn;
				bn= GDKrealloc(buf, lim+BUFSIZ);
				if ( bn == NULL) {
					GDKfree(ps);
					GDKfree(buf);
					throw(MAL, "inspect.getSource", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
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

static str
INSPECTatom_names(bat *ret)
{
	int i;
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);

	if (b == 0)
		throw(MAL, "inspect.getAtomNames", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < GDKatomcnt; i++)
		if (BUNappend(b, ATOMname(i), false) != GDK_SUCCEED)
			goto bailout;

	if (pseudo(ret,b,"view","atom","name"))
		goto bailout;

	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	throw(MAL, "inspect.getAtomNames", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
INSPECTgetEnvironment(bat *ret, bat *ret2)
{
	BAT *k, *v;

	if (GDKcopyenv(&k, &v, false) != GDK_SUCCEED)
		throw(MAL, "inspect.getEnvironment", GDK_EXCEPTION);

	BBPkeepref(*ret = k->batCacheid);
	BBPkeepref(*ret2 = v->batCacheid);
	return MAL_SUCCEED;
}

static str
INSPECTgetEnvironmentKey(str *ret, str *key)
{
	const char *s;
	*ret = 0;

	s= GDKgetenv(*key);
	if (s == 0)
		s= getenv(*key);
	if (s == 0)
		throw(MAL, "inspect.getEnvironment", "environment variable '%s' not found", *key);
	*ret = GDKstrdup(s);
	if (*ret == NULL)
		throw(MAL, "inspect.getEnvironment", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
INSPECTatom_sup_names(bat *ret)
{
	int i, k;
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);

	if (b == 0)
		throw(MAL, "inspect.getAtomSuper", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < GDKatomcnt; i++) {
		for (k = ATOMstorage(i); k > TYPE_str; k = ATOMstorage(k))
			;
		if (BUNappend(b, ATOMname(k), false) != GDK_SUCCEED)
			goto bailout;
	}

	if (pseudo(ret,b,"view","atom","sup_name"))
		goto bailout;

	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	throw(MAL, "inspect.getAtomSuper", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
INSPECTatom_sizes(bat *ret)
{
	int i;
	int s;
	BAT *b = COLnew(0, TYPE_int, 256, TRANSIENT);

	if (b == 0)
		throw(MAL, "inspect.getAtomSizes", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < GDKatomcnt; i++) {
		s = ATOMsize(i);
		if (BUNappend(b, &s, false) != GDK_SUCCEED)
			goto bailout;
	}

	if (pseudo(ret,b,"view","atom","size"))
		goto bailout;

	return MAL_SUCCEED;
  bailout:
	BBPreclaim(b);
	throw(MAL, "inspect.getAtomSizes", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
	size = (offsetof(InstrRecord, argv) +sizeof(InstrPtr)) * mb->stop;
	size += sizeof(VarRecord) * mb->vtop;
	size += args;
	return size;
}

static str
INSPECTgetSize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	lng *ret = getArgReference_lng(stk,p,0);


	*ret= INSPECTcalcSize(mb);
	(void) cntxt;
	(void) mb;
	return MAL_SUCCEED;
}

static str
INSPECTgetFunctionSize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *ret = getArgReference_lng(stk,pci,0);
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	Symbol s;
	(void) mb;

	s = findSymbol(cntxt->usermodule, getName(*mod), putName(*fcn));
	if (s == 0)
		throw(MAL, "inspect.getSize", RUNTIME_SIGNATURE_MISSING);
	*ret= INSPECTcalcSize(s->def);
	return MAL_SUCCEED;
}
/*
 * Display routines
 */

#if 0
static str
INSPECTshowFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) p;
	printFunction(cntxt->fdout, mb, stk, LIST_INPUT);
	return MAL_SUCCEED;
}

static str
INSPECTshowFunction3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str modnme = getArgName(mb, p, 1);
	str fcnnme = getArgName(mb, p, 2);
	Symbol s = NULL;

	s = findSymbol(cntxt->usermodule,getName(modnme), putName(fcnnme));

	if (s == NULL){
		char buf[BUFSIZ];
		snprintf(buf,BUFSIZ,"%s.%s", modnme, fcnnme);
		throw(MAL, "inspect.showSource",RUNTIME_SIGNATURE_MISSING "%s",buf);
	} else
		printFunction(cntxt->fdout, s->def, stk, LIST_INPUT);
	return MAL_SUCCEED;
}
#endif

static str
INSPECTequalType(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *ret;
	(void) stk;
	(void) cntxt;
	ret = getArgReference_bit(stk, pci, 0);
	*ret = getArgType(mb,pci,1)== getArgType(mb,pci,2);
	return MAL_SUCCEED;
}

static str
INSPECTtypeName(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *hn, *tn =0;

	hn = getArgReference_str(stk, pci, 0);

	(void) cntxt;
	if( pci->retc== 2){
		tn = getArgReference_str(stk, pci, 1);
		*hn = getTypeName(TYPE_oid);
		*tn = getTypeName(getBatType(getArgType(mb, pci, 2)));
	} else if (isaBatType(getArgType(mb,pci,1) ) ){
		bat *bid= getArgReference_bat(stk,pci,1);
		BAT *b;
		if ((b = BATdescriptor(*bid)) ) {
			*hn = getTypeName(newBatType(b->ttype));
			BBPunfix(b->batCacheid);
		} else
			*hn = getTypeName(getArgType(mb, pci, 1));
	} else
		*hn = getTypeName(getArgType(mb, pci, 1));
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func inspect_init_funcs[] = {
 pattern("inspect", "getDefinition", INSPECTgetDefinition, false, "Returns a string representation of a specific function.", args(1,3, batarg("",str),arg("mod",str),arg("fcn",str))),
 pattern("inspect", "getExistence", INSPECTgetExistence, false, "Returns a boolean indicating existence of a definition of a specific function.", args(1,3, arg("",bit),arg("mod",str),arg("fcn",str))),
 pattern("inspect", "getSignature", INSPECTgetSignature, false, "Returns the function signature(s).", args(1,3, batarg("",str),arg("mod",str),arg("fcn",str))),
 pattern("inspect", "getComment", INSPECTgetComment, false, "Returns the function help information.", args(1,3, batarg("",str),arg("mod",str),arg("fcn",str))),
 pattern("inspect", "getSource", INSPECTgetSource, false, "Return the original input for a function.", args(1,3, arg("",str),arg("mod",str),arg("fcn",str))),
 pattern("inspect", "getKind", INSPECTgetkind, false, "Obtain the instruction kind.", args(1,1, batarg("",str))),
 pattern("inspect", "getModule", INSPECTgetAllModules, false, "Obtain the function name.", args(1,1, batarg("",str))),
 pattern("inspect", "getFunction", INSPECTgetAllFunctions, false, "Obtain the function name.", args(1,1, batarg("",str))),
 pattern("inspect", "getSignatures", INSPECTgetAllSignatures, false, "Obtain the function signatures.", args(1,1, batarg("",str))),
 pattern("inspect", "getSize", INSPECTgetSize, false, "Return the storage size for the current function (in bytes).", args(1,1, arg("",lng))),
 pattern("inspect", "getSize", INSPECTgetFunctionSize, false, "Return the storage size for a function (in bytes).", args(1,3, arg("",lng),arg("mod",str),arg("fcn",str))),
 pattern("inspect", "getType", INSPECTtypeName, false, "Return the concrete type of a variable (expression).", args(1,2, arg("",str),argany("v",1))),
 pattern("inspect", "equalType", INSPECTequalType, false, "Return true if both operands are of the same type", args(1,3, arg("",bit),argany("l",0),argany("r",0))),
 command("inspect", "getAtomNames", INSPECTatom_names, false, "Collect a BAT with the atom names.", args(1,1, batarg("",str))),
 command("inspect", "getAtomSuper", INSPECTatom_sup_names, false, "Collect a BAT with the atom names.", args(1,1, batarg("",str))),
 command("inspect", "getAtomSizes", INSPECTatom_sizes, false, "Collect a BAT with the atom sizes.", args(1,1, batarg("",int))),
 command("inspect", "getEnvironment", INSPECTgetEnvironment, false, "Collect the environment variables.", args(2,2, batarg("k",str),batarg("v",str))),
 command("inspect", "getEnvironment", INSPECTgetEnvironmentKey, false, "Get the value of an environemnt variable", args(1,2, arg("",str),arg("k",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_inspect_mal)
{ mal_module("inspect", NULL, inspect_init_funcs); }
