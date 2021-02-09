/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 *  Martin Kersten
 * Language Extensions
 * Iterators over scalar ranges are often needed, also at the MAL level.
 * The barrier and control primitives are sufficient to mimic them directly.
 *
 * The modules located in the kernel directory should not
 * rely on the MAL datastructures. That's why we have to deal with
 * some bat operations here and delegate the signature to the
 * proper module upon loading.
 *
 * Running a script is typically used to initialize a context.
 * Therefore we need access to the runtime context.
 * For the call variants we have
 * to determine an easy way to exchange the parameter/return values.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_module.h"
#include "mal_session.h"
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_dataflow.h"

static str
CMDraise(str *ret, str *msg)
{
	str res;
	*ret = GDKstrdup(*msg);
	if( *ret == NULL)
		throw(MAL, "mal.raise", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	res = GDKstrdup(*msg);
	if( res == NULL)
		throw(MAL, "mal.raise", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return res;
}

static str
MALassertBit(void *ret, bit *val, str *msg){
	(void) ret;
	if( *val == 0 || is_bit_nil(*val))
		throw(MAL, "mal.assert", "%s", *msg);
	return MAL_SUCCEED;
}
static str
MALassertInt(void *ret, int *val, str *msg){
	(void) ret;
	if( *val == 0 || is_int_nil(*val))
		throw(MAL, "mal.assert", "%s", *msg);
	return MAL_SUCCEED;
}
static str
MALassertLng(void *ret, lng *val, str *msg){
	(void) ret;
	if( *val == 0 || is_lng_nil(*val))
		throw(MAL, "mal.assert", "%s", *msg);
	return MAL_SUCCEED;
}
#ifdef HAVE_HGE
static str
MALassertHge(void *ret, hge *val, str *msg){
	(void) ret;
	if( *val == 0 || is_hge_nil(*val))
		throw(MAL, "mal.assert", "%s", *msg);
	return MAL_SUCCEED;
}
#endif
static str
MALassertSht(void *ret, sht *val, str *msg){
	(void) ret;
	if( *val == 0 || is_sht_nil(*val))
		throw(MAL, "mal.assert", "%s", *msg);
	return MAL_SUCCEED;
}
static str
MALassertOid(void *ret, oid *val, str *msg){
	(void) ret;
	if( is_oid_nil(*val))
		throw(MAL, "mal.assert", "%s", *msg);
	return MAL_SUCCEED;
}
static str
MALassertStr(void *ret, str *val, str *msg){
	(void) ret;
	if( *val == str_nil)
		throw(MAL, "mal.assert", "%s", *msg);
	return MAL_SUCCEED;
}

static str
MALassertTriple(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) p;
	throw(MAL, "mal.assert", SQLSTATE(0A000) PROGRAM_NYI);
}

/*
 * Printing
 * The print commands are implemented as single instruction rules,
 * because they need access to the calling context.
 * At a later stage we can look into the issues related to
 * parsing the format string as part of the initialization phase.
 * The old method in V4 essentially causes a lot of overhead
 * because you have to prepare for the worst (e.g. mismatch format
 * identifier and argument value)
 *
 * Input redirectionrs
 */
static str
CMDcallString(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *s;

	(void) mb;		/* fool compiler */
	s = getArgReference_str(stk, pci, 1);
	if (strlen(*s) == 0)
		return MAL_SUCCEED;
	return callString(cntxt, *s, FALSE);
}

static str
CMDcallFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str mod = *getArgReference_str(stk,pci,1);
	str fcn = *getArgReference_str(stk,pci,2);
	char buf[BUFSIZ];

	(void) mb;		/* fool compiler */
	if (strlen(mod) == 0 || strlen(fcn) ==0)
		return MAL_SUCCEED;
	// lazy implementation of the call
	snprintf(buf,BUFSIZ,"%s.%s();",mod,fcn);
	return callString(cntxt, buf, FALSE);
}

static str
MALstartDataflow( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *ret = getArgReference_bit(stk,pci,0);
	int pc = getPC(mb,pci);

	if ( pc <0 || pc > pci->jump)
		throw(MAL,"language.dataflow","Illegal statement range");
	*ret = 0;	/* continue at end of block */
	return runMALdataflow(cntxt, mb, pc, pci->jump, stk);
}

/*
 * Garbage collection over variables can be postponed by grouping
 * all dependent ones in a single sink() instruction.
 */
static str
MALgarbagesink( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

static str
MALpass( Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	return MAL_SUCCEED;
}

static str
CMDregisterFunction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Symbol sym= NULL;
	str *mod = getArgReference_str(stk,pci,1);
	str *fcn = getArgReference_str(stk,pci,2);
	str *code = getArgReference_str(stk,pci,3);
	str *help = getArgReference_str(stk,pci,4);
	InstrPtr sig;
	str msg, ahelp;
	const char *fcnName, *modName;

	msg= compileString(&sym, cntxt,*code);
	if( msg == MAL_SUCCEED) {
		assert(cntxt->usermodule);
		//mnstr_printf(cntxt->fdout,"#register FUNCTION %s.%s\n",
			//getModuleId(sym->def->stmt[0]), getFunctionId(sym->def->stmt[0]));
		mb= sym->def;
		fcnName = putName(*fcn);
		modName = putName(*mod);
		ahelp = GDKstrdup(*help);
		if(fcnName == NULL || modName == NULL || ahelp == NULL) {
			freeSymbol(sym);
			GDKfree(ahelp);
			throw(MAL, "language.register", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		mb->help= ahelp;
		sig= getSignature(sym);
		sym->name= fcnName;
		setModuleId(sig, modName);
		setFunctionId(sig, sym->name);
		insertSymbol(findModule(cntxt->usermodule, getModuleId(sig)), sym);
	}
	return msg;
}
static str
CMDevalFile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str s = *getArgReference_str(stk,pci,1);
	char *msg = NULL;
	(void) mb;
	(void) cntxt;

	if (s == 0)
		throw(MAL, "mal.evalFile", RUNTIME_FILE_NOT_FOUND "missing file name");

	if (!MT_path_absolute(s)) {
		char *buf = GDKmalloc(strlen(monet_cwd) + strlen(s) + 2);
		if ( buf == NULL)
			throw(MAL,"language.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);

		stpcpy(stpcpy(stpcpy(buf, monet_cwd), DIR_SEP_STR), s);
		msg = evalFile(buf, 0);
		GDKfree(buf);
	} else
		msg = evalFile(s, 0);
	return msg;
}
/*
 * Calling a BAT is simply translated into a concatenation of
 * all the unquoted strings and then passing it to the callEval.
 */
static str
CMDcallBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	throw(MAL, "mal.call", SQLSTATE(0A000) PROGRAM_NYI);
}

#include "mel.h"
mel_func language_init_funcs[] = {
 pattern("language", "call", CMDcallString, false, "Evaluate a MAL string program.", args(1,2, arg("",void),arg("s",str))),
 pattern("language", "call", CMDcallFunction, false, "", args(1,3, arg("",void),arg("m",str),arg("f",str))),
 pattern("language", "call", CMDcallBAT, false, "Evaluate a program stored in a BAT.", args(1,2, arg("",void),batarg("s",str))),
 pattern("language", "source", CMDevalFile, false, "Merge the instructions stored in the file with the current program.", args(1,2, arg("",void),arg("f",str))),
 command("language", "raise", CMDraise, true, "Raise an exception labeled \nwith a specific message.", args(1,2, arg("",str),arg("msg",str))),
 command("language", "assert", MALassertBit, true, "", args(1,3, arg("",void),arg("v",bit),arg("term",str))),
 command("language", "assert", MALassertSht, true, "", args(1,3, arg("",void),arg("v",sht),arg("term",str))),
 command("language", "assert", MALassertInt, true, "", args(1,3, arg("",void),arg("v",int),arg("term",str))),
 command("language", "assert", MALassertLng, true, "", args(1,3, arg("",void),arg("v",lng),arg("term",str))),
 command("language", "assert", MALassertStr, true, "", args(1,3, arg("",void),arg("v",str),arg("term",str))),
 command("language", "assert", MALassertOid, true, "", args(1,3, arg("",void),arg("v",oid),arg("term",str))),
 pattern("language", "assert", MALassertTriple, true, "Assertion test.", args(1,5, arg("",void),argany("v",1),arg("pname",str),arg("oper",str),argany("val",2))),
 pattern("language", "dataflow", MALstartDataflow, false, "The current guarded block is executed using dataflow control. ", args(1,1, arg("",bit))),
 pattern("language", "sink", MALgarbagesink, false, "Variables to be considered together when triggering garbage collection.\nUsed in the dataflow blocks to avoid early release of values.", args(1,2, arg("",void),varargany("v",0))),
 pattern("language", "pass", MALpass, false, "Cheap instruction to disgard storage while retaining the dataflow dependency", args(0,1, argany("v",1))),
 pattern("language", "block", deblockdataflow, false, "Block on availability of all variables w, and then pass on v", args(1,3, arg("",int),arg("v",int),varargany("w",0))),
 pattern("language", "register", CMDregisterFunction, false, "Compile the code string to MAL and register it as a function.", args(1,5, arg("",void),arg("m",str),arg("f",str),arg("code",str),arg("help",str))),
#ifdef HAVE_HGE
 command("language", "assert", MALassertHge, true, "", args(0,2, arg("v",hge),arg("term",str))),
#endif
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_language_mal)
{ mal_module("language", NULL, language_init_funcs); }
