/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * author Martin Kersten
 * MAL debugger interface
 * This module provides access to the functionality offered
 * by the MonetDB debugger and interpreter status.
 * It is primarilly used in interactive sessions to activate
 * the debugger at a given point. Furthermore, the instructions
 * provide the necessary handle to generate information
 * for post-mortum analysis.
 *
 * To enable ease of debugging and performance monitoring, the MAL interpreter
 * comes with a hardwired gdb-like text-based debugger.
 * A limited set of instructions can be included in the programs themselves,
 * but beware that debugging has a global effect. Any concurrent user
 * will be affected by breakpoints being set.
 *
 * The prime scheme to inspect the MAL interpreter status is to use
 * the MAL debugger directly. However, in case of automatic exception handling
 * it helps to be able to obtain BAT versions of the critical information,
 * such as stack frame table, stack trace,
 * and the instruction(s) where an exception occurred.
 * The inspection typically occurs in the exception handling part of the
 * MAL block.
 *
 * Beware, a large class of internal errors can not easily captured this way.
 * For example, bus-errors and segmentation faults lead to premature
 * termination of the process. Similar, creation of the post-mortum
 * information may fail due to an inconsistent state or insufficient resources.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mutils.h"
#include <time.h>
#include <sys/types.h>
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#include "mal_resolve.h"
#include "mal_linker.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"
#include "mal_authorize.h"
#include "mal_function.h"

#define MDBstatus(X) \
	if( stk->cmd && X==0 ) \
		mnstr_printf(cntxt->fdout,"#Monet Debugger off\n"); \
	else if(stk->cmd==0 && X) \
		mnstr_printf(cntxt->fdout,"#Monet Debugger on\n");

static int
pseudo(bat *ret, BAT *b, const char *X1, const char *X2, const char *X3)
{
	char buf[BUFSIZ];
	snprintf(buf, BUFSIZ, "%s_%s_%s", X1, X2, X3);
	if (BBPindex(buf) <= 0 && BBPrename(b, buf) != 0)
		return -1;
	if (BATroles(b, X2) != GDK_SUCCEED)
		return -1;
	*ret = b->batCacheid;
	BBPkeepref(b);
	return 0;
}

static str
MDBgetVMsize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *ret = getArgReference_lng(stk, p, 0);

	(void) cntxt;
	(void) mb;					/* still unused */
	*ret = (lng) GDK_vm_maxsize / 1024 / 1024;
	return MAL_SUCCEED;
}

/* Set the max VM in MBs */
static str
MDBsetVMsize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	lng *ret = getArgReference_lng(stk, p, 0);

	(void) cntxt;
	(void) mb;					/* still unused */
	*ret = (lng) GDK_vm_maxsize;
	if (*getArgReference_lng(stk, p, 1) > 1024)
		GDK_vm_maxsize = (size_t) (*getArgReference_lng(stk, p, 1) * 1024 * 1024);
	return MAL_SUCCEED;
}

static str
MDBgetDebug(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int *ret = getArgReference_int(stk, p, 0);

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) p;
	*ret = (int) ATOMIC_GET(&GDKdebug);
	return MAL_SUCCEED;
}

static str
MDBsetDebug(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int *ret = getArgReference_int(stk, p, 0);
	int *flg = getArgReference_int(stk, p, 1);

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) p;
	*ret = (int) GDKgetdebug();
	GDKsetdebug((unsigned) *flg);
	return MAL_SUCCEED;
}

#define addFlag(NME, FLG, DSET) \
	state =  (DSET & FLG)  > 0;\
	if (BUNappend(flg, (void*) NME, false) != GDK_SUCCEED) goto bailout;\
	if (BUNappend(val, &state, false) != GDK_SUCCEED) goto bailout;

static str
MDBgetDebugFlags(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	bat *f = getArgReference_bat(stk, p, 0);
	bat *v = getArgReference_bat(stk, p, 1);
	BAT *flg, *val;
	bit state = 0;

	(void) cntxt;
	(void) mb;

	flg = COLnew(0, TYPE_str, 256, TRANSIENT);
	val = COLnew(0, TYPE_bit, 256, TRANSIENT);

	if (flg == NULL || val == NULL) {
		BBPreclaim(flg);
		BBPreclaim(val);
		throw(MAL, "mdb.getDebugFlags", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	ATOMIC_BASE_TYPE dbg = ATOMIC_GET(&GDKdebug);
	addFlag("threads", GRPthreads, dbg);
	addFlag("memory", GRPmemory, dbg);
	addFlag("properties", GRPproperties, dbg);
	addFlag("io", GRPio, dbg);
	addFlag("heaps", GRPheaps, dbg);
	addFlag("transactions", GRPtransactions, dbg);
	addFlag("modules", GRPmodules, dbg);
	addFlag("algorithms", GRPalgorithms, dbg);
	addFlag("performance", GRPperformance, dbg);
	addFlag("forcemito", GRPforcemito, dbg);

	*f = flg->batCacheid;
	BBPkeepref(flg);
	*v = val->batCacheid;
	BBPkeepref(val);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(flg->batCacheid);
	BBPunfix(val->batCacheid);
	throw(MAL, "mdb.getDebugFlags", SQLSTATE(HY013) "Failed to append");
}

/* Toggle the debug flags on/off */
static str
MDBsetDebugStr_(int *ret, str *flg)
{
	ATOMIC_BASE_TYPE debug = ATOMIC_GET(&GDKdebug);
	if (strcmp("threads", *flg) == 0)
		debug ^=GRPthreads;
	else if (strcmp("memory", *flg) == 0)
		debug ^=GRPmemory;
	else if (strcmp("properties", *flg) == 0)
		debug ^=GRPproperties;
	else if (strcmp("io", *flg) == 0)
		debug ^=GRPio;
	else if (strcmp("heaps", *flg) == 0)
		debug ^=GRPheaps;
	else if (strcmp("transactions", *flg) == 0)
		debug ^=GRPtransactions;
	else if (strcmp("modules", *flg) == 0)
		debug ^=GRPmodules;
	else if (strcmp("algorithms", *flg) == 0)
		debug ^=GRPalgorithms;
	else if (strcmp("performance", *flg) == 0)
		debug ^=GRPperformance;
	else if (strcmp("forcemito", *flg) == 0)
		debug ^=GRPforcemito;
	else
		throw(MAL, "mdb.setDebugStr", ILLEGAL_ARGUMENT);
	*ret = (int) GDKgetdebug();
	GDKsetdebug((unsigned) debug);

	return MAL_SUCCEED;
}

static str
MDBsetDebugStr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str *flg = (str *) getArgReference(stk, p, 1);
	int *ret = (int *) getArgReference(stk, p, 0);

	(void) cntxt;
	(void) mb;
	return MDBsetDebugStr_(ret, flg);
}

/*
 * Variables and stack information
 * The variable information can be turned into a BAT for inspection as well.
 */

static int
getStkDepth(MalStkPtr s)
{
	int i = 0;

	while (s != 0) {
		i++;
		s = s->up;
	}
	return i;
}

static str
MDBStkDepth(Client cntxt, MalBlkPtr mb, MalStkPtr s, InstrPtr p)
{
	int *ret = getArgReference_int(s, p, 0);

	(void) cntxt;
	(void) mb;					/* fool compiler */
	*ret = getStkDepth(s);
	return MAL_SUCCEED;
}

static str
MDBgetFrame(BAT *b, BAT *bn, MalBlkPtr mb, MalStkPtr s, int depth,
			const char *name)
{
	ValPtr v;
	int i;
	char *buf = 0;

	while (depth > 0 && s) {
		depth--;
		s = s->up;
	}
	if (s != 0)
		for (i = 0; i < s->stktop; i++, v++) {
			v = &s->stk[i];
			if ((buf = ATOMformat(v->vtype, VALptr(v))) == NULL ||
				BUNappend(b, getVarName(mb, i), false) != GDK_SUCCEED ||
				BUNappend(bn, buf, false) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				BBPunfix(bn->batCacheid);
				GDKfree(buf);
				throw(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			GDKfree(buf);
			buf = NULL;
		}
	return MAL_SUCCEED;
}

static str
MDBgetStackFrame(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	bat *ret = getArgReference_bat(s, p, 0);
	bat *ret2 = getArgReference_bat(s, p, 1);
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);
	BAT *bn = COLnew(0, TYPE_str, 256, TRANSIENT);
	str err;

	(void) cntxt;
	if (b == 0 || bn == 0) {
		BBPreclaim(b);
		BBPreclaim(bn);
		throw(MAL, "mdb.getStackFrame", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if ((err = MDBgetFrame(b, bn, m, s, 0, "mdb.getStackFrame")) !=MAL_SUCCEED) {
		BBPreclaim(b);
		BBPreclaim(bn);
		return err;
	}
	if (pseudo(ret, b, "view", "stk", "frame")) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "mdb.getStackFrame", GDK_EXCEPTION);
	}
	if (pseudo(ret2, bn, "view", "stk", "frame")) {
		BBPrelease(*ret);
		BBPunfix(bn->batCacheid);
		throw(MAL, "mdb.getStackFrame", GDK_EXCEPTION);
	}
	return MAL_SUCCEED;
}

static str
MDBgetStackFrameN(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	int n;
	bat *ret = getArgReference_bat(s, p, 0);
	bat *ret2 = getArgReference_bat(s, p, 1);
	BAT *b;
	BAT *bn;
	str err;

	(void) cntxt;
	n = *getArgReference_int(s, p, 2);
	if (n < 0 || n >= getStkDepth(s))
		throw(MAL, "mdb.getStackFrame", ILLEGAL_ARGUMENT " Illegal depth.");

	b = COLnew(0, TYPE_str, 256, TRANSIENT);
	bn = COLnew(0, TYPE_str, 256, TRANSIENT);
	if (b == 0 || bn == 0) {
		BBPreclaim(b);
		BBPreclaim(bn);
		throw(MAL, "mdb.getStackFrame", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if ((err = MDBgetFrame(b, bn, m, s, n, "mdb.getStackFrameN")) !=MAL_SUCCEED) {
		BBPreclaim(b);
		BBPreclaim(bn);
		return err;
	}
	if (pseudo(ret, b, "view", "stk", "frame")) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "mdb.getStackFrameN", GDK_EXCEPTION);
	}
	if (pseudo(ret2, bn, "view", "stk", "frameB")) {
		BBPrelease(*ret);
		BBPunfix(bn->batCacheid);
		throw(MAL, "mdb.getStackFrameN", GDK_EXCEPTION);
	}
	return MAL_SUCCEED;
}

static str
MDBStkTrace(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	BAT *b, *bn;
	str msg;
	char *buf;
	bat *ret = getArgReference_bat(s, p, 0);
	bat *ret2 = getArgReference_bat(s, p, 1);
	int k = 0;
	size_t len, l;
	int pcup;

	b = COLnew(0, TYPE_int, 256, TRANSIENT);
	if (b == NULL)
		throw(MAL, "mdb.getStackTrace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	bn = COLnew(0, TYPE_str, 256, TRANSIENT);
	if (bn == NULL) {
		BBPreclaim(b);
		throw(MAL, "mdb.getStackTrace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	(void) cntxt;
	if ((msg = instruction2str(s->blk, s, p, LIST_MAL_DEBUG)) == NULL) {
		BBPreclaim(b);
		BBPreclaim(bn);
		throw(MAL, "mdb.getStackTrace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	len = strlen(msg);
	buf = (char *) GDKmalloc(len + 1024);
	if (buf == NULL) {
		GDKfree(msg);
		BBPreclaim(b);
		BBPreclaim(bn);
		throw(MAL, "mdb.setTrace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	snprintf(buf, len + 1024, "%s at %s.%s[%d]", msg,
			 getModuleId(getInstrPtr(m, 0)),
			 getFunctionId(getInstrPtr(m, 0)), getPC(m, p));
	if (BUNappend(b, &k, false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED) {
		GDKfree(msg);
		GDKfree(buf);
		BBPreclaim(b);
		BBPreclaim(bn);
		throw(MAL, "mdb.setTrace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	GDKfree(msg);

	for (pcup = s->pcup, s = s->up, k++; s != NULL;
		 pcup = s->pcup, s = s->up, k++) {
		if ((msg = instruction2str(s->blk, s, getInstrPtr(s->blk, pcup),
								   LIST_MAL_DEBUG)) == NULL) {
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			throw(MAL, "mdb.setTrace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		l = strlen(msg);
		if (l > len) {
			GDKfree(buf);
			len = l;
			buf = (char *) GDKmalloc(len + 1024);
			if (buf == NULL) {
				GDKfree(msg);
				BBPunfix(b->batCacheid);
				BBPunfix(bn->batCacheid);
				throw(MAL, "mdb.setTrace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		snprintf(buf, len + 1024, "%s at %s.%s[%d]", msg,
				 getModuleId(getInstrPtr(s->blk, 0)),
				 getFunctionId(getInstrPtr(s->blk, 0)), pcup);
		if (BUNappend(b, &k, false) != GDK_SUCCEED ||
			BUNappend(bn, buf, false) != GDK_SUCCEED) {
			GDKfree(buf);
			GDKfree(msg);
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			throw(MAL, "mdb.setTrace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(msg);
	}
	GDKfree(buf);
	if (pseudo(ret, b, "view", "stk", "trace")) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "mdb.setTrace", GDK_EXCEPTION);
	}
	if (pseudo(ret2, bn, "view", "stk", "traceB")) {
		BBPrelease(*ret);
		BBPunfix(bn->batCacheid);
		throw(MAL, "mdb.setTrace", GDK_EXCEPTION);
	}
	return MAL_SUCCEED;
}

/*
 * Display routines
 */
static str
MDBlist(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) p;
	(void) stk;
	printFunction(cntxt->fdout, mb, 0, LIST_MAL_NAME);
	return MAL_SUCCEED;
}

static str
MDBlistMapi(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) p;
	(void) stk;
	printFunction(cntxt->fdout, mb, 0, LIST_MAL_ALL);
	return MAL_SUCCEED;
}

static str
MDBlist3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str modnme = *getArgReference_str(stk, p, 1);
	str fcnnme = *getArgReference_str(stk, p, 2);
	Symbol s = NULL;

	s = findSymbol(cntxt->usermodule, putName(modnme), putName(fcnnme));
	if (s == NULL)
		throw(MAL, "mdb.list", "Could not find %s.%s", modnme, fcnnme);
	printFunction(cntxt->fdout, s->def, 0, LIST_MAL_NAME);
	(void) mb;					/* fool compiler */
	return MAL_SUCCEED;
}

static str
MDBlistDetail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) p;
	(void) stk;
	printFunction(cntxt->fdout, mb, 0, LIST_MAL_DEBUG);
	return MAL_SUCCEED;
}

static str
MDBlist3Detail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str modnme = *getArgReference_str(stk, p, 1);
	str fcnnme = *getArgReference_str(stk, p, 2);
	Symbol s = NULL;

	s = findSymbol(cntxt->usermodule, putName(modnme), putName(fcnnme));
	if (s == NULL)
		throw(MAL, "mdb.list", "Could not find %s.%s", modnme, fcnnme);
	printFunction(cntxt->fdout, s->def, 0, LIST_MAL_DEBUG);
	(void) mb;					/* fool compiler */
	return NULL;
}

/* Utilities
 * Dumping a stack on a file is primarilly used for debugging.
 * Printing the stack requires access to both the symbol table and
 * the stackframes in most cases.
 * Beware that a stack frame need not be initialized with null values.
 * It has been zeroed upon creation.
 *
 * The routine  can also be used to inspect the symbol table of
 * arbitrary functions.
 */
static void
printStackHdr(stream *f, MalBlkPtr mb, ValPtr v, int index)
{
	if (v == 0 && isVarConstant(mb, index))
		v = &getVarConstant(mb, index);
	mnstr_printf(f, "#[%2d] %5s", index, getVarName(mb, index));
	mnstr_printf(f, " (%d,%d,%d) = ", getBeginScope(mb, index),
				 getLastUpdate(mb, index), getEndScope(mb, index));
	if (v)
		ATOMprint(v->vtype, VALptr(v), f);
}

static void
printBATproperties(stream *f, BAT *b)
{
	mnstr_printf(f, " count=" BUNFMT " lrefs=%d ",
				 BATcount(b), BBP_lrefs(b->batCacheid));
	if (BBP_refs(b->batCacheid) - 1)
		mnstr_printf(f, " refs=%d ", BBP_refs(b->batCacheid));
	if (b->theap->refs)
		mnstr_printf(f, " views=%llu",
					 (unsigned long long) ATOMIC_GET(&b->theap->refs));
	if (b->tvheap->refs)
		mnstr_printf(f, " shared vheaps=%llu",
					 (unsigned long long) ATOMIC_GET(&b->tvheap->refs));
	if (b->theap->parentid != b->batCacheid)
		mnstr_printf(f, "view on %s ", BBP_logical(b->theap->parentid));
}

static void
printBATelm(stream *f, bat i, BUN cnt, BUN first)
{
	BAT *b, *bs = NULL;
	str tpe;

	b = BATdescriptor(i);
	if (b) {
		tpe = getTypeName(newBatType(b->ttype));
		mnstr_printf(f, ":%s ", tpe);
		GDKfree(tpe);
		printBATproperties(f, b);
		/* perform property checking */
		BATassertProps(b);
		mnstr_printf(f, "\n");
		if (cnt && BATcount(b) > 0) {
			if (cnt < BATcount(b)) {
				mnstr_printf(f, "Sample " BUNFMT " out of " BUNFMT "\n", cnt,
							 BATcount(b));
			}
			/* cut out a portion of the BAT for display */
			bs = BATslice(b, first, first + cnt);
			/* get the void values */
			if (bs == NULL)
				mnstr_printf(f, "Failed to take chunk\n");
			else {
				if (BATprint(f, bs) != GDK_SUCCEED)
					 mnstr_printf(f, "Failed to print chunk\n");
				BBPunfix(bs->batCacheid);
			}
		}

		BBPunfix(b->batCacheid);
	} else
		mnstr_printf(f, "\n");
}

static void
printStackElm(stream *f, MalBlkPtr mb, ValPtr v, int index, BUN cnt, BUN first)
{
	str nme, nmeOnStk;
	VarPtr n = getVar(mb, index);

	printStackHdr(f, mb, v, index);

	if (v && v->vtype == TYPE_bat) {
		bat i = v->val.bval;
		BAT *b = BBPquickdesc(i);

		if (b) {
			nme = getTypeName(newBatType(b->ttype));
			mnstr_printf(f, " :%s rows=" BUNFMT, nme, BATcount(b));
		} else {
			nme = getTypeName(n->type);
			mnstr_printf(f, " :%s", nme);
		}
	} else {
		nme = getTypeName(n->type);
		mnstr_printf(f, " :%s", nme);
	}
	nmeOnStk = v ? getTypeName(v->vtype) : GDKstrdup(nme);
	/* check for type errors */
	if (nmeOnStk && strcmp(nmeOnStk, nme) && strncmp(nmeOnStk, "BAT", 3))
		mnstr_printf(f, "!%s ", nmeOnStk);
	mnstr_printf(f, " %s", (isVarConstant(mb, index) ? " constant" : ""));
	mnstr_printf(f, " %s", (isVarTypedef(mb, index) ? " type variable" : ""));
	GDKfree(nme);
	mnstr_printf(f, "\n");
	GDKfree(nmeOnStk);

	if (cnt && v && (isaBatType(n->type) || v->vtype == TYPE_bat)
		&& !is_bat_nil(v->val.bval)) {
		printBATelm(f, v->val.bval, cnt, first);
	}
}

static void
printStack(stream *f, MalBlkPtr mb, MalStkPtr s)
{
	int i = 0;

	setVariableScope(mb);
	if (s) {
		mnstr_printf(f, "#Stack '%s' size=%d top=%d\n",
					 getInstrPtr(mb, 0)->fcnname, s->stksize, s->stktop);
		for (; i < mb->vtop; i++)
			printStackElm(f, mb, s->stk + i, i, 0, 0);
	} else
		for (; i < mb->vtop; i++)
			printStackElm(f, mb, 0, i, 0, 0);
}

static str
MDBvar(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) p;
	(void) stk;
	(void) cntxt;
	(void) mb;
	printStack(cntxt->fdout, mb, stk);
	return MAL_SUCCEED;
}

static str
MDBvar3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str modnme = *getArgReference_str(stk, p, 1);
	str fcnnme = *getArgReference_str(stk, p, 2);
	Symbol s = NULL;

	s = findSymbol(cntxt->usermodule, putName(modnme), putName(fcnnme));
	if (s == NULL)
		throw(MAL, "mdb.var", "Could not find %s.%s", modnme, fcnnme);
	(void) cntxt;
	(void) mb;
	printStack(cntxt->fdout, s->def, (s->def == mb ? stk : 0));
	(void) mb;
	return NULL;
}

/*
 * It is illustrative to dump the code when you
 * have encountered an error.
 */
static str
MDBgetDefinition(Client cntxt, MalBlkPtr m, MalStkPtr stk, InstrPtr p)
{
	int i;
	bat *ret = getArgReference_bat(stk, p, 0);
	str ps;
	BAT *b = COLnew(0, TYPE_str, 256, TRANSIENT);

	(void) cntxt;
	if (b == 0)
		throw(MAL, "mdb.getDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; i < m->stop; i++) {
		if ((ps = instruction2str(m, 0, getInstrPtr(m, i), 1)) == NULL) {
			BBPreclaim(b);
			throw(MAL, "mdb.getDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (BUNappend(b, ps, false) != GDK_SUCCEED) {
			GDKfree(ps);
			BBPreclaim(b);
			throw(MAL, "mdb.getDefinition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		GDKfree(ps);
	}
	if (pseudo(ret, b, "view", "fcn", "stmt")) {
		BBPreclaim(b);
		throw(MAL, "mdb.getDefinition", GDK_EXCEPTION);
	}

	return MAL_SUCCEED;
}

static str
MDBgetExceptionVariable(str *ret, str *msg)
{
	str tail;

	tail = strchr(*msg, ':');
	if (tail == 0)
		throw(MAL, "mdb.getExceptionVariable",
			  OPERATION_FAILED " ':'<name> missing");

	*tail = 0;
	*ret = GDKstrdup(*msg);
	if (*ret == NULL)
		throw(MAL, "mdb.getExceptionVariable", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*tail = ':';
	return MAL_SUCCEED;
}

static str
MDBgetExceptionContext(str *ret, str *msg)
{
	str tail, tail2;

	tail = strchr(*msg, ':');
	if (tail == 0)
		throw(MAL, "mdb.getExceptionContext",
			  OPERATION_FAILED " ':'<name> missing");
	tail2 = strchr(tail + 1, ':');
	if (tail2 == 0)
		throw(MAL, "mdb.getExceptionContext",
			  OPERATION_FAILED " <name> missing");

	*tail2 = 0;
	*ret = GDKstrdup(tail + 1);
	if (*ret == NULL)
		throw(MAL, "mdb.getExceptionContext", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*tail2 = ':';
	return MAL_SUCCEED;
}

static str
MDBgetExceptionReason(str *ret, str *msg)
{
	str tail;

	tail = strchr(*msg, ':');
	if (tail == 0)
		throw(MAL, "mdb.getExceptionReason", OPERATION_FAILED " '::' missing");
	tail = strchr(tail + 1, ':');
	if (tail == 0)
		throw(MAL, "mdb.getExceptionReason", OPERATION_FAILED " ':' missing");

	*ret = GDKstrdup(tail + 1);
	if (*ret == NULL)
		throw(MAL, "mdb.getExceptionReason", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
MDBdummy(void *ret)
{
	(void) ret;
	throw(MAL, "mdb.dummy", OPERATION_FAILED);
}


static str
CMDmodules(bat *bid)
{
	BAT *b = getModules();

	if (b == NULL)
		throw(MAL, "mdb.modules", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*bid = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func mdb_init_funcs[] = {
 command("mdb", "modules", CMDmodules, false, "List available modules", args(1,1, batarg("",str))),
 pattern("mdb", "getVMsize", MDBgetVMsize, false, "Retrieve the max VM size", args(1,1, arg("",lng))),
 pattern("mdb", "setVMsize", MDBsetVMsize, false, "Manipulate the VM max size in MBs", args(1,2, arg("",lng),arg("l",lng))),
 pattern("mdb", "getDebugFlags", MDBgetDebugFlags, false, "Get the kernel debugging flags bit-set", args(2,2, batarg("flg",str),batarg("val",bit))),
 pattern("mdb", "getDebug", MDBgetDebug, false, "Get the kernel debugging bit-set.\nSee the MonetDB configuration file for details", args(1,1, arg("",int))),
 pattern("mdb", "setDebug", MDBsetDebugStr, false, "Set the kernel debugging bit-set and return its previous value.\nThe recognized options are: threads, memory, properties,\nio, transactions, modules, algorithms, estimates.", args(1,2, arg("",int),arg("flg",str))),
 pattern("mdb", "setDebug", MDBsetDebug, false, "Set the kernel debugging bit-set and return its previous value.", args(1,2, arg("",int),arg("flg",int))),
 command("mdb", "getException", MDBgetExceptionVariable, false, "Extract the variable name from the exception message", args(1,2, arg("",str),arg("s",str))),
 command("mdb", "getReason", MDBgetExceptionReason, false, "Extract the reason from the exception message", args(1,2, arg("",str),arg("s",str))),
 command("mdb", "getContext", MDBgetExceptionContext, false, "Extract the context string from the exception message", args(1,2, arg("",str),arg("s",str))),
 pattern("mdb", "list", MDBlist, false, "Dump the current routine on standard out.", args(1,1, arg("",void))),
 pattern("mdb", "listMapi", MDBlistMapi, false, "Dump the current routine on standard out with Mapi prefix.", args(1,1, arg("",void))),
 pattern("mdb", "list", MDBlist3, false, "Dump the routine M.F on standard out.", args(1,3, arg("",void),arg("M",str),arg("F",str))),
 pattern("mdb", "List", MDBlistDetail, false, "Dump the current routine on standard out.", args(1,1, arg("",void))),
 pattern("mdb", "List", MDBlist3Detail, false, "Dump the routine M.F on standard out.", args(1,3, arg("",void),arg("M",str),arg("F",str))),
 pattern("mdb", "var", MDBvar, false, "Dump the symboltable of current routine on standard out.", args(1,1, arg("",void))),
 pattern("mdb", "var", MDBvar3, false, "Dump the symboltable of routine M.F on standard out.", args(1,3, arg("",void),arg("M",str),arg("F",str))),
 pattern("mdb", "getStackDepth", MDBStkDepth, false, "Return the depth of the calling stack.", args(1,1, arg("",int))),
 pattern("mdb", "getStackFrame", MDBgetStackFrameN, false, "", args(2,3, batarg("",str),batarg("",str),arg("i",int))),
 pattern("mdb", "getStackFrame", MDBgetStackFrame, false, "Collect variable binding of current (n-th) stack frame.", args(2,2, batarg("",str),batarg("",str))),
 pattern("mdb", "getStackTrace", MDBStkTrace, false, "", args(2,2, batarg("",int),batarg("",str))),
 pattern("mdb", "getDefinition", MDBgetDefinition, false, "Returns a string representation of the current function \nwith typing information attached", args(1,1, batarg("",str))),
 command("mdb", "#dummy", MDBdummy, false, "Dummy function for testing", args(1,1, arg("",void))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mdb_mal)
{ mal_module("mdb", NULL, mdb_init_funcs); }
