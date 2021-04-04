/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (author) M.L. Kersten
 * For documentation see website.
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_debugger.h"
#include "mal_interpreter.h"	/* for getArgReference() */
#include "mal_listing.h"
#include "mal_function.h"
#include "mal_parser.h"
#include "mal_namespace.h"
#include "mal_private.h"

typedef struct {
	MalBlkPtr brkBlock[MAXBREAKS];
	int brkPc[MAXBREAKS];
	int brkVar[MAXBREAKS];
	const char *brkMod[MAXBREAKS];
	const char *brkFcn[MAXBREAKS];
	char brkCmd[MAXBREAKS];
	str brkRequest[MAXBREAKS];
	int brkTop;
} mdbStateRecord, *mdbState;

typedef struct MDBSTATE{
	MalBlkPtr mb;
	MalStkPtr stk;
	InstrPtr p;
	int pc;
} MdbState;

#define skipBlanc(c, X)    while (*(X) && isspace((unsigned char) *X)) { X++; }
#define skipNonBlanc(c, X) while (*(X) && !isspace((unsigned char) *X)) { X++; }
#define skipWord(c, X)     while (*(X) && (isalnum((unsigned char) *X))) { X++; } \
	skipBlanc(c, X);

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
	mnstr_printf(f, "#[%2d] %5s", index, getVarName(mb,index));
	mnstr_printf(f, " (%d,%d,%d) = ", getBeginScope(mb,index), getLastUpdate(mb,index),getEndScope(mb, index));
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
	if (b->batSharecnt)
		mnstr_printf(f, " views=%d", b->batSharecnt);
	if (b->theap->parentid != b->batCacheid)
		mnstr_printf(f, "view on %s ", BBPname(b->theap->parentid));
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
				mnstr_printf(f, "Sample " BUNFMT " out of " BUNFMT "\n", cnt, BATcount(b));
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
		BAT *b = BBPquickdesc(i, true);

		if (b) {
			nme = getTypeName(newBatType(b->ttype));
			mnstr_printf(f, " :%s rows="BUNFMT, nme, BATcount(b));
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

	if (cnt && v && (isaBatType(n->type) || v->vtype == TYPE_bat) && !is_bat_nil(v->val.bval)) {
		printBATelm(f,v->val.bval,cnt,first);
	}
}

void
printStack(stream *f, MalBlkPtr mb, MalStkPtr s)
{
	int i = 0;

	if (s) {
		mnstr_printf(f, "#Stack '%s' size=%d top=%d\n",
				getInstrPtr(mb, 0)->fcnname, s->stksize, s->stktop);
		for (; i < mb->vtop; i++)
			printStackElm(f, mb, s->stk + i, i, 0, 0);
	} else
		for (; i < mb->vtop; i++)
			printStackElm(f, mb, 0, i, 0, 0);
}

/* utility to display an instruction being called and its stack position */
static void
printCall(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc)
{
	str msg;
	msg = instruction2str(mb, stk, getInstrPtr(mb, pc), LIST_MAL_CALL);
	mnstr_printf(cntxt->fdout, "#%s at %s.%s[%d]\n", (msg?msg:"failed instruction2str()") ,
			getModuleId(getInstrPtr(mb, 0)),
			getFunctionId(getInstrPtr(mb, 0)), pc);
	GDKfree(msg);
}

static void
mdbBacktrace(Client cntxt, MalStkPtr stk, int pci)
{
	for (; stk != NULL; stk = stk->up) {
		printCall(cntxt, stk->blk, stk, pci);
		if (stk->up)
			pci = stk->up->pcup;
	}
}

void
mdbDump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i = getPC(mb, pci);
	mnstr_printf(cntxt->fdout, "!MDB dump of instruction %d\n", i);
	if( i < 0)
		return;
	printFunction(cntxt->fdout, mb, stk, LIST_MAL_ALL);
	mdbBacktrace(cntxt, stk, i);
	printStack(cntxt->fdout, mb, stk);
}

static inline char *
pre(const char *s1, const char *s2, char *buf)
{
	snprintf(buf, 64, "%s%s", s1, s2);
	return buf;
}

static inline char *
local_itoa(ssize_t i, char *buf)
{
	snprintf(buf, 32, "%zd", i);
	return buf;
}
static inline char *
local_utoa(size_t i, char *buf)
{
	snprintf(buf, 32, "%zu", i);
	return buf;
}

static inline char *
oidtostr(oid i, char *p, size_t len)
{
	if (OIDtoStr(&p, &len, &i, false) < 0)
		return NULL;
	return p;
}

static gdk_return
infoHeap(BAT *bk, BAT*bv, Heap *hp, str nme)
{
	char buf[1024], *p = buf;

	if (!hp)
		return GDK_SUCCEED;
	while (*nme)
		*p++ = *nme++;
	strcpy(p, "free");
	if (BUNappend(bk, buf, false) != GDK_SUCCEED ||
		BUNappend(bv, local_utoa(hp->free, buf), false) != GDK_SUCCEED)
		return GDK_FAIL;
	strcpy(p, "size");
	if (BUNappend(bk, buf, false) != GDK_SUCCEED ||
		BUNappend(bv, local_utoa(hp->size, buf), false) != GDK_SUCCEED)
		return GDK_FAIL;
	strcpy(p, "storage");
	if (BUNappend(bk, buf, false) != GDK_SUCCEED ||
		BUNappend(bv, (hp->base == NULL || hp->base == (char*)1) ? "absent" : (hp->storage == STORE_MMAP) ? (hp->filename[0] ? "memory mapped" : "anonymous vm") : (hp->storage == STORE_PRIV) ? "private map" : "malloced", false) != GDK_SUCCEED)
		return GDK_FAIL;
	strcpy(p, "newstorage");
	if (BUNappend(bk, buf, false) != GDK_SUCCEED ||
		BUNappend(bv, (hp->newstorage == STORE_MEM) ? "malloced" : (hp->newstorage == STORE_PRIV) ? "private map" : "memory mapped", false) != GDK_SUCCEED)
		return GDK_FAIL;
	strcpy(p, "filename");
	if (BUNappend(bk, buf, false) != GDK_SUCCEED ||
		BUNappend(bv, hp->filename[0] ? hp->filename : "no file", false) != GDK_SUCCEED)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

#define COLLISION (8 * sizeof(size_t))

static gdk_return
HASHinfo(BAT *bk, BAT *bv, Hash *h, str s)
{
	BUN i;
	BUN j;
	BUN k;
	BUN cnt[COLLISION + 1];
	char buf[32];
	char prebuf[64];

	if (BUNappend(bk, pre(s, "type", prebuf), false) != GDK_SUCCEED ||
	    BUNappend(bv, ATOMname(h->type),false) != GDK_SUCCEED ||
	    BUNappend(bk, pre(s, "mask", prebuf), false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(h->nbucket, buf),false) != GDK_SUCCEED)
		return GDK_FAIL;

	for (i = 0; i < COLLISION + 1; i++) {
		cnt[i] = 0;
	}
	for (i = 0; i < h->nbucket; i++) {
		j = HASHlist(h, i);
		for (k = 0; j; k++)
			j >>= 1;
		cnt[k]++;
	}

	for (i = 0; i < COLLISION + 1; i++)
		if (cnt[i]) {
			if (BUNappend(bk, pre(s, local_utoa(i?(((size_t)1)<<(i-1)):0, buf), prebuf), false) != GDK_SUCCEED ||
			    BUNappend(bv, local_utoa((size_t) cnt[i], buf), false) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	return GDK_SUCCEED;
}

str
BATinfo(BAT **key, BAT **val, const bat bid)
{
	const char *mode, *accessmode;
	BAT *bk = NULL, *bv= NULL, *b;
	char bf[oidStrlen];
	char buf[32];

	if ((b = BATdescriptor(bid)) == NULL) {
		throw(MAL, "BATinfo", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	bk = COLnew(0, TYPE_str, 128, TRANSIENT);
	bv = COLnew(0, TYPE_str, 128, TRANSIENT);
	if (bk == NULL || bv == NULL) {
		BBPreclaim(bk);
		BBPreclaim(bv);
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.getInfo", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if (b->batTransient) {
		mode = "transient";
	} else {
		mode = "persistent";
	}

	switch (b->batRestricted) {
	case BAT_READ:
		accessmode = "read-only";
		break;
	case BAT_WRITE:
		accessmode = "updatable";
		break;
	case BAT_APPEND:
		accessmode = "append-only";
		break;
	default:
		accessmode = "unknown";
	}

	if (BUNappend(bk, "batId", false) != GDK_SUCCEED ||
	    BUNappend(bv, BATgetId(b), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batCacheid", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) b->batCacheid, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tparentid", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) b->theap->parentid, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batSharecnt", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) b->batSharecnt, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batCount", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa((size_t) b->batCount, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batCapacity", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa((size_t) b->batCapacity, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "head", false) != GDK_SUCCEED ||
	    BUNappend(bv, ATOMname(TYPE_void), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tail", false) != GDK_SUCCEED ||
	    BUNappend(bv, ATOMname(b->ttype), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batPersistence", false) != GDK_SUCCEED ||
	    BUNappend(bv, mode, false) != GDK_SUCCEED ||
	    BUNappend(bk, "batRestricted", false) != GDK_SUCCEED ||
	    BUNappend(bv, accessmode, false) != GDK_SUCCEED ||
	    BUNappend(bk, "batRefcnt", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) BBP_refs(b->batCacheid), buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batLRefcnt", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) BBP_lrefs(b->batCacheid), buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batDirty", false) != GDK_SUCCEED ||
	    BUNappend(bv, BATdirty(b) ? "dirty" : "clean", false) != GDK_SUCCEED ||

	    BUNappend(bk, "hseqbase", false) != GDK_SUCCEED ||
	    BUNappend(bv, oidtostr(b->hseqbase, bf, sizeof(bf)), FALSE) != GDK_SUCCEED ||

	    BUNappend(bk, "tident", false) != GDK_SUCCEED ||
	    BUNappend(bv, b->tident, false) != GDK_SUCCEED ||
	    BUNappend(bk, "tdense", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) BATtdense(b), buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tseqbase", false) != GDK_SUCCEED ||
	    BUNappend(bv, oidtostr(b->tseqbase, bf, sizeof(bf)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tsorted", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) BATtordered(b), buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "trevsorted", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) BATtrevordered(b), buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tkey", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) b->tkey, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tvarsized", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) b->tvarsized, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tnosorted", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnosorted, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tnorevsorted", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnorevsorted, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tnokey[0]", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnokey[0], buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tnokey[1]", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnokey[1], buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tnonil", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnonil, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "tnil", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnil, buf), false) != GDK_SUCCEED ||

	    BUNappend(bk, "batInserted", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->batInserted, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "ttop", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->theap->free, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batCopiedtodisk", false) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t) b->batCopiedtodisk, buf), false) != GDK_SUCCEED ||
	    BUNappend(bk, "batDirtydesc", false) != GDK_SUCCEED ||
	    BUNappend(bv, b->batDirtydesc ? "dirty" : "clean", false) != GDK_SUCCEED ||

	    BUNappend(bk, "theap.dirty", false) != GDK_SUCCEED ||
	    BUNappend(bv, b->theap->dirty ? "dirty" : "clean", false) != GDK_SUCCEED ||
		infoHeap(bk, bv, b->theap, "tail.") != GDK_SUCCEED ||

	    BUNappend(bk, "tvheap->dirty", false) != GDK_SUCCEED ||
	    BUNappend(bv, (b->tvheap && b->tvheap->dirty) ? "dirty" : "clean", false) != GDK_SUCCEED ||
		infoHeap(bk, bv, b->tvheap, "theap.") != GDK_SUCCEED ||

		/* dump index information */
		(b->thash &&
		 HASHinfo(bk, bv, b->thash, "thash->") != GDK_SUCCEED)) {
		BBPreclaim(bk);
		BBPreclaim(bv);
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.getInfo", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	*key = bk;
	*val = bv;
	assert(BATcount(bk) == BATcount(bv));
	BBPunfix(bid);
	return MAL_SUCCEED;
}

#ifndef NDEBUG
static void
printBatDetails(stream *f, bat bid)
{
	BAT *b[2];

	/* at this level we don't know bat kernel primitives */
	mnstr_printf(f, "#Show info for %d\n", bid);
	if (BATinfo(&b[0],&b[1], bid) != MAL_SUCCEED)
		return;
	BATprintcolumns(f, 2, b);
	BBPunfix(b[0]->batCacheid);
	BBPunfix(b[1]->batCacheid);
}

static void
printBatInfo(stream *f, VarPtr n, ValPtr v)
{
	if (isaBatType(n->type) && v->val.ival)
		printBatDetails(f, v->val.ival);
}

static void
mdbHelp(stream *f)
{
	mnstr_printf(f, "next             -- Advance to next statement\n");
	mnstr_printf(f, "continue         -- Continue program being debugged\n");
	mnstr_printf(f, "catch            -- Catch the next exception \n");
	mnstr_printf(f, "break [<var>]    -- set breakpoint on current instruction or <var>\n");
	mnstr_printf(f, "delete [<var>]   -- remove break/trace point <var>\n");
	mnstr_printf(f, "debug <int>      -- set kernel debugging mask\n");
	mnstr_printf(f, "step             -- advance to next MAL instruction\n");
	mnstr_printf(f, "module           -- display a module signatures\n");
	mnstr_printf(f, "atom             -- show atom list\n");
	mnstr_printf(f, "finish           -- finish current call\n");
	mnstr_printf(f, "exit             -- terminate executionr\n");
	mnstr_printf(f, "quit             -- turn off debugging\n");
	mnstr_printf(f, "list <obj>       -- list current program block\n");
	mnstr_printf(f, "list #  [+#],-#  -- list current program block slice\n");
	mnstr_printf(f, "List <obj> [#]   -- list with type information[slice]\n");
	mnstr_printf(f, "list [#] <obj>   -- list program block after optimizer <#>\n");
	mnstr_printf(f, "List #  [+#],-#  -- list current program block slice\n");
	mnstr_printf(f, "var  <obj>       -- print symbol table for module\n");
	mnstr_printf(f, "optimizer <obj>  -- display optimizer steps\n");
	mnstr_printf(f, "print <var>      -- display value of a variable\n");
	mnstr_printf(f, "print <var> <cnt>[<first>] -- display BAT chunk\n");
	mnstr_printf(f, "info <var>       -- display bat variable properties\n");
	mnstr_printf(f, "run              -- restart current procedure\n");
	mnstr_printf(f, "where            -- print stack trace\n");
	mnstr_printf(f, "down             -- go down the stack\n");
	mnstr_printf(f, "up               -- go up the stack\n");
	mnstr_printf(f, "trace <var>      -- trace assignment to variables\n");
	mnstr_printf(f, "trap <mod>.<fcn> -- catch MAL function call in debugger\n");
	mnstr_printf(f, "help             -- this message\n");
}

/*
 * The debugger flags overview
 */
static mdbStateRecord *mdbTable;

bool
mdbInit(void)
{
	/*
	 * Each client has its own breakpoint administration, kept in a
	 * global table.  Although a little space consumptive, it is the
	 * easiest to maintain and much less expensive as reserving debugger
	 * space in each instruction.
	 */
	mdbTable = GDKzalloc(sizeof(mdbStateRecord) * MAL_MAXCLIENTS);
	return mdbTable != NULL;
}

void
mdbExit(void)
{
	if (mdbTable) {
		GDKfree(mdbTable);
		mdbTable = 0;
	}
}

static char
isBreakpoint(Client cntxt, MalBlkPtr mb, InstrPtr p, int pc)
{
	int i, j;

	for (i = 0; i < mdbTable[cntxt->idx].brkTop; i++) {
		if (mdbTable[cntxt->idx].brkBlock[i] != mb)
			continue;
		if (mdbTable[cntxt->idx].brkPc[i] == pc)
			return mdbTable[cntxt->idx].brkCmd[i];

		if (mdbTable[cntxt->idx].brkMod[i] && getModuleId(p) &&
			mdbTable[cntxt->idx].brkFcn[i] && getFunctionId(p) &&
			strcmp(mdbTable[cntxt->idx].brkMod[i], getModuleId(p)) == 0 &&
			strcmp(mdbTable[cntxt->idx].brkFcn[i], getFunctionId(p)) == 0)
			return mdbTable[cntxt->idx].brkCmd[i];

		if (mdbTable[cntxt->idx].brkVar[i] >= 0)
			for (j = 0; j < p->retc; j++)
				if (mdbTable[cntxt->idx].brkVar[i] == getArg(p, j))
					return mdbTable[cntxt->idx].brkCmd[i];
	}
	return 0;
}

/*
 * Break points can be set on assignment to a specific variable,
 * specific operation, or a instruction line
 */
void
mdbSetBreakRequest(Client cntxt, MalBlkPtr mb, str request, char cmd)
{
	int i;
	str modnme, fcnnme;
	mdbState mdb = mdbTable + cntxt->idx;
	Symbol sym;

	/* set breakpoint on specific line */
	if (*request == '#') {
		i = atoi(request + 1);
		if (i < 0 || i >= mb->stop)
			mnstr_printf(cntxt->fdout, "breakpoint on #%d (<%d) not set\n",
					i, mb->stop);
		else {
			mdb->brkBlock[mdb->brkTop] = mb;
			mdb->brkPc[mdb->brkTop] = i;
			mdb->brkVar[mdb->brkTop] = -1;
			mdb->brkMod[mdb->brkTop] = 0;
			mdb->brkFcn[mdb->brkTop] = 0;
			mdb->brkRequest[mdb->brkTop] = GDKstrdup(request);
			mdb->brkCmd[mdb->brkTop] = cmd;
			if (mdb->brkTop + 1 < MAXBREAKS)
				mdb->brkTop++;
		}
		return;
	}

	/* check for a [module.]function request */
	fcnnme = strchr(request, '.');
	if (fcnnme) {
		modnme = request;
		*fcnnme = 0;
		fcnnme++;
		sym = findSymbol(cntxt->usermodule, modnme, fcnnme);
		mdb->brkBlock[mdb->brkTop] = sym ? sym->def : mb;
		mdb->brkPc[mdb->brkTop] = -1;
		mdb->brkVar[mdb->brkTop] = -1;
		mdb->brkMod[mdb->brkTop] = putName(modnme);
		mdb->brkFcn[mdb->brkTop] = putName(fcnnme);
		fcnnme--;
		*fcnnme = '.';
		mdb->brkRequest[mdb->brkTop] = GDKstrdup(request);
		mdb->brkCmd[mdb->brkTop] = cmd;
		if (mdb->brkTop + 1 < MAXBREAKS)
			mdb->brkTop++;
		return;
	}
	/* the final step is to break on a variable */
	i = findVariable(mb, request);
	if ( i < 0)
		i = findVariable(mb, request+1);
	if (i < 0)
		mnstr_printf(cntxt->fdout, "breakpoint on %s not set\n", request);
	else {
		mdb->brkBlock[mdb->brkTop] = mb;
		mdb->brkPc[mdb->brkTop] = -1;
		mdb->brkVar[mdb->brkTop] = i;
		mdb->brkMod[mdb->brkTop] = 0;
		mdb->brkFcn[mdb->brkTop] = 0;
		mdb->brkRequest[mdb->brkTop] = GDKstrdup(request);
		mdb->brkCmd[mdb->brkTop] = cmd;
		if (mdb->brkTop + 1 < MAXBREAKS)
			mdb->brkTop++;
	}
}

/* A breakpoint should be set once for each combination */
static void
mdbSetBreakpoint(Client cntxt, MalBlkPtr mb, int pc, char cmd)
{
	mdbState mdb = mdbTable + cntxt->idx;
	char buf[20];

	snprintf(buf, 20, "#%d", pc);
	mdb->brkBlock[mdb->brkTop] = mb;
	mdb->brkPc[mdb->brkTop] = pc;
	mdb->brkVar[mdb->brkTop] = -1;
	mdb->brkMod[mdb->brkTop] = 0;
	mdb->brkFcn[mdb->brkTop] = 0;
	mdb->brkRequest[mdb->brkTop] = GDKstrdup(buf);
	mdb->brkCmd[mdb->brkTop] = cmd;
	if (mdb->brkTop + 1 < MAXBREAKS)
		mdb->brkTop++;
}

static void
mdbShowBreakpoints(Client cntxt)
{
	int i;
	mdbState mdb = mdbTable + cntxt->idx;

	for (i = 0; i < mdb->brkTop; i++)
		mnstr_printf(cntxt->fdout, "breakpoint on '%s'\n", mdb->brkRequest[i]);
}

static void
mdbClrBreakpoint(Client cntxt, int pc)
{
	int i, j = 0;
	mdbState mdb = mdbTable + cntxt->idx;

	for (i = 0; i < mdb->brkTop; i++) {
		mdb->brkBlock[j] = mdb->brkBlock[i];
		mdb->brkPc[j] = mdb->brkPc[i];
		mdb->brkVar[j] = mdb->brkVar[i];
		mdb->brkMod[j] = mdb->brkMod[i];
		mdb->brkFcn[j] = mdb->brkFcn[i];
		mdb->brkRequest[j] = mdb->brkRequest[i];
		mdb->brkCmd[j] = mdb->brkCmd[i];
		if (mdb->brkPc[i] != pc)
			j++;
		else {
			GDKfree(mdb->brkRequest[i]);
			mdb->brkRequest[i] = 0;
		}
	}
	mdb->brkTop = j;
}

static void
mdbClrBreakRequest(Client cntxt, str request)
{
	int i, j = 0;
	mdbState mdb = mdbTable + cntxt->idx;

	for (i = 0; i < mdb->brkTop; i++) {
		mdb->brkBlock[j] = mdb->brkBlock[i];
		mdb->brkPc[j] = mdb->brkPc[i];
		mdb->brkVar[j] = mdb->brkVar[i];
		mdb->brkMod[j] = mdb->brkMod[i];
		mdb->brkFcn[j] = mdb->brkFcn[i];
		mdb->brkRequest[j] = mdb->brkRequest[i];
		mdb->brkCmd[j] = mdb->brkCmd[i];
		if (strcmp(mdb->brkRequest[i], request))
			j++;
		else {
			GDKfree(mdb->brkRequest[i]);
			mdb->brkRequest[i] = 0;
		}
	}
	mdb->brkTop = j;
}

/* utility to display instruction and dispose of structure */
static void
printTraceCall(stream *out, MalBlkPtr mb, MalStkPtr stk, int pc, int flags)
{
	str msg;
	InstrPtr p;

	p = getInstrPtr(mb, pc);
	msg = instruction2str(mb, stk, p, flags);
	mnstr_printf(out, "#%s%s\n", (mb->errors != MAL_SUCCEED ? "!" : ""), msg?msg:"failed instruction2str()");
	GDKfree(msg);
}

/* MAL debugger parser
 * The debugger structure is inherited from GDB.
 * The routine mdbCommand is called with p=0 after finishing a mal- function call
 * and before continuing at the next level of invocation.
 * The commands are self-explanatory.
 *
 * The prompt string sent to the user indicates the debugger mode.
 *
 * The history of the optimizers is maintained, which can be localized
 * for inspection.
 */
#define MDBstatus(X) if (cntxt->fdout) \
		mnstr_printf(cntxt->fdout, "#MonetDB Debugger %s\n", (X ? "on" : "off"));

static MalBlkPtr
mdbLocateMalBlk(Client cntxt, MalBlkPtr mb, str b)
{
	MalBlkPtr m = mb;
	char *h = 0;
	int idx = -1;

	skipBlanc(cntxt, b);
	/* start with function in context */
	if (*b == '[') {
		if( !isdigit((unsigned char) *(b+1))){
			m = getMalBlkOptimized(mb, b+1);
			if( m ==0 )
				mnstr_printf(cntxt->fdout,"Integer or optimizer named expected");
			else
				return m;
		} else
			idx = atoi(b + 1);
		if( idx < 0)
			return NULL;
		return getMalBlkHistory(mb, idx);
	} else if (isdigit((unsigned char) *b)) {
		idx = atoi(b);
		if( idx < 0)
			return NULL;
		return getMalBlkHistory(mb, idx);
	} else if (*b != 0) {
		char *fcnname = strchr(b, '.');
		Symbol fsym;
		if (fcnname == NULL)
			return NULL;
		*fcnname = 0;
		if ((h = strchr(fcnname + 1, '['))) {
			*h = 0;
			if( !isdigit((unsigned char) *(h+1))){
				m = getMalBlkOptimized(mb, h+1);
				if( m ==0 )
					mnstr_printf(cntxt->fdout,"Integer or optimizer named expected");
				else
					return m;
			} else
				idx = atoi(h + 1);
			if( idx < 0)
				return NULL;
		}
		fsym = findSymbolInModule(findModule(cntxt->usermodule, putName(b)), fcnname + 1);
		*fcnname = '.';
		if (h)
			*h = '[';
		if (fsym == 0) {
			return NULL;
		}
		m = fsym->def;
		return getMalBlkHistory(m, h ? idx : -1);
	}
	return getMalBlkHistory(mb, -1);
}

static void
printBatProperties(stream *f, VarPtr n, ValPtr v, str props)
{
	if (isaBatType(n->type) && v->val.ival) {
		bat bid = v->val.ival;
		BAT *b[2];
		str res;
		BUN p;

		mnstr_printf(f, "BAT %d %s= ", bid, props);
		if ((res = BATinfo(&b[0],&b[1], bid)) != MAL_SUCCEED) {
			GDKfree(res);
			mnstr_printf(f, "mal.info failed\n");
			return;
		}
		p = BUNfnd(b[0], props);
		if (p != BUN_NONE) {
			BATiter bi = bat_iterator(b[1]);
			mnstr_printf(f, " %s\n", (str) BUNtvar(bi, p));
		} else {
			mnstr_printf(f, " not found\n");
		}
		BBPunfix(b[0]->batCacheid);
		BBPunfix(b[1]->batCacheid);
	}
}


static void
mdbCommand(Client cntxt, MalBlkPtr mb, MalStkPtr stkbase, InstrPtr p, int pc)
{
	int m = 1;
	char *b= 0, *c, lastcmd = 0;
	stream *out = cntxt->fdout;
	char *oldprompt = cntxt->prompt;
	size_t oldpromptlength = cntxt->promptlength;
	MalStkPtr stk = stkbase;
	int first = pc - ( pc == 1);
	int stepsize = 1000;
	char oldcmd[1024] = { 0 };
	str msg = MAL_SUCCEED;

	do {
		if (p != NULL) {
			/* help mclients with fake prompt */
			if (lastcmd != 'l' && lastcmd != 'L') {
				mnstr_printf(out, "mdb>");
				printTraceCall(out, mb, stk, pc, LIST_MAL_CALL);
			}

		}

		if (cntxt->phase[MAL_SCENARIO_READER]) {
retryRead:
			msg = (char *) (*cntxt->phase[MAL_SCENARIO_READER])(cntxt);
			if (msg != MAL_SUCCEED || cntxt->mode == FINISHCLIENT){
				freeException(msg);
				break;
			}
			/* SQL patch, it should only react to Smessages, Xclose requests to be ignored */
			if (strncmp(cntxt->fdin->buf, "Xclose", 6) == 0) {
				cntxt->fdin->pos = cntxt->fdin->len;
				goto retryRead;
			}
		}
		b = CURRENT(cntxt);

		/* terminate the line with zero */
		c = strchr(b, '\n');
		if (c) {
			*c = 0;
			strcpy_len(oldcmd, b, sizeof(oldcmd));
			cntxt->fdin->pos += (c - b) + 1;
		} else
			cntxt->fdin->pos = cntxt->fdin->len;

		skipBlanc(cntxt, b);
		if (*b)
			lastcmd = *b;
		else
			strcpy(b = cntxt->fdin->buf, oldcmd);
		b = oldcmd;
		switch (*b) {
		case 0:
			m = 0;
			break;
		case 'c':
			if (strncmp("check",b,5) == 0){
				Symbol fs;
				int i;
				str fcnnme;

				skipWord(cntxt,b);
				skipBlanc(cntxt, b);
				if( (fcnnme = strchr(b,'.')) != NULL ){
					str modnme = b;

					*fcnnme++  = 0;

					fs = findSymbol(cntxt->usermodule, putName(modnme),putName(fcnnme));
					if (fs == 0) {
						mnstr_printf(out, "#'%s.%s' not found\n", modnme,fcnnme);
						continue;
					}
					for(; fs; fs = fs->peer){
						for(i=0; i< fs->def->stop; i++)
							fs->def->stmt[i]->typechk = TYPE_UNKNOWN;
						msg = chkProgram(cntxt->usermodule, fs->def);
						if( msg != MAL_SUCCEED){
							mnstr_printf(out, "#<modnme>.<fcnnme> contains errors: %s\n", msg);
							freeException(msg);
						}
					}
				} else
					mnstr_printf(out, "#<modnme>.<fcnnme> expected\n");
				break;
			}
			if (strncmp("catch", b, 3) == 0) {
				/* catch the next exception */
				stk->cmd = 'C';
				break;
			}
			stk->cmd = 'c';
			skipWord(cntxt, b);
			m = 0;
			break;
		case 'e':
		{
			/* terminate the execution for ordinary functions only */
			if (strncmp("exit", b, 4) == 0) {
			case 'x':
				if (!(getInstrPtr(mb, 0)->token == FACcall)) {
					stk->cmd = 'x';
					cntxt->prompt = oldprompt;
					cntxt->promptlength = oldpromptlength;
				}
			}
			return;
		}
		case 'q':
		{
			MalStkPtr su;

			/* return from this debugger */
			for (su = stk; su; su = su->up)
				su->cmd = 0;
			cntxt->itrace = 0;
			mnstr_printf(out, "mdb>#EOD\n");
			/* MDBstatus(0); */
			cntxt->prompt = oldprompt;
			cntxt->promptlength = oldpromptlength;
			return;
		}
		case 'f':   /* finish */
		case 'n':   /* next */
		case 's':   /* step */
			if (strncmp("scenarios", b, 9) == 0) {
				showAllScenarios(out);
				continue;
			} else if (strncmp("scenario", b, 3) == 0) {
				showScenarioByName(out, cntxt->scenario);
				continue;
			}
			stk->cmd = *b;
			m = 0;
			break;
		case 'M':	/* dump all module names */
			dumpModules(out);
			break;
		case 'm':   /* display a module */
		{
			str modname, fcnname;
			Module fsym;
			Symbol fs;
			int i;

			skipWord(cntxt, b);
			skipBlanc(cntxt, b);
			if (*b) {
				modname = b;
				fcnname = strchr(b, '.');
				if (fcnname) {
					*fcnname = 0;
					fcnname++;
				}
				fsym = findModule(cntxt->usermodule, putName(modname));

				if (fsym == cntxt->usermodule && strcmp(modname, "user")) {
					mnstr_printf(out, "#module '%s' not found\n", modname);
					continue;
				}
				for (i = 0; i < MAXSCOPE; i++) {
					fs = fsym->space[i];
					while (fs != NULL) {
						printSignature(out, fs, 0);
						fs = fs->peer;
					}
				}
				continue;
			} else {
				Module* list;
				int length;
				int i;
				mnstr_printf(out,"#%s ",cntxt->usermodule->name);
				getModuleList(&list, &length);
				for(i = 0; i < length; i++) {
					mnstr_printf(out, "%s ", list[i]->name);
				}
				freeModuleList(list);
				mnstr_printf(out,"\n");
			}
		}
		break;
		case 'T':   /* debug type resolver for a function call */
			if (strncmp("Trace", b, 5) == 0) {
				char *w;
				skipWord(cntxt, b);
				skipBlanc(cntxt, b);
				if ((w = strchr(b, '\n')))
					*w = 0;
			}
			break;
		case 't':   /* trace a variable toggle */
			if (strncmp("trap", b, 4) == 0) {
				char *w, *mod, *fcn;
				skipWord(cntxt, b);
				skipBlanc(cntxt, b);
				mod = b;
				skipWord(cntxt, b);
				*b = 0;
				fcn = b + 1;
				if ((w = strchr(b + 1, '\n')))
					*w = 0;
				mnstr_printf(out, "#trap %s.%s\n", mod, fcn);
			}
			if (strncmp("trace", b, 5) == 0) {
				char *w;
				skipWord(cntxt, b);
				skipBlanc(cntxt, b);
				if ((w = strchr(b, '\n')))
					*w = 0;
				mdbSetBreakRequest(cntxt, mb, b, 't');
			}
			break;
		case 'v':   /* show the symbol table and bindings */
		case 'V': {
			str modname, fcnname;
			Module fsym;
			Symbol fs;
			int i;

			skipWord(cntxt, b);
			if (*b != 0) {
				modname = b;
				fcnname = strchr(b, '.');
				if (fcnname == NULL) {
					fsym = findModule(cntxt->usermodule, putName(modname));
					if (fsym == 0) {
						mnstr_printf(out, "#%s module not found\n", modname);
						continue;
					}
					for (i = 0; i < MAXSCOPE; i++) {
						fs = fsym->space[i];
						while (fs != NULL) {
							printStack(out, fs->def, 0);
							fs = fs->peer;
						}
					}
					continue;
				}
				*fcnname = 0;
				fcnname++;
				fsym = findModule(cntxt->usermodule, putName(modname));
				if (fsym == 0) {
					mnstr_printf(out, "#%s module not found\n", modname);
					continue;
				}
				/* display the overloaded symbol definition */
				for (i = 0; i < MAXSCOPE; i++) {
					fs = fsym->space[i];
					while (fs != NULL) {
						if (strcmp(fs->name, fcnname) == 0)
							printStack(out, fs->def, 0);
						fs = fs->peer;
					}
				}
			} else
				printStack(out, mb, stk);
			break;
		}
		case 'b':
			if (strncmp(b, "bbp", 3) == 0) {
				int i, limit, inuse = 0;

				skipWord(cntxt, b);
				i = BBPindex(b);
				if (i)
					limit = i + 1;
				else {
					limit = getBBPsize();
					i = 1;
				}
				/* the 'dense' qualification only shows entries with a hard ref */
				/* watchout, you don't want to wait for locks by others */
				mnstr_printf(out, "BBP contains %d entries\n", limit);
				for (; i < limit; i++)
					if ((BBP_lrefs(i) || BBP_refs(i)) && BBP_cache(i)) {
						mnstr_printf(out, "#[%d] %-15s", i, BBP_logical(i));
						if (BBP_cache(i))
							printBATproperties(out, BBP_cache(i));
						if ((*b == 'd' && BBP_refs(i) == 0) || BBP_cache(i) == 0) {
							mnstr_printf(out, "\n");
							continue;
						}
						inuse++;
						if (BATdirty(BBP_cache(i)))
							mnstr_printf(out, " dirty");
						if (*BBP_logical(i) == '.')
							mnstr_printf(out, " zombie ");
						if (BBPstatus(i) & BBPLOADED)
							mnstr_printf(out, " loaded ");
						if (BBPstatus(i) & BBPSWAPPED)
							mnstr_printf(out, " swapped ");
						if (BBPstatus(i) & BBPTMP)
							mnstr_printf(out, " tmp ");
						if (BBPstatus(i) & BBPDELETED)
							mnstr_printf(out, " deleted ");
						if (BBPstatus(i) & BBPEXISTING)
							mnstr_printf(out, " existing ");
						if (BBPstatus(i) & BBPNEW)
							mnstr_printf(out, " new ");
						if (BBPstatus(i) & BBPPERSISTENT)
							mnstr_printf(out, " persistent ");
						mnstr_printf(out, "\n");
					}

				mnstr_printf(out, "#Entries displayed %d\n", inuse);
				continue;
			}
			if (strncmp(b, "breakpoints", 11) == 0) {
				mdbShowBreakpoints(cntxt);
				continue;
			}
			if (strncmp(b, "break", 5) == 0)
				b += 4;
			if (isspace((unsigned char) b[1])) {
				skipWord(cntxt, b);
				if (*b && !isspace((unsigned char) *b) && !isdigit((unsigned char) *b))
					/* set breakpoints by name */
					mdbSetBreakRequest(cntxt, mb, b, 's');
				else if (*b && isdigit((unsigned char) *b))
					/* set breakpoint at instruction */
					mdbSetBreakpoint(cntxt, mb, atoi(b), 's');
				else
					/* set breakpoint at current instruction */
					mdbSetBreakpoint(cntxt, mb, pc, 's');
				continue;
			}
			continue;
		case 'd':
			if (strncmp(b, "debug", 5) == 0) {
				skipWord(cntxt, b);
				GDKsetdebug(atoi(b));
				mnstr_printf(out, "#Set debug mask to %d\n", GDKdebug);
				break;
			}
			if (strncmp(b, "down", 4) == 0) {
				MalStkPtr ref = stk;
				/* find the previous one from the base */
				stk = stkbase;
				while (stk != ref && stk->up && stk->up != ref)
					stk = stk->up;
				mnstr_printf(out, "#%sgo down the stack\n", "#mdb ");
				mb = stk->blk;
				break;
			}
			skipWord(cntxt, b);
			/* get rid of break point */
			if (*b && !isspace((unsigned char) *b) && !isdigit((unsigned char) *b))
				mdbClrBreakRequest(cntxt, b);
			else if (isdigit((unsigned char) *b))
				mdbClrBreakpoint(cntxt, atoi(b));
			else {
				mdbClrBreakpoint(cntxt, pc);
			}
			continue;
		case 'I':
		case 'i':
		{
			int i;
			char *t;

			/* the user wants information about variables */
			if (*b == 'I') {
				skipWord(cntxt, b);
				for (i = 0; i < mb->vtop; i++)
					printBatProperties(out, getVar(mb, i), stk->stk + i, b);
				continue;
			}
			skipWord(cntxt, b);
			t = b;
			skipNonBlanc(cntxt, t);
			*t = 0;
			/* search the symbol */
			i = findVariable(mb, b);
			if (i < 0) {
				/* could be the name of a BAT */
				i = BBPindex(b);
				if (i != 0)
					printBatDetails(out, i);
				else
					mnstr_printf(out, "#%s Symbol not found\n", "#mdb ");
			} else {
				printBatInfo(out, getVar(mb, i), stk->stk + i);
			}
			continue;
		}
		case 'P':
		case 'p':
		{
			BUN size = 0, first = 0;
			int i;
			char *t;
			char upper = *b;

			skipWord(cntxt, b);
			t = b;
			skipNonBlanc(cntxt, t);
			*t = 0;
			/* you can identify a start and length */
			t++;
			skipBlanc(cntxt, t);
			if (isdigit((unsigned char) *t)) {
				size = (BUN) atol(t);
				skipWord(cntxt, t);
				if (isdigit((unsigned char) *t))
					first = (BUN) atol(t);
			}
			/* search the symbol */
			i = findVariable(mb, b);
			if (i < 0) {
				// deal with temporary
				if( *b == 'X' ) b++;
				i = findVariable(mb, b);
			}
			if (i < 0) {
				mnstr_printf(out, "#%s Symbol not found\n", b);
				continue;
			}
			if (isaBatType(getVarType(mb, i)) && upper == 'p') {
				printStackHdr(out, mb, stk->stk + i, i);
				printBATelm(out, stk->stk[i].val.bval, size, first);
			} else
				printStackElm(out, mb, stk->stk + i, i, size, first);
			continue;
		}
		case 'u':
			if (stk->up == NULL)
				break;
			mnstr_printf(out, "#%s go up the stack\n", "#mdb ");
			stk = stk->up;
			mb = stk->blk;
			printCall(cntxt, mb, stk, pc);
			continue;
		case 'w':
		{
			mdbBacktrace(cntxt, stk, pc);
			continue;
		}
/*
 * While debugging it should be possible to inspect the symbol
 * table using the 'module.function' name. The default is to list all
 * signatures satisfying the pattern.
 */
		case 'L':
		case 'l':   /* list the current MAL block or module */
		{
			Symbol fs;
			int lstng;

			lstng = LIST_MAL_NAME;
			if(*b == 'L')
				lstng = LIST_MAL_NAME | LIST_MAL_VALUE | LIST_MAL_TYPE | LIST_MAL_PROPS;
			skipWord(cntxt, b);
			skipBlanc(cntxt, b);
			if (*b != 0) {
				/* debug the current block */
				MalBlkPtr m = mdbLocateMalBlk(cntxt, mb, b);

				if ( m == 0)
					m = mb;
				if ( m ){
					const char *nme = getFunctionId(mb->stmt[0]);
					str s = strstr(b, nme);
					if( s ){
						b = s + strlen(nme);
						skipBlanc(cntxt,b);
					}
				}
				if (isdigit((unsigned char) *b) || *b == '-' || *b == '+')
					goto partial;

				/* inspect another function */
				if( strchr(b,'.') ){
					str modnme = b;
					str fcnnme;
					fcnnme = strchr(b,'.');
					*fcnnme++  = 0;
					b = fcnnme;
					skipNonBlanc(cntxt, b);
					if ( b)
						*b++  = 0;

					fs = findSymbol(cntxt->usermodule, putName(modnme),putName(fcnnme));
					if (fs == 0) {
						mnstr_printf(out, "#'%s' not found\n", modnme);
						continue;
					}
					for(; fs; fs = fs->peer)
					if( strcmp(fcnnme, fs->name)==0) {
						if( lstng == LIST_MAL_NAME)
							printFunction(out, fs->def, 0, lstng);
						else
							debugFunction(out, fs->def, 0, lstng, 0,mb->stop);
					}
					continue;
				}
				if (m){
					if( lstng == LIST_MAL_NAME)
						printFunction(out, m, 0, lstng);
					else
						debugFunction(out, m, 0, lstng, 0,m->stop);
				}
			} else {
/*
 * Listing the program starts at the pc last given.
 * Repeated use of the list command moves you up and down the program
 */
partial:
				if (isdigit((unsigned char) *b)) {
					first = (int) atoi(b);
					skipWord(cntxt, b);
					skipBlanc(cntxt, b);
				}
				if (*b == '-') {
					stepsize = (int) atoi(b + 1);
					first -= stepsize++;
				} else if (*b == '+')
					stepsize = (int) atoi(b + 1);
				else if (atoi(b))
					stepsize = (int) atoi(b);
				*b = 0;
				if (stepsize < 0)
					first -= stepsize;
				if( first > mb->stop ) {
					mnstr_printf(out, "#line %d out of range (<=%d)\n", first, mb->stop);
					first = pc;
				} else {
					debugFunction(out, mb, 0, lstng, first, stepsize);
					first = first + stepsize > mb->stop ? first : first + stepsize;
				}
			}
			continue;
		}
		case 'h':
			mdbHelp(out);
			continue;
		case 'o':
		case 'O':   /* optimizer and scheduler steps */
		{
			MalBlkPtr mdot = mb;
			skipWord(cntxt, b);
			skipBlanc(cntxt, b);
			if (*b) {
				mdot = mdbLocateMalBlk(cntxt, mb, b);
				if (mdot != NULL)
					showMalBlkHistory(out, mdot);
				else
					mnstr_printf(out, "#'%s' not found\n", b);
			} else
				showMalBlkHistory(out, mb);
			break;
		}
		case 'r':   /* reset program counter */
			mnstr_printf(out, "#%s restart with current stack\n", "#mdb ");
			stk->cmd = 'r';
			break;
		default:
			mnstr_printf(out, "#%s debugger command expected\n", "#mdb ");
			mdbHelp(out);
		}
	} while (m);
	cntxt->prompt = oldprompt;
	cntxt->promptlength = oldpromptlength;
}

static str
mdbTrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int pc = getPC(mb,p);

	mnstr_printf(cntxt->fdout, "#trapped %s.%s[%d]\n",
			getModuleId(mb->stmt[0]), getFunctionId(mb->stmt[0]), pc);
	printInstruction(cntxt->fdout, mb, stk, p, LIST_MAL_DEBUG);
	cntxt->itrace = 'W';
	return MAL_SUCCEED;
}

void
mdbStep(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int pc)
{
	InstrPtr p;
	char ch;
	stream *out = cntxt->fdout;

	/* mdbSanityCheck(cntxt, mb, stk, pc); expensive */
	/* process should sleep */
	if (cntxt->itrace == 'S') {
		MdbState state;
		state.mb = mb;
		state.stk = stk;
		state.p = getInstrPtr(mb, pc);
		state.pc = pc;
		mnstr_printf(cntxt->fdout, "#Process %d put to sleep\n", (int) (cntxt - mal_clients));
		cntxt->itrace = 'W';
		mdbTrap(cntxt, mb, stk, state.p);
		while (cntxt->itrace == 'W')
			MT_sleep_ms(300);
		mnstr_printf(cntxt->fdout, "#Process %d woke up\n", (int) (cntxt - mal_clients));
		return;
	}
	if (stk->cmd == 0)
		stk->cmd = 'n';
	p = getInstrPtr(mb, pc);
	switch (stk->cmd) {
	case 'c':
		ch = isBreakpoint(cntxt, mb, p, pc);
		if (ch == 't') {
			/* help mclients with fake prompt */
			mnstr_printf(out, "mdb>");
			printTraceCall(out, mb, stk, pc, LIST_MAL_CALL);
		} else if (ch)
			mdbCommand(cntxt, mb, stk, p, pc);
		break;
	case 's':
	case 'n':
		mdbCommand(cntxt, mb, stk, p, pc);
		break;
	case 't':
		printTraceCall(out, mb, stk, pc, LIST_MAL_CALL);
		break;
	}
	if (mb->errors != MAL_SUCCEED) {
		MalStkPtr su;

		/* return from this debugger */
		for (su = stk; su; su = su->up)
			su->cmd = 0;
		mnstr_printf(out, "mdb>#EOD\n");
		stk->cmd = 'x'; /* will force a graceful termination */
	}
}

/*
 * It would come in handy if at any time you could activate
 * the debugger on a specific function. This calls for the
 * creation of a minimal execution environment first.
 */
str
runMALDebugger(Client cntxt, MalBlkPtr mb)
{
	str oldprompt= cntxt->prompt;
	int oldtrace = cntxt->itrace;
	str msg;

	cntxt->itrace = 'n';

	msg = runMAL(cntxt, mb, 0, 0);

	cntxt->prompt =oldprompt;
	cntxt->itrace = oldtrace;
	mnstr_printf(cntxt->fdout, "mdb>#EOD\n");
	removeMalBlkHistory(mb);
	return msg;
}

#else
str runMALDebugger(Client cntxt, MalBlkPtr mb)
{
	(void)cntxt; (void)mb;
	return NULL;
}
void mdbSetBreakRequest(Client cntxt, MalBlkPtr mb, str request, char cmd) {
	(void)cntxt; (void)mb; (void)request; (void)cmd;
}
#endif
