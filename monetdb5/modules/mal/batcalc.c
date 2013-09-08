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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "math.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define batcalc_export extern __declspec(dllimport)
#else
#define batcalc_export extern __declspec(dllexport)
#endif
#else
#define batcalc_export extern
#endif

static str
mythrow(enum malexception type, const char *fcn, const char *msg)
{
	char *errbuf = GDKerrbuf;
	char *s;

	if (errbuf && *errbuf) {
		if (strncmp(errbuf, "!ERROR: ", 8) == 0)
			errbuf += 8;
		if (strchr(errbuf, '!') == errbuf + 5) {
			s = createException(type, fcn, "%s", errbuf);
		} else if ((s = strchr(errbuf, ':')) != NULL && s[1] == ' ') {
			s = createException(type, fcn, "%s", s + 2);
		} else {
			s = createException(type, fcn, "%s", errbuf);
		}
		*GDKerrbuf = 0;
		return s;
	}
	return createException(type, fcn, "%s", msg);
}

static str
CMDbatUNARY(MalStkPtr stk, InstrPtr pci,
			BAT *(*batfunc)(BAT *, BAT *), const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL, *t, *map;

	bid = (bat *) getArgReference(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bat *sid = (bat *) getArgReference(stk, pci, 2);
		if (*sid && (s = BATdescriptor(*sid)) == NULL) {
			BBPreleaseref(b->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
	}

	if (!BAThdense(b)) {
		map = BATmark(b, 0);
		t = BATmirror(BATmark(BATmirror(b), 0));
		BBPreleaseref(b->batCacheid);
		b = t;
		assert(s == NULL);
	} else {
		map = NULL;
	}
	bn = (*batfunc)(b, s);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL) {
		if (map)
			BBPreleaseref(map->batCacheid);
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	if (map) {
		t = BATleftfetchjoin(map, bn, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = t;
		BBPreleaseref(map->batCacheid);
	}
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatUNARY1(MalStkPtr stk, InstrPtr pci, int abort_on_error,
			 BAT *(*batfunc)(BAT *, BAT *, int), const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL, *t, *map;

	bid = (bat *) getArgReference(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bat *sid = (bat *) getArgReference(stk, pci, 2);
		if (*sid && (s = BATdescriptor(*sid)) == NULL) {
			BBPreleaseref(b->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
	}

	if (!BAThdense(b)) {
		map = BATmark(b, 0);
		t = BATmirror(BATmark(BATmirror(b), 0));
		BBPreleaseref(b->batCacheid);
		b = t;
		assert(s == NULL);
	} else {
		map = NULL;
	}
	bn = (*batfunc)(b, s, abort_on_error);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL) {
		if (map)
			BBPreleaseref(map->batCacheid);
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	if (map) {
		t = BATleftfetchjoin(map, bn, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = t;
		BBPreleaseref(map->batCacheid);
	}
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

batcalc_export str CMDbatISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalciszero, "batcalc.iszero");
}

batcalc_export str CMDbatISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcisnil, "batcalc.isnil");
}

batcalc_export str CMDbatNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcnot, "batcalc.not");
}

batcalc_export str CMDbatABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcabsolute, "batcalc.abs");
}

batcalc_export str CMDbatINCR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatINCR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY1(stk, pci, 1, BATcalcincr, "batcalc.incr");
}

batcalc_export str CMDbatDECR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDECR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY1(stk, pci, 1, BATcalcdecr, "batcalc.decr");
}

batcalc_export str CMDbatNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcnegate, "batcalc.neg");
}

batcalc_export str CMDbatSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcsign, "batcalc.sign");
}

static int
calctype(int tp1, int tp2)
{
	int tp1s = ATOMstorage(tp1);
	int tp2s = ATOMstorage(tp2);
	if (tp1s == TYPE_str && tp2s == TYPE_str)
		return TYPE_str;
	if (tp1s < TYPE_flt && tp2s < TYPE_flt) {
		if (tp1s > tp2s)
			return tp1;
		if (tp1s < tp2s)
			return tp2;
		return MAX(tp1, tp2);
	}
	if (tp1s == TYPE_dbl || tp2s == TYPE_dbl)
		return TYPE_dbl;
	if (tp1s == TYPE_flt || tp2s == TYPE_flt)
		return TYPE_flt;
#ifdef HAVE_HGE
	if (tp1s == TYPE_hge || tp2s == TYPE_hge)
		return TYPE_hge;
#endif
	return TYPE_lng;
}

static int
calctypeenlarge(int tp1, int tp2)
{
	tp1 = calctype(tp1, tp2);
	switch (tp1) {
	case TYPE_bte:
		return TYPE_sht;
	case TYPE_sht:
		return TYPE_int;
	case TYPE_int:
#if SIZEOF_WRD == SIZEOF_INT
	case TYPE_wrd:
#endif
		return TYPE_lng;
#ifdef HAVE_HGE
#if SIZEOF_WRD == SIZEOF_LNG
	case TYPE_wrd:
#endif
	case TYPE_lng:
		return TYPE_hge;
#endif
	case TYPE_flt:
		return TYPE_dbl;
	default:
		/* we shouldn't get here */
		return tp1;
	}
}

static int
calcdivtype(int tp1, int tp2)
{
	/* if right hand side is floating point, the result is floating
	 * point, otherwise the result has the type of the left hand
	 * side */
	tp1 = ATOMstorage(tp1);
	tp2 = ATOMstorage(tp2);
	if (tp1 == TYPE_dbl || tp2 == TYPE_dbl)
		return TYPE_dbl;
	if (tp1 == TYPE_flt || tp2 == TYPE_flt)
		return TYPE_flt;
	return tp1;
}

#if 0
static int
calcdivtypeflt(int tp1, int tp2)
{
	(void) tp1;
	(void) tp2;
	return TYPE_flt;
}

static int
calcdivtypedbl(int tp1, int tp2)
{
	(void) tp1;
	(void) tp2;
	return TYPE_dbl;
}
#endif

static int
calcmodtype(int tp1, int tp2)
{
	tp1 = ATOMstorage(tp1);
	tp2 = ATOMstorage(tp2);
	assert(tp1 > 0 && tp1 < TYPE_str && tp1 != TYPE_bat && tp1 != TYPE_ptr);
	assert(tp2 > 0 && tp2 < TYPE_str && tp2 != TYPE_bat && tp2 != TYPE_ptr);
	if (tp1 == TYPE_dbl || tp2 == TYPE_dbl)
		return TYPE_dbl;
	if (tp1 == TYPE_flt || tp2 == TYPE_flt)
		return TYPE_flt;
	return MIN(tp1, tp2);
}

static str
CMDbatBINARY2(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, int, int),
			  BAT *(batfunc1)(BAT *, const ValRecord *, BAT *, int, int),
			  BAT *(batfunc2)(const ValRecord *, BAT *, BAT *, int, int),
			  int (*typefunc)(int, int),
			  int abort_on_error, const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL, *t, *map;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	if (pci->argc == 4) {
		bat *sid = (bat *) getArgReference(stk, pci, 3);
		if (*sid && (s = BATdescriptor(*sid)) == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		BAT *b2 = NULL;
		bid = (bat *) getArgReference(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPreleaseref(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (tp2 == TYPE_bat || isaBatType(tp2)) {
			bid = (bat *) getArgReference(stk, pci, 2);
			b2 = BATdescriptor(*bid);
			if (b2 == NULL) {
				BBPreleaseref(b->batCacheid);
				if (s)
					BBPreleaseref(s->batCacheid);
				throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
			}
		}
		if (!BAThdense(b) || (b2 != NULL && !BAThdense(b2))) {
			map = BATmark(b, 0); /* [head,dense] */
			t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
			BBPreleaseref(b->batCacheid);
			b = t;
			if (b2) {
				t = BATmirror(BATmark(BATmirror(b2), 0)); /* [dense,tail] */
				BBPreleaseref(b2->batCacheid);
				b2 = t;
			}
			assert(s == NULL);
		} else {
			map = NULL;
		}
		if (b2) {
			bn = (*batfunc)(b, b2, s, (*typefunc)(b->T->type, b2->T->type),
							abort_on_error);
			BBPreleaseref(b2->batCacheid);
		} else {
			bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)], s,
							 (*typefunc)(b->T->type, tp2), abort_on_error);
		}
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(tp2 == TYPE_bat || isaBatType(tp2));
		bid = (bat *) getArgReference(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPreleaseref(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (!BAThdense(b)) {
			map = BATmark(b, 0); /* [head,dense] */
			t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
			BBPreleaseref(b->batCacheid);
			b = t;
			assert(s == NULL);
		} else {
			map = NULL;
		}
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b, s,
						 (*typefunc)(tp1, b->T->type), abort_on_error);
	}
	BBPreleaseref(b->batCacheid);
	if (bn == NULL) {
		if (map)
			BBPreleaseref(map->batCacheid);
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	if (map) {
		t = BATleftfetchjoin(map, bn, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = t;
		BBPreleaseref(map->batCacheid);
	}
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY1(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, int),
			  BAT *(*batfunc1)(BAT *, const ValRecord *, BAT *, int),
			  BAT *(*batfunc2)(const ValRecord *, BAT *, BAT *, int),
			  int abort_on_error,
			  const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL, *t, *map;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	if (pci->argc == 4) {
		bat *sid = (bat *) getArgReference(stk, pci, 3);
		if (*sid && (s = BATdescriptor(*sid)) == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		BAT *b2 = NULL;
		bid = (bat *) getArgReference(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPreleaseref(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (tp2 == TYPE_bat || isaBatType(tp2)) {
			bid = (bat *) getArgReference(stk, pci, 2);
			b2 = BATdescriptor(*bid);
			if (b2 == NULL) {
				BBPreleaseref(b->batCacheid);
				if (s)
					BBPreleaseref(s->batCacheid);
				throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
			}
		}
		if (!BAThdense(b) || (b2 != NULL && !BAThdense(b2))) {
			map = BATmark(b, 0); /* [head,dense] */
			t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
			BBPreleaseref(b->batCacheid);
			b = t;
			if (b2) {
				t = BATmirror(BATmark(BATmirror(b2), 0)); /* [dense,tail] */
				BBPreleaseref(b2->batCacheid);
				b2 = t;
			}
			assert(s == NULL);
		} else {
			map = NULL;
		}
		if (b2) {
			bn = (*batfunc)(b, b2, s, abort_on_error);
			BBPreleaseref(b2->batCacheid);
		} else {
			bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)], s, abort_on_error);
		}
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(tp2 == TYPE_bat || isaBatType(tp2));
		bid = (bat *) getArgReference(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPreleaseref(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (!BAThdense(b)) {
			map = BATmark(b, 0); /* [head,dense] */
			t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
			BBPreleaseref(b->batCacheid);
			b = t;
			assert(s == NULL);
		} else {
			map = NULL;
		}
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b, s, abort_on_error);
	}
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL) {
		if (map)
			BBPreleaseref(map->batCacheid);
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	if (map) {
		t = BATleftfetchjoin(map, bn, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = t;
		BBPreleaseref(map->batCacheid);
	}
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY0(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *),
			  BAT *(*batfunc1)(BAT *, const ValRecord *, BAT *),
			  BAT *(*batfunc2)(const ValRecord *, BAT *, BAT *),
			  const char *malfunc)
{
	bat *bid;
	BAT *bn, *b, *s = NULL, *t, *map;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	if (pci->argc == 4) {
		bat *sid = (bat *) getArgReference(stk, pci, 3);
		if (*sid && (s = BATdescriptor(*sid)) == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		BAT *b2 = NULL;
		bid = (bat *) getArgReference(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPreleaseref(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (tp2 == TYPE_bat || isaBatType(tp2)) {
			bid = (bat *) getArgReference(stk, pci, 2);
			b2 = BATdescriptor(*bid);
			if (b2 == NULL) {
				BBPreleaseref(b->batCacheid);
				if (s)
					BBPreleaseref(s->batCacheid);
				throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
			}
		}
		if (!BAThdense(b) || (b2 != NULL && !BAThdense(b2))) {
			map = BATmark(b, 0); /* [head,dense] */
			t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
			BBPreleaseref(b->batCacheid);
			b = t;
			if (b2) {
				t = BATmirror(BATmark(BATmirror(b2), 0)); /* [dense,tail] */
				BBPreleaseref(b2->batCacheid);
				b2 = t;
			}
			assert(s == NULL);
		} else {
			map = NULL;
		}
		if (b2) {
			bn = (*batfunc)(b, b2, s);
			BBPreleaseref(b2->batCacheid);
		} else {
			bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)], s);
		}
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(tp2 == TYPE_bat || isaBatType(tp2));
		bid = (bat *) getArgReference(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL) {
			if (s)
				BBPreleaseref(s->batCacheid);
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		}
		if (!BAThdense(b)) {
			map = BATmark(b, 0); /* [head,dense] */
			t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
			BBPreleaseref(b->batCacheid);
			b = t;
		} else {
			map = NULL;
		}
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b, s);
	}
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL) {
		if (map)
			BBPreleaseref(map->batCacheid);
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	if (map) {
		t = BATleftfetchjoin(map, bn, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = t;
		BBPreleaseref(map->batCacheid);
	}
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

batcalc_export str CMDbatADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctype, 0, "batcalc.add_noerror");
}

batcalc_export str CMDbatADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctype, 1, "batcalc.+");
}

batcalc_export str CMDbatADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctypeenlarge, 1, "batcalc.add_enlarge");
}

batcalc_export str CMDbatSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctype, 0, "batcalc.sub_noerror");
}

batcalc_export str CMDbatSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctype, 1, "batcalc.-");
}

batcalc_export str CMDbatSUBenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctypeenlarge, 1, "batcalc.sub_enlarge");
}

batcalc_export str CMDbatMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctype, 0, "batcalc.mul_noerror");
}

batcalc_export str CMDbatMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctype, 1, "batcalc.*");
}

batcalc_export str CMDbatMULenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctypeenlarge, 1, "batcalc.mul_enlarge");
}

batcalc_export str CMDbatDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 1, "batcalc./");
}

#if 0
batcalc_export str CMDbatDIVflt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVflt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 0, "batcalc.div_fltnoerror");
}

batcalc_export str CMDbatDIVfltsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVfltsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 1, "batcalc.div_flt");
}

batcalc_export str CMDbatDIVdbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVdbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtypedbl, 0, "batcalc.div_dblnoerror");
}

batcalc_export str CMDbatDIVdblsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVdblsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtypedbl, 1, "batcalc.div_dbl");
}
#endif

batcalc_export str CMDbatMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcmod, BATcalcmodcst, BATcalccstmod,
						 calcmodtype, 0, "batcalc.mod_noerror");
}

batcalc_export str CMDbatMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2(stk, pci, BATcalcmod, BATcalcmodcst, BATcalccstmod,
						 calcmodtype, 1, "batcalc.%");
}

batcalc_export str CMDbatXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcxor, BATcalcxorcst, BATcalccstxor,
						 "batcalc.xor");
}

batcalc_export str CMDbatOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcor, BATcalcorcst, BATcalccstor,
						 "batcalc.or");
}

batcalc_export str CMDbatAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcand, BATcalcandcst, BATcalccstand,
						 "batcalc.and");
}

batcalc_export str CMDbatLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalclsh, BATcalclshcst, BATcalccstlsh, 0,
						 "batcalc.lsh_noerror");
}

batcalc_export str CMDbatLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalclsh, BATcalclshcst, BATcalccstlsh, 1,
						 "batcalc.<<");
}

batcalc_export str CMDbatRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcrsh, BATcalcrshcst, BATcalccstrsh, 0,
						 "batcalc.rsh_noerror");
}

batcalc_export str CMDbatRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcrsh, BATcalcrshcst, BATcalccstrsh, 1,
						 "batcalc.>>");
}

batcalc_export str CMDbatLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalclt, BATcalcltcst, BATcalccstlt,
						 "batcalc.<");
}

batcalc_export str CMDbatLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcle, BATcalclecst, BATcalccstle,
						 "batcalc.<=");
}

batcalc_export str CMDbatGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcgt, BATcalcgtcst, BATcalccstgt,
						 "batcalc.>");
}

batcalc_export str CMDbatGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcge, BATcalcgecst, BATcalccstge,
						 "batcalc.>=");
}

batcalc_export str CMDbatEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalceq, BATcalceqcst, BATcalccsteq,
						 "batcalc.==");
}

batcalc_export str CMDbatNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcne, BATcalcnecst, BATcalccstne,
						 "batcalc.!=");
}

batcalc_export str CMDbatCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalccmp, BATcalccmpcst, BATcalccstcmp,
						 "batcalc.cmp");
}

batcalc_export str CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *bid;
	BAT *bn, *b, *lo = NULL, *hi = NULL, *s = NULL, *t, *map;
	int tp1, tp2, tp3;

	(void) cntxt;
	(void) mb;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	tp3 = stk->stk[getArg(pci, 3)].vtype;
	if (pci->argc == 5) {
		bat *sid = (bat *) getArgReference(stk, pci, 4);
		if (*sid && (s = BATdescriptor(*sid)) == NULL)
			throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
	}

	if (tp1 != TYPE_bat && !isaBatType(tp1))
		throw(MAL, "batcalc.between", ILLEGAL_ARGUMENT);
	bid = (bat *) getArgReference(stk, pci, 1);
	b = BATdescriptor(*bid);
	if (b == NULL)
		throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = (bat *) getArgReference(stk, pci, 2);
		lo = BATdescriptor(*bid);
		if (lo == NULL) {
			BBPreleaseref(b->batCacheid);
			if (s)
				BBPreleaseref(s->batCacheid);
			throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
		}
	}
	if (tp3 == TYPE_bat || isaBatType(tp3)) {
		bid = (bat *) getArgReference(stk, pci, 3);
		hi = BATdescriptor(*bid);
		if (hi == NULL) {
			BBPreleaseref(b->batCacheid);
			if (lo)
				BBPreleaseref(lo->batCacheid);
			if (s)
				BBPreleaseref(s->batCacheid);
			throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
		}
	}
	if (!BAThdense(b) ||
		(lo != NULL && !BAThdense(lo)) ||
		(hi != NULL && !BAThdense(hi))) {
		map = BATmark(b, 0); /* [head,dense] */
		t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
		BBPreleaseref(b->batCacheid);
		b = t;
		if (lo) {
			t = BATmirror(BATmark(BATmirror(lo), 0)); /* [dense,tail] */
			BBPreleaseref(lo->batCacheid);
			lo = t;
		}
		if (hi) {
			t = BATmirror(BATmark(BATmirror(hi), 0)); /* [dense,tail] */
			BBPreleaseref(hi->batCacheid);
			hi = t;
		}
		assert(s == NULL);
	} else {
		map = NULL;
	}
	if (lo == NULL) {
		if (hi == NULL) {
			bn = BATcalcbetweencstcst(b, &stk->stk[getArg(pci, 2)],
									  &stk->stk[getArg(pci, 3)], s);
		} else {
			bn = BATcalcbetweencstbat(b, &stk->stk[getArg(pci, 2)], hi, s);
		}
	} else {
		if (hi == NULL) {
			bn = BATcalcbetweenbatcst(b, lo, &stk->stk[getArg(pci, 3)], s);
		} else {
			bn = BATcalcbetween(b, lo, hi, s);
		}
	}
	BBPreleaseref(b->batCacheid);
	if (lo)
		BBPreleaseref(lo->batCacheid);
	if (hi)
		BBPreleaseref(hi->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL) {
		if (map)
			BBPreleaseref(map->batCacheid);
		return mythrow(MAL, "batcalc.between", OPERATION_FAILED);
	}
	if (map) {
		t = BATleftfetchjoin(map, bn, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = t;
		BBPreleaseref(map->batCacheid);
	}
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

batcalc_export str CMDcalcavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDcalcavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	dbl avg;
	BUN vals;
	bat *bid;
	BAT *b, *s = NULL, *t;
	int ret;

	(void) cntxt;
	(void) mb;

	bid = (bat *) getArgReference(stk, pci, pci->retc + 0);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.avg", RUNTIME_OBJECT_MISSING);
	if (pci->retc == pci->retc + 2) {
		bat *sid = (bat *) getArgReference(stk, pci, pci->retc + 1);
		if (*sid && (s = BATdescriptor(*sid)) == NULL) {
			BBPreleaseref(b->batCacheid);
			throw(MAL, "aggr.avg", RUNTIME_OBJECT_MISSING);
		}
	}
	if (!BAThdense(b)) {
		t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
		BBPreleaseref(b->batCacheid);
		b = t;
		assert(s == NULL);
	}
	ret = BATcalcavg(b, s, &avg, &vals);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (ret == GDK_FAIL)
		return mythrow(MAL, "aggr.avg", OPERATION_FAILED);
	* (dbl *) getArgReference(stk, pci, 0) = avg;
	if (pci->retc == 2)
		* (lng *) getArgReference(stk, pci, 1) = vals;
	return MAL_SUCCEED;
}

static str
CMDconvertbat(MalStkPtr stk, InstrPtr pci, int tp, int abort_on_error)
{
	bat *bid;
	BAT *b, *bn, *s = NULL, *t, *map;

	bid = (bat *) getArgReference(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.convert", RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bat *sid = (bat *) getArgReference(stk, pci, 2);
		if (*sid && (s = BATdescriptor(*sid)) == NULL) {
			BBPreleaseref(b->batCacheid);
			throw(MAL, "batcalc.convert", RUNTIME_OBJECT_MISSING);
		}
	}

	if (!BAThdense(b)) {
		map = BATmark(b, 0); /* [head,dense] */
		t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
		BBPreleaseref(b->batCacheid);
		b = t;
		assert(s == NULL);
	} else {
		map = NULL;
	}
	bn = BATconvert(b, s, tp, abort_on_error);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (bn == NULL) {
		char buf[20];
		snprintf(buf, sizeof(buf), "batcalc.%s", ATOMname(tp));
		if (map)
			BBPreleaseref(map->batCacheid);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	if (map) {
		t = BATleftfetchjoin(map, bn, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = t;
		BBPreleaseref(map->batCacheid);
	}
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

batcalc_export str CMDconvert_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bit, 0);
}

str
CMDconvertsignal_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bit, 1);
}

batcalc_export str CMDconvert_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bte, 0);
}

str
CMDconvertsignal_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_bte, 1);
}

batcalc_export str CMDconvert_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_sht, 0);
}

str
CMDconvertsignal_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_sht, 1);
}

batcalc_export str CMDconvert_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_int, 0);
}

str
CMDconvertsignal_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_int, 1);
}

batcalc_export str CMDconvert_wrd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_wrd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_wrd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_wrd, 0);
}

str
CMDconvertsignal_wrd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_wrd, 1);
}

batcalc_export str CMDconvert_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_lng, 0);
}

str
CMDconvertsignal_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_lng, 1);
}

#ifdef HAVE_HGE
batcalc_export str CMDconvert_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_hge, 0);
}

str
CMDconvertsignal_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_hge, 1);
}
#endif

batcalc_export str CMDconvert_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_flt, 0);
}

str
CMDconvertsignal_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_flt, 1);
}

batcalc_export str CMDconvert_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_dbl, 0);
}

str
CMDconvertsignal_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_dbl, 1);
}

batcalc_export str CMDconvert_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_oid, 0);
}

str
CMDconvertsignal_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_oid, 1);
}

batcalc_export str CMDconvert_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
batcalc_export str CMDconvertsignal_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDconvert_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_str, 0);
}

str
CMDconvertsignal_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDconvertbat(stk, pci, TYPE_str, 1);
}

batcalc_export str CMDifthen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDifthen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b, *b1, *b2, *bn, *t, *map;
	int tp1, tp2;
	bat *ret;

	(void) cntxt;
	(void) mb;

	ret = (bat *) getArgReference(stk, pci, 0);
	b = BATdescriptor(* (bat *) getArgReference(stk, pci, 1));
	if (b == NULL)
		throw(MAL, "batcalc.ifthen(else)", RUNTIME_OBJECT_MISSING);
	if (!BAThdense(b)) {
		map = BATmark(b, 0); /* [head,dense] */
		t = BATmirror(BATmark(BATmirror(b), 0)); /* [dense,tail] */
		BBPreleaseref(b->batCacheid);
		b = t;
	} else {
		map = NULL;
	}

	tp1 = stk->stk[getArg(pci, 2)].vtype;
	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		b1 = BATdescriptor(* (bat *) getArgReference(stk, pci, 2));
		if (b1 == NULL) {
			BBPreleaseref(b->batCacheid);
			if (map)
				BBPreleaseref(map->batCacheid);
			throw(MAL, "batcalc.ifthen(else)", RUNTIME_OBJECT_MISSING);
		}
		if (!BAThdense(b1)) {
			/* we ignore the head column of b1 */
			t = BATmirror(BATmark(BATmirror(b1), 0)); /* [dense,tail] */
			BBPreleaseref(b1->batCacheid);
			b1 = t;
		}
		if (pci->argc == 4) {
			tp2 = stk->stk[getArg(pci, 3)].vtype;
			if (tp2 == TYPE_bat || isaBatType(tp2)) {
				b2 = BATdescriptor(* (bat *) getArgReference(stk, pci, 3));
				if (b2 == NULL) {
					BBPreleaseref(b->batCacheid);
					BBPreleaseref(b1->batCacheid);
					if (map)
						BBPreleaseref(map->batCacheid);
					throw(MAL, "batcalc.ifthen(else)", RUNTIME_OBJECT_MISSING);
				}
				if (!BAThdense(b2)) {
					/* we ignore the head column of b2 */
					t = BATmirror(BATmark(BATmirror(b2), 0)); /* [dense,tail] */
					BBPreleaseref(b2->batCacheid);
					b2 = t;
				}
				bn = BATcalcifthenelse(b, b1, b2);
				BBPreleaseref(b2->batCacheid);
			} else {
				bn = BATcalcifthenelsecst(b, b1, &stk->stk[getArg(pci, 3)]);
			}
		} else {
			bn = BATcalcifthenelse(b, b1, NULL);
		}
		BBPreleaseref(b1->batCacheid);
	} else {
		if (pci->argc == 4) {
			tp2 = stk->stk[getArg(pci, 3)].vtype;
			if (tp2 == TYPE_bat || isaBatType(tp2)) {
				b2 = BATdescriptor(* (bat *) getArgReference(stk, pci, 3));
				if (b2 == NULL) {
					BBPreleaseref(b->batCacheid);
					if (map)
						BBPreleaseref(map->batCacheid);
					throw(MAL, "batcalc.ifthen(else)", RUNTIME_OBJECT_MISSING);
				}
				if (!BAThdense(b2)) {
					/* we ignore the head column of b2 */
					t = BATmirror(BATmark(BATmirror(b2), 0)); /* [dense,tail] */
					BBPreleaseref(b2->batCacheid);
					b2 = t;
				}
				bn = BATcalcifthencstelse(b, &stk->stk[getArg(pci, 2)], b2);
				BBPreleaseref(b2->batCacheid);
			} else {
				bn = BATcalcifthencstelsecst(b, &stk->stk[getArg(pci, 2)], &stk->stk[getArg(pci, 3)]);
			}
		} else {
			bn = BATcalcifthencstelse(b, &stk->stk[getArg(pci, 2)], NULL);
		}
	}
	BBPreleaseref(b->batCacheid);
	if (bn == NULL) {
		if (map)
			BBPreleaseref(map->batCacheid);
		return mythrow(MAL, "batcalc.ifthen(else)", OPERATION_FAILED);
	}
	if (map) {
		t = BATleftjoin(map, bn, BATcount(bn));
		BBPreleaseref(bn->batCacheid);
		bn = t;
		BBPreleaseref(map->batCacheid);
	}
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}
