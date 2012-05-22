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
 * Copyright August 2008-2012 MonetDB B.V.
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
CMDbatUNARY(bat *ret, bat *bid, BAT *(*batfunc)(BAT *), const char *malfunc)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	bn = (*batfunc)(b);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatUNARY1(bat *ret, bat *bid, int accum,
			 BAT *(*batfunc)(BAT *, int), const char *malfunc)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	bn = (*batfunc)(b, accum);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatUNARY2(bat *ret, bat *bid, int accum, int abort_on_error,
			 BAT *(*batfunc)(BAT *, int, int), const char *malfunc)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	bn = (*batfunc)(b, accum, abort_on_error);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

batcalc_export str CMDbatISZERO(bat *ret, bat *bid);

str
CMDbatISZERO(bat *ret, bat *bid)
{
	return CMDbatUNARY(ret, bid, BATcalciszero, "batcalc.iszero");
}

batcalc_export str CMDbatISNIL(bat *ret, bat *bid);

str
CMDbatISNIL(bat *ret, bat *bid)
{
	return CMDbatUNARY(ret, bid, BATcalcisnil, "batcalc.isnil");
}

batcalc_export str CMDbatNOT(bat *ret, bat *bid);

str
CMDbatNOT(bat *ret, bat *bid)
{
	return CMDbatUNARY1(ret, bid, 0, BATcalcnot, "batcalc.not");
}

batcalc_export str CMDbatNOTaccum(bat *ret, bat *bid, int *accum);

str
CMDbatNOTaccum(bat *ret, bat *bid, int *accum)
{
	return CMDbatUNARY1(ret, bid, *accum, BATcalcnot, "batcalc.not");
}

batcalc_export str CMDbatABS(bat *ret, bat *bid);

str
CMDbatABS(bat *ret, bat *bid)
{
	return CMDbatUNARY1(ret, bid, 0, BATcalcabsolute, "batcalc.abs");
}

batcalc_export str CMDbatABSaccum(bat *ret, bat *bid, int *accum);

str
CMDbatABSaccum(bat *ret, bat *bid, int *accum)
{
	return CMDbatUNARY1(ret, bid, *accum, BATcalcabsolute, "batcalc.abs");
}

batcalc_export str CMDbatINCR(bat *ret, bat *bid);

str
CMDbatINCR(bat *ret, bat *bid)
{
	return CMDbatUNARY2(ret, bid, 0, 1, BATcalcincr, "batcalc.incr");
}

batcalc_export str CMDbatINCRaccum(bat *ret, bat *bid, int *accum);

str
CMDbatINCRaccum(bat *ret, bat *bid, int *accum)
{
	return CMDbatUNARY2(ret, bid, *accum, 1, BATcalcincr, "batcalc.incr");
}

batcalc_export str CMDbatDECR(bat *ret, bat *bid);

str
CMDbatDECR(bat *ret, bat *bid)
{
	return CMDbatUNARY2(ret, bid, 0, 1, BATcalcdecr, "batcalc.decr");
}

batcalc_export str CMDbatDECRaccum(bat *ret, bat *bid, int *accum);

str
CMDbatDECRaccum(bat *ret, bat *bid, int *accum)
{
	return CMDbatUNARY2(ret, bid, *accum, 1, BATcalcdecr, "batcalc.decr");
}

batcalc_export str CMDbatNEG(bat *ret, bat *bid);

str
CMDbatNEG(bat *ret, bat *bid)
{
	return CMDbatUNARY1(ret, bid, 0, BATcalcnegate, "batcalc.neg");
}

batcalc_export str CMDbatNEGaccum(bat *ret, bat *bid, int *accum);

str
CMDbatNEGaccum(bat *ret, bat *bid, int *accum)
{
	return CMDbatUNARY1(ret, bid, *accum, BATcalcnegate, "batcalc.neg");
}

batcalc_export str CMDbatSIGN(bat *ret, bat *bid);

str
CMDbatSIGN(bat *ret, bat *bid)
{
	return CMDbatUNARY(ret, bid, BATcalcsign, "batcalc.sign");
}

static int
calctype(int tp1, int tp2)
{
	int tp1s = ATOMstorage(tp1);
	int tp2s = ATOMstorage(tp2);
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
	assert(tp1 > 0 && tp1 < TYPE_str && tp1 != TYPE_flt && tp1 != TYPE_dbl && tp1 != TYPE_bat && tp1 != TYPE_ptr);
	assert(tp2 > 0 && tp2 < TYPE_str && tp2 != TYPE_flt && tp2 != TYPE_dbl && tp2 != TYPE_bat && tp2 != TYPE_ptr);
	return MIN(tp1, tp2);
}

static str
CMDbatBINARY3(bat *ret, bat *bid1, bat *bid2,
			  BAT *(*batfunc)(BAT *, BAT *, int, int, int),
			  int (*typefunc)(int, int),
			  int accum, int abort_on_error,
			  const char *malfunc)
{
	BAT *bn, *b1, *b2;

	b1 = BATdescriptor(*bid1);
	b2 = BATdescriptor(*bid2);
	if (b1 == NULL || b2 == NULL) {
		if (b1)
			BBPreleaseref(b1->batCacheid);
		if (b2)
			BBPreleaseref(b2->batCacheid);
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}
	bn = (*batfunc)(b1, b2, (*typefunc)(b1->T->type, b2->T->type),
					accum, abort_on_error);
	BBPreleaseref(b1->batCacheid);
	BBPreleaseref(b2->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY3cst(MalStkPtr stk, InstrPtr pci,
				 BAT *(batfunc1)(BAT *, const ValRecord *, int, int, int),
				 BAT *(batfunc2)(const ValRecord *, BAT *, int, int, int),
				 int (*typefunc)(int, int),
				 int accum, int abort_on_error, const char *malfunc)
{
	bat *bid;
	BAT *bn, *b;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		assert(tp2 != TYPE_bat && !isaBatType(tp2));
		bid = getArgReference(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)],
						 (*typefunc)(b->T->type, tp2), accum, abort_on_error);
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(tp2 == TYPE_bat || isaBatType(tp2));
		bid = getArgReference(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b,
						 (*typefunc)(tp1, b->T->type), accum, abort_on_error);
	}
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY2(bat *ret, bat *bid1, bat *bid2,
			  BAT *(*batfunc)(BAT *, BAT *, int, int),
			  int accum, int abort_on_error,
			  const char *malfunc)
{
	BAT *bn, *b1, *b2;

	b1 = BATdescriptor(*bid1);
	b2 = BATdescriptor(*bid2);
	if (b1 == NULL || b2 == NULL) {
		if (b1)
			BBPreleaseref(b1->batCacheid);
		if (b2)
			BBPreleaseref(b2->batCacheid);
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}
	bn = (*batfunc)(b1, b2, accum, abort_on_error);
	BBPreleaseref(b1->batCacheid);
	BBPreleaseref(b2->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY2cst(MalStkPtr stk, InstrPtr pci,
				 BAT *(*batfunc1)(BAT *, const ValRecord *, int, int),
				 BAT *(*batfunc2)(const ValRecord *, BAT *, int),
				 int accum, int abort_on_error,
				 const char *malfunc)
{
	bat *bid;
	BAT *bn, *b;
	int tp1;

	tp1 = stk->stk[getArg(pci, 1)].vtype;

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		assert(stk->stk[getArg(pci, 2)].vtype != TYPE_bat &&
			   !isaBatType(stk->stk[getArg(pci, 2)].vtype));
		bid = getArgReference(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)], accum, abort_on_error);
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(stk->stk[getArg(pci, 2)].vtype == TYPE_bat ||
			   isaBatType(stk->stk[getArg(pci, 2)].vtype));
		bid = getArgReference(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b, abort_on_error);
	}
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY1(bat *ret, bat *bid1, bat *bid2,
			  BAT *(*batfunc)(BAT *, BAT *, int),
			  int accum,
			  const char *malfunc)
{
	BAT *bn, *b1, *b2;

	b1 = BATdescriptor(*bid1);
	b2 = BATdescriptor(*bid2);
	if (b1 == NULL || b2 == NULL) {
		if (b1)
			BBPreleaseref(b1->batCacheid);
		if (b2)
			BBPreleaseref(b2->batCacheid);
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}
	bn = (*batfunc)(b1, b2, accum);
	BBPreleaseref(b1->batCacheid);
	BBPreleaseref(b2->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY1cst(MalStkPtr stk, InstrPtr pci,
				 BAT *(*batfunc1)(BAT *, const ValRecord *, int),
				 BAT *(*batfunc2)(const ValRecord *, BAT *, int),
				 int accum,
				 const char *malfunc)
{
	bat *bid;
	BAT *bn, *b;
	int tp1;

	tp1 = stk->stk[getArg(pci, 1)].vtype;

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		assert(stk->stk[getArg(pci, 2)].vtype != TYPE_bat &&
			   !isaBatType(stk->stk[getArg(pci, 2)].vtype));
		bid = getArgReference(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)], accum);
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(stk->stk[getArg(pci, 2)].vtype == TYPE_bat ||
			   isaBatType(stk->stk[getArg(pci, 2)].vtype));
		bid = getArgReference(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b, accum);
	}
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY0(bat *ret, bat *bid1, bat *bid2,
			  BAT *(*batfunc)(BAT *, BAT *),
			  const char *malfunc)
{
	BAT *bn, *b1, *b2;

	b1 = BATdescriptor(*bid1);
	b2 = BATdescriptor(*bid2);
	if (b1 == NULL || b2 == NULL) {
		if (b1)
			BBPreleaseref(b1->batCacheid);
		if (b2)
			BBPreleaseref(b2->batCacheid);
		throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
	}
	bn = (*batfunc)(b1, b2);
	BBPreleaseref(b1->batCacheid);
	BBPreleaseref(b2->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatBINARY0cst(MalStkPtr stk, InstrPtr pci,
				 BAT *(*batfunc1)(BAT *, const ValRecord *),
				 BAT *(*batfunc2)(const ValRecord *, BAT *),
				 const char *malfunc)
{
	bat *bid;
	BAT *bn, *b;
	int tp1;

	tp1 = stk->stk[getArg(pci, 1)].vtype;

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		assert(stk->stk[getArg(pci, 2)].vtype != TYPE_bat &&
			   !isaBatType(stk->stk[getArg(pci, 2)].vtype));
		bid = getArgReference(stk, pci, 1);
		b = BATdescriptor(*bid);
		if (b == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		bn = (*batfunc1)(b, &stk->stk[getArg(pci, 2)]);
	} else {
		assert(tp1 != TYPE_bat && !isaBatType(tp1));
		assert(stk->stk[getArg(pci, 2)].vtype == TYPE_bat ||
			   isaBatType(stk->stk[getArg(pci, 2)].vtype));
		bid = getArgReference(stk, pci, 2);
		b = BATdescriptor(*bid);
		if (b == NULL)
			throw(MAL, malfunc, RUNTIME_OBJECT_MISSING);
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b);
	}
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

batcalc_export str CMDbatADD(bat *ret, bat *bid1, bat *bid2);

str
CMDbatADD(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcadd,
						 calctype, 0, 0, "batcalc.add_noerror");
}

batcalc_export str CMDbatADDsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatADDsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcadd,
						 calctype, 0, 1, "batcalc.+");
}

batcalc_export str CMDbatADDsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatADDsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcadd,
						 calctype, *accum, 1, "batcalc.+");
}

batcalc_export str CMDbatADDenlarge(bat *ret, bat *bid1, bat *bid2);

str
CMDbatADDenlarge(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcadd,
						 calctypeenlarge, 0, 1, "batcalc.add_enlarge");
}

batcalc_export str CMDbatADDcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcaddcst, BATcalccstadd,
							calctype, 0, 0, "batcalc.add_noerror");
}

batcalc_export str CMDbatADDcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcaddcst, BATcalccstadd,
							calctype, accum, 1, "batcalc.+");
}

batcalc_export str CMDbatADDcstenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDcstenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcaddcst, BATcalccstadd,
							calctypeenlarge, 0, 1, "batcalc.add_enlarge");
}

batcalc_export str CMDbatSUB(bat *ret, bat *bid1, bat *bid2);

str
CMDbatSUB(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcsub,
						 calctype, 0, 0, "batcalc.sub_noerror");
}

batcalc_export str CMDbatSUBsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatSUBsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcsub,
						 calctype, 0, 1, "batcalc.-");
}

batcalc_export str CMDbatSUBsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatSUBsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcsub,
						 calctype, *accum, 1, "batcalc.-");
}

batcalc_export str CMDbatSUBenlarge(bat *ret, bat *bid1, bat *bid2);

str
CMDbatSUBenlarge(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcsub,
						 calctypeenlarge, 0, 1, "batcalc.sub_enlarge");
}

batcalc_export str CMDbatSUBcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcsubcst, BATcalccstsub,
							calctype, 0, 0, "batcalc.sub_noerror");
}

batcalc_export str CMDbatSUBcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcsubcst, BATcalccstsub,
							calctype, accum, 1, "batcalc.-");
}

batcalc_export str CMDbatSUBcstenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBcstenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcsubcst, BATcalccstsub,
							calctypeenlarge, 0, 1, "batcalc.sub_enlarge");
}

batcalc_export str CMDbatMUL(bat *ret, bat *bid1, bat *bid2);

str
CMDbatMUL(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcmul,
						 calctype, 0, 0, "batcalc.mul_noerror");
}

batcalc_export str CMDbatMULsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatMULsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcmul,
						 calctype, 0, 1, "batcalc.*");
}

batcalc_export str CMDbatMULsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatMULsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcmul,
						 calctype, *accum, 1, "batcalc.*");
}

batcalc_export str CMDbatMULenlarge(bat *ret, bat *bid1, bat *bid2);

str
CMDbatMULenlarge(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcmul,
						 calctypeenlarge, 0, 1, "batcalc.mul_enlarge");
}

batcalc_export str CMDbatMULcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcmulcst, BATcalccstmul,
							calctype, 0, 0, "batcalc.mul_noerror");
}

batcalc_export str CMDbatMULcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcmulcst, BATcalccstmul,
							calctype, accum, 1, "batcalc.*");
}

batcalc_export str CMDbatMULcstenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULcstenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcmulcst, BATcalccstmul,
							calctypeenlarge, 0, 1, "batcalc.mul_enlarge");
}

batcalc_export str CMDbatDIV(bat *ret, bat *bid1, bat *bid2);

str
CMDbatDIV(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcdiv,
						 calcdivtype, 0, 0, "batcalc.div_noerror");
}

batcalc_export str CMDbatDIVsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatDIVsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcdiv,
						 calcdivtype, 0, 1, "batcalc./");
}

batcalc_export str CMDbatDIVsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatDIVsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcdiv,
						 calcdivtype, *accum, 1, "batcalc./");
}

batcalc_export str CMDbatDIVcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcdivcst, BATcalccstdiv,
							calcdivtype, 0, 0, "batcalc.div_noerror");
}

batcalc_export str CMDbatDIVcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcdivcst, BATcalccstdiv,
							calcdivtype, accum, 1, "batcalc./");
}

#if 0
batcalc_export str CMDbatDIVflt(bat *ret, bat *bid1, bat *bid2);

str
CMDbatDIVflt(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcdiv,
						 calcdivtypeflt, 0, 0, "batcalc.div_fltnoerror");
}

batcalc_export str CMDbatDIVfltsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatDIVfltsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcdiv,
						 calcdivtypeflt, 0, 1, "batcalc.div_flt");
}

batcalc_export str CMDbatDIVdbl(bat *ret, bat *bid1, bat *bid2);

str
CMDbatDIVdbl(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcdiv,
						 calcdivtypedbl, 0, 0, "batcalc.div_dblnoerror");
}

batcalc_export str CMDbatDIVdblsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatDIVdblsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcdiv,
						 calcdivtypedbl, 0, 1, "batcalc.div_dbl");
}
#endif

batcalc_export str CMDbatMOD(bat *ret, bat *bid1, bat *bid2);

str
CMDbatMOD(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcmod,
						 calcmodtype, 0, 0, "batcalc.mod_noerror");
}

batcalc_export str CMDbatMODsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatMODsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcmod,
						 calcmodtype, 0, 1, "batcalc.%");
}

batcalc_export str CMDbatMODsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatMODsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY3(ret, bid1, bid2, BATcalcmod,
						 calcmodtype, *accum, 1, "batcalc.%");
}

batcalc_export str CMDbatMODcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMODcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcmodcst, BATcalccstmod,
							calcmodtype, 0, 0, "batcalc.mod_noerror");
}

batcalc_export str CMDbatMODcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMODcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY3cst(stk, pci, BATcalcmodcst, BATcalccstmod,
							calcmodtype, accum, 1, "batcalc.%");
}

batcalc_export str CMDbatXOR(bat *ret, bat *bid1, bat *bid2);

str
CMDbatXOR(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY1(ret, bid1, bid2, BATcalcxor, 0, "batcalc.xor");
}

batcalc_export str CMDbatXORaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatXORaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY1(ret, bid1, bid2, BATcalcxor, *accum, "batcalc.xor");
}

batcalc_export str CMDbatXORcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatXORcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1cst(stk, pci, BATcalcxorcst, BATcalccstxor, accum,
							"batcalc.xor");
}

batcalc_export str CMDbatOR(bat *ret, bat *bid1, bat *bid2);

str
CMDbatOR(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY1(ret, bid1, bid2, BATcalcor, 0, "batcalc.or");
}

batcalc_export str CMDbatORaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatORaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY1(ret, bid1, bid2, BATcalcor, *accum, "batcalc.or");
}

batcalc_export str CMDbatORcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatORcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1cst(stk, pci, BATcalcorcst, BATcalccstor, accum,
							"batcalc.or");
}

batcalc_export str CMDbatAND(bat *ret, bat *bid1, bat *bid2);

str
CMDbatAND(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY1(ret, bid1, bid2, BATcalcand, 0, "batcalc.and");
}

batcalc_export str CMDbatANDaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatANDaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY1(ret, bid1, bid2, BATcalcand, *accum, "batcalc.and");
}

batcalc_export str CMDbatANDcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatANDcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1cst(stk, pci, BATcalcandcst, BATcalccstand, accum,
							"batcalc.and");
}

batcalc_export str CMDbatLSH(bat *ret, bat *bid1, bat *bid2);

str
CMDbatLSH(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY2(ret, bid1, bid2, BATcalclsh, 0, 0, "batcalc.lsh_noerror");
}

batcalc_export str CMDbatLSHcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLSHcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2cst(stk, pci, BATcalclshcst, BATcalccstlsh, 0, 0,
							"batcalc.lsh_noerror");
}

batcalc_export str CMDbatLSHsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatLSHsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY2(ret, bid1, bid2, BATcalclsh, 0, 1, "batcalc.<<");
}

batcalc_export str CMDbatLSHsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatLSHsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY2(ret, bid1, bid2, BATcalclsh, *accum, 1, "batcalc.<<");
}

batcalc_export str CMDbatLSHcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLSHcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2cst(stk, pci, BATcalclshcst, BATcalccstlsh, accum, 1,
							"batcalc.<<");
}

batcalc_export str CMDbatRSH(bat *ret, bat *bid1, bat *bid2);

str
CMDbatRSH(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY2(ret, bid1, bid2, BATcalcrsh, 0, 0, "batcalc.rsh_noerror");
}

batcalc_export str CMDbatRSHcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatRSHcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2cst(stk, pci, BATcalcrshcst, BATcalccstrsh, 0, 0,
							"batcalc.rsh_noerror");
}

batcalc_export str CMDbatRSHsignal(bat *ret, bat *bid1, bat *bid2);

str
CMDbatRSHsignal(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY2(ret, bid1, bid2, BATcalcrsh, 0, 1, "batcalc.>>");
}

batcalc_export str CMDbatRSHsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum);

str
CMDbatRSHsignalaccum(bat *ret, bat *bid1, bat *bid2, int *accum)
{
	return CMDbatBINARY2(ret, bid1, bid2, BATcalcrsh, *accum, 1, "batcalc.>>");
}

batcalc_export str CMDbatRSHcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatRSHcstsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int accum = pci->argc == 4 ? * (int *) getArgReference(stk, pci, 3) : 0;

	(void) cntxt;
	(void) mb;

	return CMDbatBINARY2cst(stk, pci, BATcalcrshcst, BATcalccstrsh, accum, 1,
							"batcalc.>>");
}

batcalc_export str CMDbatLT(bat *ret, bat *bid1, bat *bid2);

str
CMDbatLT(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY0(ret, bid1, bid2, BATcalclt, "batcalc.<");
}

batcalc_export str CMDbatLTcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLTcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0cst(stk, pci, BATcalcltcst, BATcalccstlt,
							"batcalc.<");
}

batcalc_export str CMDbatLE(bat *ret, bat *bid1, bat *bid2);

str
CMDbatLE(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY0(ret, bid1, bid2, BATcalcle, "batcalc.<=");
}

batcalc_export str CMDbatLEcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLEcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0cst(stk, pci, BATcalclecst, BATcalccstle,
							"batcalc.<=");
}

batcalc_export str CMDbatGT(bat *ret, bat *bid1, bat *bid2);

str
CMDbatGT(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY0(ret, bid1, bid2, BATcalcgt, "batcalc.>");
}

batcalc_export str CMDbatGTcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatGTcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0cst(stk, pci, BATcalcgtcst, BATcalccstgt,
							"batcalc.>");
}

batcalc_export str CMDbatGE(bat *ret, bat *bid1, bat *bid2);

str
CMDbatGE(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY0(ret, bid1, bid2, BATcalcge, "batcalc.>=");
}

batcalc_export str CMDbatGEcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatGEcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0cst(stk, pci, BATcalcgecst, BATcalccstge,
							"batcalc.>=");
}

batcalc_export str CMDbatEQ(bat *ret, bat *bid1, bat *bid2);

str
CMDbatEQ(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY0(ret, bid1, bid2, BATcalceq, "batcalc.==");
}

batcalc_export str CMDbatEQcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatEQcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0cst(stk, pci, BATcalceqcst, BATcalccsteq,
							"batcalc.==");
}

batcalc_export str CMDbatNE(bat *ret, bat *bid1, bat *bid2);

str
CMDbatNE(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY0(ret, bid1, bid2, BATcalcne, "batcalc.!=");
}

batcalc_export str CMDbatNEcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNEcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0cst(stk, pci, BATcalcnecst, BATcalccstne,
							"batcalc.!=");
}

batcalc_export str CMDbatCMP(bat *ret, bat *bid1, bat *bid2);

str
CMDbatCMP(bat *ret, bat *bid1, bat *bid2)
{
	return CMDbatBINARY0(ret, bid1, bid2, BATcalccmp, "batcalc.cmp");
}

batcalc_export str CMDbatCMPcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatCMPcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0cst(stk, pci, BATcalccmpcst, BATcalccstcmp,
							"batcalc.cmp");
}

batcalc_export str CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *bid;
	BAT *bn, *b, *lo = NULL, *hi = NULL;
	int tp1, tp2, tp3;

	(void) cntxt;
	(void) mb;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	tp3 = stk->stk[getArg(pci, 3)].vtype;

	if (tp1 != TYPE_bat && !isaBatType(tp1))
		throw(MAL, "batcalc.between", ILLEGAL_ARGUMENT);
	bid = getArgReference(stk, pci, 1);
	b = BATdescriptor(*bid);
	if (b == NULL)
		throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = getArgReference(stk, pci, 2);
		lo = BATdescriptor(*bid);
		if (lo == NULL)
			throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
	}
	if (tp3 == TYPE_bat || isaBatType(tp3)) {
		bid = getArgReference(stk, pci, 3);
		hi = BATdescriptor(*bid);
		if (hi == NULL)
			throw(MAL, "batcalc.between", RUNTIME_OBJECT_MISSING);
	}
	if (lo == NULL) {
		if (hi == NULL) {
			bn = BATcalcbetweencstcst(b, &stk->stk[getArg(pci, 2)], &stk->stk[getArg(pci, 3)]);
		} else {
			bn = BATcalcbetweencstbat(b, &stk->stk[getArg(pci, 2)], hi);
		}
	} else {
		if (hi == NULL) {
			bn = BATcalcbetweenbatcst(b, lo, &stk->stk[getArg(pci, 3)]);
		} else {
			bn = BATcalcbetween(b, lo, hi);
		}
	}
	BBPreleaseref(b->batCacheid);
	if (lo)
		BBPreleaseref(lo->batCacheid);
	if (hi)
		BBPreleaseref(hi->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, "batcalc.between", OPERATION_FAILED);
	bid = (bat *) getArgReference(stk, pci, 0);
	BBPkeepref(*bid = bn->batCacheid);
	return MAL_SUCCEED;
}

batcalc_export str CMDcalcavg2(dbl *avg, lng *vals, bat *bid);

str
CMDcalcavg2(dbl *avg, lng *vals, bat *bid)
{
	BAT *b;
	int ret;
	BUN v;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "aggr.avg", RUNTIME_OBJECT_MISSING);
	ret = BATcalcavg(b, avg, &v);
	BBPreleaseref(b->batCacheid);
	if (ret == GDK_FAIL)
		return mythrow(MAL, "aggr.avg", OPERATION_FAILED);
	if (vals)
		*vals = (lng) v;
	return MAL_SUCCEED;
}

batcalc_export str CMDcalcavg(dbl *avg, bat *bid);

str
CMDcalcavg(dbl *avg, bat *bid)
{
	return CMDcalcavg2(avg, NULL, bid);
}

static str
CMDconvertbat(bat *ret, bat *bid, int tp, int abort_on_error)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.convert", RUNTIME_OBJECT_MISSING);
	bn = BATconvert(b, tp, abort_on_error);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL) {
		char buf[20];
		snprintf(buf, sizeof(buf), "batcalc.%s", ATOMname(tp));
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

#define BATCONVERT_TYPE(TYPE)						\
str													\
CMDconvert_##TYPE(bat *ret, bat *bid)				\
{													\
	return CMDconvertbat(ret, bid, TYPE_##TYPE, 0);	\
}													\
str													\
CMDconvertsignal_##TYPE(bat *ret, bat *bid)			\
{													\
	return CMDconvertbat(ret, bid, TYPE_##TYPE, 1);	\
}

/* exports outside of define for clients exports test */
batcalc_export str CMDconvert_bit(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_bit(bat *ret, bat *bid);
BATCONVERT_TYPE(bit)
batcalc_export str CMDconvert_bte(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_bte(bat *ret, bat *bid);
BATCONVERT_TYPE(bte)
batcalc_export str CMDconvert_sht(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_sht(bat *ret, bat *bid);
BATCONVERT_TYPE(sht)
batcalc_export str CMDconvert_int(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_int(bat *ret, bat *bid);
BATCONVERT_TYPE(int)
batcalc_export str CMDconvert_wrd(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_wrd(bat *ret, bat *bid);
BATCONVERT_TYPE(wrd)
batcalc_export str CMDconvert_lng(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_lng(bat *ret, bat *bid);
BATCONVERT_TYPE(lng)
batcalc_export str CMDconvert_flt(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_flt(bat *ret, bat *bid);
BATCONVERT_TYPE(flt)
batcalc_export str CMDconvert_dbl(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_dbl(bat *ret, bat *bid);
BATCONVERT_TYPE(dbl)
batcalc_export str CMDconvert_oid(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_oid(bat *ret, bat *bid);
BATCONVERT_TYPE(oid)
batcalc_export str CMDconvert_str(bat *ret, bat *bid);
batcalc_export str CMDconvertsignal_str(bat *ret, bat *bid);
BATCONVERT_TYPE(str)
