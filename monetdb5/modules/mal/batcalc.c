/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include <math.h>
#include "mal_exception.h"
#include "mal_interpreter.h"

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
		GDKclrerr();
		return s;
	}
	return createException(type, fcn, "%s", msg);
}

static str
CMDbatUNARY(MalStkPtr stk, InstrPtr pci,
			BAT *(*batfunc)(BAT *, BAT *, BAT *), const char *malfunc)
{
	bat bid;
	BAT *bn, *b, *s = NULL, *r = NULL;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid)) {
			if ((s = BATdescriptor(bid)) == NULL) {
				BBPunfix(b->batCacheid);
				throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			if (s->ttype == TYPE_bit) {
				r = s;
				s = NULL;
			}
		}
	} else if (pci->argc == 4) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid) && (s = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		bid = *getArgReference_bat(stk, pci, 3);
		if (!is_bat_nil(bid) && (r = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			BBPunfix(s->batCacheid);
			throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	}

	bn = (*batfunc)(b, s, r);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbatUNARY1(MalStkPtr stk, InstrPtr pci, bool abort_on_error,
			 BAT *(*batfunc)(BAT *, BAT *, BAT *, bool), const char *malfunc)
{
	bat bid;
	BAT *bn, *b, *s = NULL, *r = NULL;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid)) {
			if ((s = BATdescriptor(bid)) == NULL) {
				BBPunfix(b->batCacheid);
				throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			if (s->ttype == TYPE_bit) {
				r = s;
				s = NULL;
			}
		}
	} else if (pci->argc == 4) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid) && (s = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		bid = *getArgReference_bat(stk, pci, 3);
		if (!is_bat_nil(bid) && (r = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			BBPunfix(s->batCacheid);
			throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	}

	bn = (*batfunc)(b, s, r, abort_on_error);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, malfunc, OPERATION_FAILED);
	}
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

mal_export str CMDbatISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalciszero, "batcalc.iszero");
}

mal_export str CMDbatISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcisnil, "batcalc.isnil");
}

mal_export str CMDbatISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcisnotnil, "batcalc.isnotnil");
}

mal_export str CMDbatNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcnot, "batcalc.not");
}

mal_export str CMDbatABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcabsolute, "batcalc.abs");
}

mal_export str CMDbatINCR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatINCR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY1(stk, pci, true, BATcalcincr, "batcalc.incr");
}

mal_export str CMDbatDECR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDECR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY1(stk, pci, true, BATcalcdecr, "batcalc.decr");
}

mal_export str CMDbatNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatUNARY(stk, pci, BATcalcnegate, "batcalc.neg");
}

mal_export str CMDbatSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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
	int tp1s = ATOMbasetype(tp1);
	int tp2s = ATOMbasetype(tp2);
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
		return TYPE_lng;
#ifdef HAVE_HGE
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
	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
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
	tp1 = ATOMbasetype(tp1);
	tp2 = ATOMbasetype(tp2);
	assert(tp1 > 0 && tp1 < TYPE_str && tp1 != TYPE_bat && tp1 != TYPE_ptr);
	assert(tp2 > 0 && tp2 < TYPE_str && tp2 != TYPE_bat && tp2 != TYPE_ptr);
	if (tp1 == TYPE_dbl || tp2 == TYPE_dbl)
		return TYPE_dbl;
	if (tp1 == TYPE_flt || tp2 == TYPE_flt)
		return TYPE_flt;
	return MIN(tp1, tp2);
}

/* MAL function has one of the following signatures:
 * # without candidate list
 * func(b1:bat, b2:bat) :bat
 * func(b1:bat, v2:any) :bat
 * func(v1:any, b2:bat) :bat
 * # with candidate list
 * func(b1:bat, b2:bat, s1:bat, s2:bat) :bat
 * func(b1:bat, v2:any, s1:bat) :bat
 * func(v1:any, b2:bat, s2:bat) :bat
 * # without candidate list
 * func(b1:bat, b2:bat, r:bat) :bat
 * func(b1:bat, v2:any, r:bat) :bat
 * func(v1:any, b2:bat, r:bat) :bat
 * # with candidate list
 * func(b1:bat, b2:bat, s1:bat, s2:bat, r:bat) :bat
 * func(b1:bat, v2:any, s1:bat, r:bat) :bat
 * func(v1:any, b2:bat, s2:bat, r:bat) :bat
 */
static str
CMDbatBINARY2(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, BAT *, BAT *, int, bool),
			  BAT *(batfunc1)(BAT *, const ValRecord *, BAT *, BAT *, int, bool),
			  BAT *(batfunc2)(const ValRecord *, BAT *, BAT *, BAT *, int, bool),
			  int (*typefunc)(int, int),
			  bool abort_on_error, const char *malfunc)
{
	bat bid;
	BAT *bn, *b1 = NULL, *b2 = NULL, *s1 = NULL, *s2 = NULL, *r = NULL;
	int tp1, tp2, tp3;

	tp1 = stk->stk[getArg(pci, 1)].vtype; /* first argument */
	tp2 = stk->stk[getArg(pci, 2)].vtype; /* second argument */
	tp3 = getArgType(mb, pci, 0);		  /* return argument */
	assert(isaBatType(tp3));
	tp3 = getBatType(tp3);

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		bid = *getArgReference_bat(stk, pci, 1);
		b1 = BATdescriptor(bid);
		if (b1 == NULL)
			goto bailout;
	}

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = *getArgReference_bat(stk, pci, 2);
		b2 = BATdescriptor(bid);
		if (b2 == NULL)
			goto bailout;
	}

	if (pci->argc > 5) {
		assert(pci->argc == 6);
		bid = *getArgReference_bat(stk, pci, 5);
		if (!is_bat_nil(bid)) {
			r = BATdescriptor(bid);
			if (r == NULL)
				goto bailout;
			assert(r->ttype == TYPE_bit);
		}
	}
	if (pci->argc > 4) {
		bid = *getArgReference_bat(stk, pci, 4);
		if (!is_bat_nil(bid)) {
			s2 = BATdescriptor(bid);
			if (s2 == NULL)
				goto bailout;
			if (s2->ttype == TYPE_bit) {
				assert(pci->argc == 5);
				assert(r == NULL);
				assert(b1 == NULL || b2 == NULL);
				r = s2;
				s2 = NULL;
			}
		}
	}
	if (pci->argc > 3) {
		bid = *getArgReference_bat(stk, pci, 3);
		if (!is_bat_nil(bid)) {
			s1 = BATdescriptor(bid);
			if (s1 == NULL)
				goto bailout;
			if (s1->ttype == TYPE_bit) {
				assert(pci->argc == 4);
				r = s1;
				s1 = NULL;
			} else if (b1 == NULL) {
				s2 = s1;
				s1 = NULL;
			}
		}
	}

	if (b1 && b2) {
		if (tp3 == TYPE_any)
			tp3 = (*typefunc)(b1->ttype, b2->ttype);
		bn = (*batfunc)(b1, b2, s1, s2, r, tp3, abort_on_error);
	} else if (b1) {
		if (tp3 == TYPE_any)
			tp3 = (*typefunc)(b1->ttype, tp2);
		bn = (*batfunc1)(b1, &stk->stk[getArg(pci, 2)], s1, r, tp3, abort_on_error);
	} else if (b2) {
		if (tp3 == TYPE_any)
			tp3 = (*typefunc)(tp1, b2->ttype);
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b2, s2, r, tp3, abort_on_error);
	} else
		goto bailout;			/* cannot happen */
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, GDK_EXCEPTION);
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;

bailout:
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

/* MAL function has one of the signatures for CMDbatBINARY2, or one of
 * the following:
 * # without candidate list
 * func(b1:bat, b2:bat, abort_on_error:bit) :bat
 * func(b1:bat, v2:any, abort_on_error:bit) :bat
 * func(v1:any, b2:bat, abort_on_error:bit) :bat
 * # with candidate list
 * func(b1:bat, b2:bat, s1:bat, s2:bat, abort_on_error:bit) :bat
 * func(b1:bat, v2:any, s1:bat, abort_on_error:bit) :bat
 * func(v1:any, b2:bat, s2:bat, abort_on_error:bit) :bat
 * # without candidate list
 * func(b1:bat, b2:bat, r:bat, abort_on_error:bit) :bat
 * func(b1:bat, v2:any, r:bat, abort_on_error:bit) :bat
 * func(v1:any, b2:bat, r:bat, abort_on_error:bit) :bat
 * # with candidate list
 * func(b1:bat, b2:bat, s1:bat, s2:bat, r:bat, abort_on_error:bit) :bat
 * func(b1:bat, v2:any, s1:bat, r:bat, abort_on_error:bit) :bat
 * func(v1:any, b2:bat, s2:bat, r:bat, abort_on_error:bit) :bat
 */
static str
CMDbatBINARY1(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, BAT *, BAT *, bool),
			  BAT *(*batfunc1)(BAT *, const ValRecord *, BAT *, BAT *, bool),
			  BAT *(*batfunc2)(const ValRecord *, BAT *, BAT *, BAT *, bool),
			  bool abort_on_error,
			  const char *malfunc)
{
	bat bid;
	BAT *bn, *b1 = NULL, *b2 = NULL, *s1 = NULL, *s2 = NULL, *r = NULL;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype; /* first argument */
	tp2 = stk->stk[getArg(pci, 2)].vtype; /* second argument */

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		bid = *getArgReference_bat(stk, pci, 1);
		b1 = BATdescriptor(bid);
		if (b1 == NULL)
			goto bailout;
	}

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = *getArgReference_bat(stk, pci, 2);
		b2 = BATdescriptor(bid);
		if (b2 == NULL)
			goto bailout;
	}

	if (pci->argc > 6) {
		assert(pci->argc == 7);
		abort_on_error = *getArgReference_bit(stk, pci, 6);
	}
	if (pci->argc > 5) {
		if (stk->stk[getArg(pci, 5)].vtype == TYPE_bat) {
			bid = *getArgReference_bat(stk, pci, 5);
			if (!is_bat_nil(bid)) {
				r = BATdescriptor(bid);
				if (r == NULL)
					goto bailout;
				assert(r->ttype == TYPE_bit);
			}
		} else {
			assert(pci->argc == 6);
			abort_on_error = *getArgReference_bit(stk, pci, 5);
		}
	}
	if (pci->argc > 4) {
		if (stk->stk[getArg(pci, 4)].vtype == TYPE_bat) {
			bid = *getArgReference_bat(stk, pci, 4);
			if (!is_bat_nil(bid)) {
				s2 = BATdescriptor(bid);
				if (s2 == NULL)
					goto bailout;
				if (s2->ttype == TYPE_bit) {
					assert(r == NULL);
					assert(b1 == NULL || b2 == NULL);
					r = s2;
					s2 = NULL;
				}
			}
		} else {
			assert(pci->argc == 5);
			abort_on_error = *getArgReference_bit(stk, pci, 4);
		}
	}
	if (pci->argc > 3) {
		if (stk->stk[getArg(pci, 3)].vtype == TYPE_bat) {
			bid = *getArgReference_bat(stk, pci, 3);
			if (!is_bat_nil(bid)) {
				s1 = BATdescriptor(bid);
				if (s1 == NULL)
					goto bailout;
				if (s1->ttype == TYPE_bit) {
					r = s1;
					s1 = NULL;
				} else if (b1 == NULL) {
					s2 = s1;
					s1 = NULL;
				}
			}
		} else {
			assert(pci->argc == 4);
			abort_on_error = *getArgReference_bit(stk, pci, 3);
		}
	}

	if (b1 && b2)
		bn = (*batfunc)(b1, b2, s1, s2, r, abort_on_error);
	else if (b1)
		bn = (*batfunc1)(b1, &stk->stk[getArg(pci, 2)], s1, r, abort_on_error);
	else if (b2)
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b2, s2, r, abort_on_error);
	else
		goto bailout;			/* cannot happen */
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, GDK_EXCEPTION);
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;

bailout:
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

/* MAL function has one of the signatures for CMDbatBINARY2
 */
static str
CMDbatBINARY0(MalStkPtr stk, InstrPtr pci,
			  BAT *(*batfunc)(BAT *, BAT *, BAT *, BAT *, BAT *),
			  BAT *(*batfunc1)(BAT *, const ValRecord *, BAT *, BAT *),
			  BAT *(*batfunc2)(const ValRecord *, BAT *, BAT *, BAT *),
			  const char *malfunc)
{
	bat bid;
	BAT *bn, *b1 = NULL, *b2 = NULL, *s1 = NULL, *s2 = NULL, *r = NULL;
	int tp1, tp2;

	tp1 = stk->stk[getArg(pci, 1)].vtype; /* first argument */
	tp2 = stk->stk[getArg(pci, 2)].vtype; /* second argument */

	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		bid = *getArgReference_bat(stk, pci, 1);
		b1 = BATdescriptor(bid);
		if (b1 == NULL)
			goto bailout;
	}

	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = *getArgReference_bat(stk, pci, 2);
		b2 = BATdescriptor(bid);
		if (b2 == NULL)
			goto bailout;
	}

	if (pci->argc > 5) {
		assert(pci->argc == 6);
		bid = *getArgReference_bat(stk, pci, 5);
		if (!is_bat_nil(bid)) {
			r = BATdescriptor(bid);
			if (r == NULL)
				goto bailout;
			assert(r->ttype == TYPE_bit);
		}
	}
	if (pci->argc > 4) {
		bid = *getArgReference_bat(stk, pci, 4);
		if (!is_bat_nil(bid)) {
			s2 = BATdescriptor(bid);
			if (s2 == NULL)
				goto bailout;
			if (s2->ttype == TYPE_bit) {
				assert(pci->argc == 5);
				assert(r == NULL);
				assert(b1 == NULL || b2 == NULL);
				r = s2;
				s2 = NULL;
			}
		}
	}
	if (pci->argc > 3) {
		bid = *getArgReference_bat(stk, pci, 3);
		if (!is_bat_nil(bid)) {
			s1 = BATdescriptor(bid);
			if (s1 == NULL)
				goto bailout;
			if (s1->ttype == TYPE_bit) {
				assert(pci->argc == 4);
				r = s1;
				s1 = NULL;
			} else if (b1 == NULL) {
				s2 = s1;
				s1 = NULL;
			}
		}
	}

	if (b1 && b2)
		bn = (*batfunc)(b1, b2, s1, s2, r);
	else if (b1)
		bn = (*batfunc1)(b1, &stk->stk[getArg(pci, 2)], s1, r);
	else if (b2)
		bn = (*batfunc2)(&stk->stk[getArg(pci, 1)], b2, s2, r);
	else
		goto bailout;			/* cannot happen */
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, malfunc, GDK_EXCEPTION);
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;

bailout:
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (s1)
		BBPunfix(s1->batCacheid);
	if (s2)
		BBPunfix(s2->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	throw(MAL, malfunc, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

mal_export str CMDbatMIN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMIN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmin, BATcalcmincst, BATcalccstmin, "batcalc.min");
}

mal_export str CMDbatMIN_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMIN_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmin_no_nil, BATcalcmincst_no_nil, BATcalccstmin_no_nil, "batcalc.min_no_nil");
}

mal_export str CMDbatMAX(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMAX(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmax, BATcalcmaxcst, BATcalccstmax, "batcalc.max");
}

mal_export str CMDbatMAX_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMAX_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcmax_no_nil, BATcalcmaxcst_no_nil, BATcalccstmax_no_nil, "batcalc.max_no_nil");
}

mal_export str CMDbatADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctype, 0, "batcalc.add_noerror");
}

mal_export str CMDbatADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctype, 1, "batcalc.+");
}

mal_export str CMDbatADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatADDenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcadd, BATcalcaddcst, BATcalccstadd,
						 calctypeenlarge, 1, "batcalc.add_enlarge");
}

mal_export str CMDbatSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctype, 0, "batcalc.sub_noerror");
}

mal_export str CMDbatSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctype, 1, "batcalc.-");
}

mal_export str CMDbatSUBenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatSUBenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcsub, BATcalcsubcst, BATcalccstsub,
						 calctypeenlarge, 1, "batcalc.sub_enlarge");
}

mal_export str CMDbatMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctype, 0, "batcalc.mul_noerror");
}

mal_export str CMDbatMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctype, 1, "batcalc.*");
}

mal_export str CMDbatMULenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMULenlarge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmul, BATcalcmulcst, BATcalccstmul,
						 calctypeenlarge, 1, "batcalc.mul_enlarge");
}

mal_export str CMDbatDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 0, "batcalc.div_noerror");
}

mal_export str CMDbatDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcdiv, BATcalcdivcst, BATcalccstdiv,
						 calcdivtype, 1, "batcalc./");
}

mal_export str CMDbatMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmod, BATcalcmodcst, BATcalccstmod,
						 calcmodtype, 0, "batcalc.mod_noerror");
}

mal_export str CMDbatMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDbatBINARY2(mb, stk, pci, BATcalcmod, BATcalcmodcst, BATcalccstmod,
						 calcmodtype, 1, "batcalc.%");
}

mal_export str CMDbatXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcxor, BATcalcxorcst, BATcalccstxor,
						 "batcalc.xor");
}

mal_export str CMDbatOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcor, BATcalcorcst, BATcalccstor,
						 "batcalc.or");
}

mal_export str CMDbatAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcand, BATcalcandcst, BATcalccstand,
						 "batcalc.and");
}

mal_export str CMDbatLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalclsh, BATcalclshcst, BATcalccstlsh,
						 false, "batcalc.lsh_noerror");
}

mal_export str CMDbatLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalclsh, BATcalclshcst, BATcalccstlsh,
						 true, "batcalc.<<");
}

mal_export str CMDbatRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcrsh, BATcalcrshcst, BATcalccstrsh,
						 false, "batcalc.rsh_noerror");
}

mal_export str CMDbatRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcrsh, BATcalcrshcst, BATcalccstrsh,
						 true, "batcalc.>>");
}

mal_export str CMDbatLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalclt, BATcalcltcst, BATcalccstlt,
						 "batcalc.<");
}

mal_export str CMDbatLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcle, BATcalclecst, BATcalccstle,
						 "batcalc.<=");
}

mal_export str CMDbatGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcgt, BATcalcgtcst, BATcalccstgt,
						 "batcalc.>");
}

mal_export str CMDbatGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalcge, BATcalcgecst, BATcalccstge,
						 "batcalc.>=");
}

mal_export str CMDbatEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalceq, BATcalceqcst, BATcalccsteq,
						 false, "batcalc.==");
}

mal_export str CMDbatNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY1(stk, pci, BATcalcne, BATcalcnecst, BATcalccstne,
						 false, "batcalc.!=");
}

mal_export str CMDbatCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	return CMDbatBINARY0(stk, pci, BATcalccmp, BATcalccmpcst, BATcalccstcmp,
						 "batcalc.cmp");
}

mal_export str CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDbatBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat bid;
	BAT *bn, *b = NULL, *lo = NULL, *hi = NULL, *s = NULL, *slo = NULL, *shi = NULL, *r = NULL;
	int tp1, tp2, tp3, tp;
	int bc = 0;					/* number of extra BAT arguments */
	bool symmetric, linc, hinc, nils_false, anti, has_cand = false;

	(void) cntxt;
	(void) mb;

	tp1 = stk->stk[getArg(pci, 1)].vtype;
	tp2 = stk->stk[getArg(pci, 2)].vtype;
	tp3 = stk->stk[getArg(pci, 3)].vtype;
	if (tp1 != TYPE_bat && !isaBatType(tp1))
		goto bailout;
	bid = *getArgReference_bat(stk, pci, 1);
	b = BATdescriptor(bid);
	if (b == NULL)
		goto bailout;
	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		bid = *getArgReference_bat(stk, pci, 2);
		lo = BATdescriptor(bid);
		if (lo == NULL)
			goto bailout;
	}
	if (tp3 == TYPE_bat || isaBatType(tp3)) {
		bid = *getArgReference_bat(stk, pci, 3);
		hi = BATdescriptor(bid);
		if (hi == NULL)
			goto bailout;
	}
	tp = getArgType(mb, pci, 4);
	if (tp == TYPE_bat || isaBatType(tp)) {
		bid = *getArgReference_bat(stk, pci, 4);
		has_cand = true;
		if (!is_bat_nil(bid)) {
			s = BATdescriptor(bid);
			if (s == NULL)
				goto bailout;
			if (s->ttype == TYPE_bit) {
				r = s;
				s = NULL;
				has_cand = false;
			}
		}
		bc++;
	}
	if (has_cand && lo) {
		tp = getArgType(mb, pci, 4 + bc);
		if (tp == TYPE_bat || isaBatType(tp)) {
			bid = *getArgReference_bat(stk, pci, 4 + bc);
			if (!is_bat_nil(bid)) {
				slo = BATdescriptor(bid);
				if (slo == NULL)
					goto bailout;
			}
			bc++;
		} else {
			if (s == NULL) {
				/* apparently the extra bat was a NIL conditional
				 * execution bat */
				has_cand = false;
			} else
				goto bailout;
		}
	}
	if (has_cand && hi) {
		tp = getArgType(mb, pci, 4 + bc);
		if (tp != TYPE_bat && !isaBatType(tp))
			goto bailout;
		bid = *getArgReference_bat(stk, pci, 4 + bc);
		if (!is_bat_nil(bid)) {
			shi = BATdescriptor(bid);
			if (shi == NULL)
				goto bailout;
		}
		bc++;
	}
	tp = getArgType(mb, pci, 4 + bc);
	if (r == NULL && (tp == TYPE_bat || isaBatType(tp))) {
		bid = *getArgReference_bat(stk, pci, 4 + bc);
		if (!is_bat_nil(bid)) {
			r = BATdescriptor(bid);
			if (r == NULL)
				goto bailout;
			if (r->ttype != TYPE_bit)
				goto bailout;
		}
		bc++;
	}

	symmetric = *getArgReference_bit(stk, pci, bc + 4);
	linc = *getArgReference_bit(stk, pci, bc + 5);
	hinc = *getArgReference_bit(stk, pci, bc + 6);
	nils_false = *getArgReference_bit(stk, pci, bc + 7);
	anti = *getArgReference_bit(stk, pci, bc + 8);

	if (b && lo && hi)
		bn = BATcalcbetween(b, lo, hi, s, slo, shi, r,
							symmetric, linc, hinc, nils_false, anti);
	else if (b && lo)
		bn = BATcalcbetweenbatcst(b, lo, &stk->stk[getArg(pci, 3)], s, slo, r,
								  symmetric, linc, hinc, nils_false, anti);
	else if (b && hi)
		bn = BATcalcbetweencstbat(b, &stk->stk[getArg(pci, 2)], hi, s, shi, r,
								  symmetric, linc, hinc, nils_false, anti);
	else
		bn = BATcalcbetweencstcst(b, &stk->stk[getArg(pci, 2)],
								  &stk->stk[getArg(pci, 3)], s, r,
								  symmetric, linc, hinc, nils_false, anti);
	BBPunfix(b->batCacheid);
	if (lo)
		BBPunfix(lo->batCacheid);
	if (hi)
		BBPunfix(hi->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (slo)
		BBPunfix(slo->batCacheid);
	if (shi)
		BBPunfix(shi->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn == NULL)
		return mythrow(MAL, "batcalc.between", OPERATION_FAILED);
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;

  bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (lo)
		BBPunfix(lo->batCacheid);
	if (hi)
		BBPunfix(hi->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (slo)
		BBPunfix(slo->batCacheid);
	if (shi)
		BBPunfix(shi->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	throw(MAL, "batcalc.between", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
}

mal_export str CMDcalcavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDcalcavg(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	dbl avg;
	BUN vals;
	bat bid;
	BAT *b, *s = NULL;
	gdk_return ret;
	int scale = 0;

	(void) cntxt;
	(void) mb;

	bid = *getArgReference_bat(stk, pci, pci->retc + 0);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((pci->argc == pci->retc + 2 &&
		 stk->stk[pci->argv[pci->retc + 1]].vtype == TYPE_bat) ||
		pci->argc == pci->retc + 3) {
		bid = *getArgReference_bat(stk, pci, pci->retc + 1);
		if (!is_bat_nil(bid) && (s = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	}
	if (pci->argc >= pci->retc + 2 &&
		stk->stk[pci->argv[pci->argc - 1]].vtype == TYPE_int) {
		scale = *getArgReference_int(stk, pci, pci->argc - 1);
	}
	ret = BATcalcavg(b, s, &avg, &vals, scale);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (ret != GDK_SUCCEED)
		return mythrow(MAL, "aggr.avg", OPERATION_FAILED);
	* getArgReference_dbl(stk, pci, 0) = avg;
	if (pci->retc == 2)
		* getArgReference_lng(stk, pci, 1) = vals;
	return MAL_SUCCEED;
}

static str
CMDconvertbat(MalStkPtr stk, InstrPtr pci, int tp, bool abort_on_error)
{
	bat bid;
	BAT *b, *bn, *s = NULL, *r = NULL;

	bid = *getArgReference_bat(stk, pci, 1);
	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "batcalc.convert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (pci->argc == 3) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid) && (s = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.convert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		switch (s->ttype) {
		case TYPE_oid:
		case TYPE_void:
			break;
		case TYPE_bit:
			r = s;
			s = NULL;
			break;
		default:
			BBPunfix(b->batCacheid);
			BBPunfix(s->batCacheid);
			throw(MAL, "batcalc.convert", SQLSTATE(42000) ILLEGAL_ARGUMENT);
		}
	} else if (pci->argc == 4) {
		bid = *getArgReference_bat(stk, pci, 2);
		if (!is_bat_nil(bid) && (s = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.convert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		bid = *getArgReference_bat(stk, pci, 3);
		if (!is_bat_nil(bid) && (r = BATdescriptor(bid)) == NULL) {
			BBPunfix(b->batCacheid);
			BBPunfix(s->batCacheid);
			throw(MAL, "batcalc.convert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	}

	bn = BATconvert(b, s, r, tp, abort_on_error);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (r)
		BBPunfix(r->batCacheid);
	if (bn == NULL) {
		char buf[20];
		snprintf(buf, sizeof(buf), "batcalc.%s", ATOMname(tp));
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

mal_export str CMDconvert_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_bit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDconvert_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_bte(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDconvert_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_sht(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDconvert_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDconvert_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_lng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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
mal_export str CMDconvert_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_hge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDconvert_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_flt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDconvert_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_dbl(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDconvert_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_oid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDconvert_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDconvertsignal_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

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

mal_export str CMDifthen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDifthen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *b1 = NULL, *b2 = NULL, *bn;
	int tp0, tp1, tp2;
	bat *ret;
	BUN cnt = BUN_NONE;

	(void) cntxt;
	(void) mb;

	if (pci->argc != 4)
		throw(MAL, "batcalc.ifthen", "Operation not supported.");

	ret = getArgReference_bat(stk, pci, 0);
	tp0 = stk->stk[getArg(pci, 1)].vtype;
	tp1 = stk->stk[getArg(pci, 2)].vtype;
	tp2 = stk->stk[getArg(pci, 3)].vtype;
	if (tp0 == TYPE_bat || isaBatType(tp0)) {
		b = BATdescriptor(* getArgReference_bat(stk, pci, 1));
		if (b == NULL)
			throw(MAL, "batcalc.ifthenelse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		cnt = BATcount(b);
	}
	if (tp1 == TYPE_bat || isaBatType(tp1)) {
		b1 = BATdescriptor(* getArgReference_bat(stk, pci, 2));
		if (b1 == NULL) {
			if (b)
				BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.ifthenelse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (cnt == BUN_NONE)
			cnt = BATcount(b1);
		else if (BATcount(b1) != cnt) {
			BBPunfix(b->batCacheid);
			throw(MAL, "batcalc.ifthenelse", ILLEGAL_ARGUMENT);
		}
	}
	if (tp2 == TYPE_bat || isaBatType(tp2)) {
		b2 = BATdescriptor(* getArgReference_bat(stk, pci, 3));
		if (b2 == NULL) {
			if (b)
				BBPunfix(b->batCacheid);
			if (b1)
				BBPunfix(b1->batCacheid);
			throw(MAL, "batcalc.ifthenelse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (cnt == BUN_NONE)
			cnt = BATcount(b2);
		else if (BATcount(b2) != cnt) {
			if (b)
				BBPunfix(b->batCacheid);
			if (b1)
				BBPunfix(b1->batCacheid);
			throw(MAL, "batcalc.ifthenelse", ILLEGAL_ARGUMENT);
		}
	}
	if (b == NULL && b1 == NULL && b2 == NULL) {
		/* at least one BAT required */
		throw(MAL, "batcalc.ifthenelse", ILLEGAL_ARGUMENT);
	}
	if (b != NULL) {
		if (b1 != NULL) {
			if (b2 != NULL) {
				bn = BATcalcifthenelse(b, b1, b2);
			} else {
				bn = BATcalcifthenelsecst(b, b1, &stk->stk[getArg(pci, 3)]);
			}
		} else {
			if (b2 != NULL) {
				bn = BATcalcifthencstelse(b, &stk->stk[getArg(pci, 2)], b2);
			} else {
				bn = BATcalcifthencstelsecst(b, &stk->stk[getArg(pci, 2)], &stk->stk[getArg(pci, 3)]);
			}
		}
	} else {
		bit v = *getArgReference_bit(stk, pci, 1);
		if (is_bit_nil(v)) {
			if (b1 != NULL)
				bn = BATconstant(b1->hseqbase, b1->ttype, ATOMnilptr(b1->ttype), BATcount(b1), TRANSIENT);
			else
				bn = BATconstant(b2->hseqbase, b2->ttype, ATOMnilptr(b2->ttype), BATcount(b2), TRANSIENT);
		} else if (v) {
			if (b1 != NULL)
				bn = COLcopy(b1, b1->ttype, false, TRANSIENT);
			else
				bn = BATconstant(b2->hseqbase, b2->ttype, VALptr(&stk->stk[getArg(pci, 2)]), BATcount(b2), TRANSIENT);
		} else {
			if (b2 != NULL)
				bn = COLcopy(b2, b2->ttype, false, TRANSIENT);
			else
				bn = BATconstant(b1->hseqbase, b1->ttype, VALptr(&stk->stk[getArg(pci, 3)]), BATcount(b1), TRANSIENT);
		}
	}
	if (b)
		BBPunfix(b->batCacheid);
	if (b1)
		BBPunfix(b1->batCacheid);
	if (b2)
		BBPunfix(b2->batCacheid);
	if (bn == NULL) {
		return mythrow(MAL, "batcalc.ifthenelse", OPERATION_FAILED);
	}
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}
