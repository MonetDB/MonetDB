/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
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
CMDvarSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcsub(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) != GDK_SUCCEED)
		return mythrow(MAL, "calc.-", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcsub(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) != GDK_SUCCEED)
		return mythrow(MAL, "calc.sub_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcadd(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) != GDK_SUCCEED)
		return mythrow(MAL, "calc.+", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcadd(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) != GDK_SUCCEED)
		return mythrow(MAL, "calc.add_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarADDstr(str *ret, str *s1, str *s2)
{
	str s;
	size_t l1;

	if (strNil(*s1) || strNil(*s2)) {
		*ret= GDKstrdup(str_nil);
		if (*ret == NULL)
			return mythrow(MAL, "calc.+", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	s = GDKzalloc((l1 = strlen(*s1)) + strlen(*s2) + 1);
	if (s == NULL)
		return mythrow(MAL, "calc.+", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	strcpy(s, *s1);
	strcpy(s + l1, *s2);
	*ret = s;
	return MAL_SUCCEED;
}


static str
CMDvarADDstrint(str *ret, str *s1, int *i)
{
	str s;
	size_t len;

	if (strNil(*s1) || is_int_nil(*i)) {
		*ret= GDKstrdup(str_nil);
		if (*ret == NULL)
			return mythrow(MAL, "calc.+", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	len = strlen(*s1) + 16;		/* maxint = 2147483647 which fits easily */
	s = GDKmalloc(len);
	if (s == NULL)
		return mythrow(MAL, "calc.+", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	snprintf(s, len, "%s%d", *s1, *i);
	*ret = s;
	return MAL_SUCCEED;
}


static str
CMDvarMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcmul(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) != GDK_SUCCEED)
		return mythrow(MAL, "calc.*", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcmul(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) != GDK_SUCCEED)
		return mythrow(MAL, "calc.mul_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcdiv(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) != GDK_SUCCEED)
		return mythrow(MAL, "calc./", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcdiv(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) != GDK_SUCCEED)
		return mythrow(MAL, "calc.div_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcmod(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) != GDK_SUCCEED)
		return mythrow(MAL, "calc.%", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcmod(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) != GDK_SUCCEED)
		return mythrow(MAL, "calc.modmod", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalclsh(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) != GDK_SUCCEED)
		return mythrow(MAL, "calc.<<", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalclsh(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) != GDK_SUCCEED)
		return mythrow(MAL, "calc.lsh_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcrsh(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) != GDK_SUCCEED)
		return mythrow(MAL, "calc.>>", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcrsh(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) != GDK_SUCCEED)
		return mythrow(MAL, "calc.rsh_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcand(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
		return mythrow(MAL, "calc.and", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcor(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
		return mythrow(MAL, "calc.or", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcxor(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
		return mythrow(MAL, "calc.xor", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalclt(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
		return mythrow(MAL, "calc.<", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcle(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
		return mythrow(MAL, "calc.<=", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcgt(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
		return mythrow(MAL, "calc.>", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcge(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
		return mythrow(MAL, "calc.>=", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalceq(&stk->stk[getArg(pci, 0)],
				  &stk->stk[getArg(pci, 1)],
				  &stk->stk[getArg(pci, 2)],
				  pci->argc == 3 ? false : *getArgReference_bit(stk, pci, 3)
			) != GDK_SUCCEED)
		return mythrow(MAL, "calc.==", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcne(&stk->stk[getArg(pci, 0)],
				  &stk->stk[getArg(pci, 1)],
				  &stk->stk[getArg(pci, 2)],
				  pci->argc == 3 ? false : *getArgReference_bit(stk, pci, 3)
			) != GDK_SUCCEED)
		return mythrow(MAL, "calc.!=", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalccmp(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) != GDK_SUCCEED)
		return mythrow(MAL, "calc.cmp", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDvarBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bool symmetric, linc, hinc, nils_false, anti;

	symmetric = *getArgReference_bit(stk, pci, 4);
	linc = *getArgReference_bit(stk, pci, 5);
	hinc = *getArgReference_bit(stk, pci, 6);
	nils_false = *getArgReference_bit(stk, pci, 7);
	anti = *getArgReference_bit(stk, pci, 8);
	if (VARcalcbetween(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], &stk->stk[getArg(pci, 3)], symmetric, linc, hinc, nils_false, anti) != GDK_SUCCEED)
		return mythrow(MAL, "calc.between", OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDstrlength(int *ret, str *v)
{
	size_t l = strlen(*v);

	if (l > (size_t) GDK_int_max)
		return mythrow(MAL, "calc.length", OPERATION_FAILED);
	*ret = (int) l;
	return MAL_SUCCEED;
}


static str
CMDvarCONVERT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARconvert(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], 1, 0, 0, 0) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarCONVERTptr(ptr *ret, ptr *v)
{
	*ret = *v;
	return MAL_SUCCEED;
}


static str
CMDvarISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalciszero(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcisnil(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcisnotnil(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcnot(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcabsolute(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcsign(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcnegate(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarINCRsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcincr(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], 1) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CMDvarDECRsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcdecr(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], 1) != GDK_SUCCEED) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}


static str
CALCswitchbit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	ptr p;
	ptr retval = getArgReference(stk, pci, 0);
	bit b = stk->stk[getArg(pci, 1)].vtype == TYPE_msk
		? (bit) *getArgReference_msk(stk, pci, 1)
		: *getArgReference_bit(stk, pci, 1);
	int t1 = getArgType(mb, pci, 2);
	int t2 = getArgType(mb, pci, 3);

	(void) cntxt;
	if (t1 != t2)
		return mythrow(MAL, "ifthenelse", SEMANTIC_TYPE_MISMATCH);

	if (b && !is_bit_nil(b)) {
		p = getArgReference(stk, pci, 2);
	} else {
		p = getArgReference(stk, pci, 3);
	}
	if (ATOMextern(t1)) {
		*(ptr **) retval = ATOMdup(t1, *(ptr**)p);
		if (*(ptr **) retval == NULL)
			throw(MAL, "ifthenelse", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else if (t1 == TYPE_void) {
		memcpy(retval, p, sizeof(oid));
	} else {
		memcpy(retval, p, ATOMsize(t1));
	}
	return MAL_SUCCEED;
}


static str
CALCmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int t = getArgType(mb, pci, 1);
	const void *p1 = getArgReference(stk, pci, 1);
	const void *p2 = getArgReference(stk, pci, 2);
	const void *nil;

	(void) cntxt;
	if (t != getArgType(mb, pci, 2))
		return mythrow(MAL, "calc.min", SEMANTIC_TYPE_MISMATCH);
	nil = ATOMnilptr(t);
	if (t >= TYPE_str && ATOMstorage(t) >= TYPE_str) {
		p1 = *(ptr *)p1;
		p2 = *(ptr *)p2;
	}
	if (ATOMcmp(t, p1, nil) == 0 || ATOMcmp(t, p2, nil) == 0)
		p1 = nil;
	else if (ATOMcmp(t, p1, p2) > 0)
		p1 = p2;
	if (VALinit(&stk->stk[getArg(pci, 0)], t, p1) == NULL)
		return mythrow(MAL, "calc.min", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}


static str
CALCmin_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int t = getArgType(mb, pci, 1);
	ptr p1 = getArgReference(stk, pci, 1);
	ptr p2 = getArgReference(stk, pci, 2);
	const void *nil;

	(void) cntxt;
	if (t != getArgType(mb, pci, 2))
		return mythrow(MAL, "calc.min", SEMANTIC_TYPE_MISMATCH);
	nil = ATOMnilptr(t);
	if (t >= TYPE_str && ATOMstorage(t) >= TYPE_str) {
		p1 = *(ptr *)p1;
		p2 = *(ptr *)p2;
	}
	if (ATOMcmp(t, p1, nil) == 0 ||
		(ATOMcmp(t, p2, nil) != 0 && ATOMcmp(t, p1, p2) > 0))
		p1 = p2;
	if (VALinit(&stk->stk[getArg(pci, 0)], t, p1) == NULL)
		return mythrow(MAL, "calc.min", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}


static str
CALCmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int t = getArgType(mb, pci, 1);
	const void *p1 = getArgReference(stk, pci, 1);
	const void *p2 = getArgReference(stk, pci, 2);
	const void *nil;

	(void) cntxt;
	if (t != getArgType(mb, pci, 2))
		return mythrow(MAL, "calc.max", SEMANTIC_TYPE_MISMATCH);
	nil = ATOMnilptr(t);
	if (t >= TYPE_str && ATOMstorage(t) >= TYPE_str) {
		p1 = *(ptr *)p1;
		p2 = *(ptr *)p2;
	}
	if (ATOMcmp(t, p1, nil) == 0 || ATOMcmp(t, p2, nil) == 0)
		p1 = nil;
	else if (ATOMcmp(t, p1, p2) < 0)
		p1 = p2;
	if (VALinit(&stk->stk[getArg(pci, 0)], t, p1) == NULL)
		return mythrow(MAL, "calc.max", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}


static str
CALCmax_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int t = getArgType(mb, pci, 1);
	ptr p1 = getArgReference(stk, pci, 1);
	ptr p2 = getArgReference(stk, pci, 2);
	const void *nil;

	(void) cntxt;
	if (t != getArgType(mb, pci, 2))
		return mythrow(MAL, "calc.max", SEMANTIC_TYPE_MISMATCH);
	nil = ATOMnilptr(t);
	if (t >= TYPE_str && ATOMstorage(t) >= TYPE_str) {
		p1 = *(ptr *)p1;
		p2 = *(ptr *)p2;
	}
	if (ATOMcmp(t, p1, nil) == 0 ||
		(ATOMcmp(t, p2, nil) != 0 && ATOMcmp(t, p1, p2) < 0))
		p1 = p2;
	if (VALinit(&stk->stk[getArg(pci, 0)], t, p1) == NULL)
		return mythrow(MAL, "calc.max", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CMDBATsumprod(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
			  gdk_return (*sumprod)(void *, int, BAT *, BAT *, bool, bool, bool),
			  const char *func)
{
	ValPtr ret = &stk->stk[getArg(pci, 0)];
	bat bid = * getArgReference_bat(stk, pci, 1);
	BAT *b;
	BAT *s = NULL;
	bool nil_if_empty = true;
	gdk_return r;

	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, func, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (pci->argc >= 3) {
		if (getArgType(mb, pci, 2) == TYPE_bit) {
			assert(pci->argc == 3);
			nil_if_empty = * getArgReference_bit(stk, pci, 2);
		} else {
			bat sid = * getArgReference_bat(stk, pci, 2);
			if (!is_bat_nil(sid) && (s = BATdescriptor(sid)) == NULL) {
				BBPunfix(b->batCacheid);
				throw(MAL, func, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			}
			if (pci->argc >= 4) {
				assert(pci->argc == 4);
				assert(getArgType(mb, pci, 3) == TYPE_bit);
				nil_if_empty = * getArgReference_bit(stk, pci, 3);
			}
		}
	}
	r = (*sumprod)(VALget(ret), ret->vtype, b, s, true, true, nil_if_empty);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (r != GDK_SUCCEED)
		return mythrow(MAL, func, OPERATION_FAILED);
	return MAL_SUCCEED;
}


static str
CMDBATsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDBATsumprod(mb, stk, pci, BATsum, "aggr.sum");
}


static str
CMDBATprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDBATsumprod(mb, stk, pci, BATprod, "aggr.prod");
}

#define arg_type(stk, pci, k) ((stk)->stk[pci->argv[k]].vtype)

static str
CMDBATavg3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	ValPtr ret = &stk->stk[getArg(pci, 0)];
	lng *rest = NULL, *cnt = NULL;
	bat *bid, *sid;
	bit *skip_nils;
	BAT *b = NULL, *s = NULL, *avgs, *cnts, *rems;

	gdk_return rc;
	(void)cntxt;
	(void)mb;

	/* optional results rest and count */
	if (arg_type(stk, pci, 1) == TYPE_lng)
		rest = getArgReference_lng(stk, pci, 1);
	if (arg_type(stk, pci, 2) == TYPE_lng)
		cnt = getArgReference_lng(stk, pci, 2);
	bid = getArgReference_bat(stk, pci, 3);
	sid = getArgReference_bat(stk, pci, 4);
	skip_nils = getArgReference_bit(stk, pci, 5);
	b = BATdescriptor(*bid);
	s = sid != NULL && !is_bat_nil(*sid) ? BATdescriptor(*sid) : NULL;
	if (b == NULL ||
		(sid != NULL && !is_bat_nil(*sid) && s == NULL)) {
		if (b)
			BBPunfix(b->batCacheid);
		throw(MAL, "aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	rc = BATgroupavg3(&avgs, &rems, &cnts, b, NULL, NULL, s, *skip_nils);
	if (avgs && BATcount(avgs) == 1) {
		/* only type bte, sht, int, lng and hge */
		ptr res = VALget(ret);
		lng xcnt = 0;

		if (avgs->ttype == TYPE_bte) {
			*(bte*)res = *(bte*) Tloc(avgs, 0);
		} else if (avgs->ttype == TYPE_sht) {
			*(sht*)res = *(sht*) Tloc(avgs, 0);
		} else if (avgs->ttype == TYPE_int) {
			*(int*)res = *(int*) Tloc(avgs, 0);
		} else if (avgs->ttype == TYPE_lng) {
			*(lng*)res = *(lng*) Tloc(avgs, 0);
#ifdef HAVE_HGE
		} else if (avgs->ttype == TYPE_hge) {
			*(hge*)res = *(hge*) Tloc(avgs, 0);
#endif
		}
		if (cnt)
			xcnt = *cnt = *(lng*) Tloc(cnts, 0);
		if (rest)
			*rest = *(lng*) Tloc(rems, 0);
		if (xcnt == 0)
			VALset(ret, ret->vtype, (ptr)ATOMnilptr(ret->vtype));
	} else {
		VALset(ret, ret->vtype, (ptr)ATOMnilptr(ret->vtype));
		if (rest)
			*rest = lng_nil;
		if (cnt)
			*cnt = lng_nil;
	}
	if (avgs)
		BBPunfix(avgs->batCacheid);
	if (rems)
		BBPunfix(rems->batCacheid);
	if (cnts)
		BBPunfix(cnts->batCacheid);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (rc != GDK_SUCCEED)
		return mythrow(MAL, "aggr.avg", OPERATION_FAILED);
	return MAL_SUCCEED;
}

static str
CMDBATavg3comb(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	ValPtr ret = &stk->stk[getArg(pci, 0)];
	BAT *b = NULL, *r = NULL, *c = NULL, *avgs;
	bat *bid = getArgReference_bat(stk, pci, 1);
	bat *rid = getArgReference_bat(stk, pci, 2);
	bat *cid = getArgReference_bat(stk, pci, 3);

	(void)cntxt;
	(void)mb;

	b = BATdescriptor(*bid);
	r = BATdescriptor(*rid);
	c = BATdescriptor(*cid);
	if (b == NULL || r == NULL || c == NULL) {
		if (b)
			BBPunfix(b->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		if (c)
			BBPunfix(c->batCacheid);
		throw(MAL, "aggr.avg", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	avgs = BATgroupavg3combine(b, r, c, NULL, NULL, TRUE);
	if (avgs && BATcount(avgs) == 1) {
		/* only type bte, sht, int, lng and hge */
		ptr res = VALget(ret);

		if (avgs->ttype == TYPE_bte) {
			*(bte*)res = *(bte*) Tloc(avgs, 0);
		} else if (avgs->ttype == TYPE_sht) {
			*(sht*)res = *(sht*) Tloc(avgs, 0);
		} else if (avgs->ttype == TYPE_int) {
			*(int*)res = *(int*) Tloc(avgs, 0);
		} else if (avgs->ttype == TYPE_lng) {
			*(lng*)res = *(lng*) Tloc(avgs, 0);
#ifdef HAVE_HGE
		} else if (avgs->ttype == TYPE_hge) {
			*(hge*)res = *(hge*) Tloc(avgs, 0);
#endif
		}
	} else {
		VALset(ret, ret->vtype, (ptr)ATOMnilptr(ret->vtype));
	}
	if (avgs)
		BBPunfix(avgs->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(r->batCacheid);
	BBPunfix(c->batCacheid);
	if (avgs == NULL)
		throw(MAL, "aggr.avg", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
CMDBATstr_group_concat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	ValPtr ret = &stk->stk[getArg(pci, 0)];
	bat bid = *getArgReference_bat(stk, pci, 1), sid = 0;
	BAT *b, *s = NULL, *sep = NULL;
	bool nil_if_empty = true;
	int next_argument = 2;
	const char *separator = ",";
	gdk_return r;

	(void) cntxt;

	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, "aggr.str_group_concat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (isaBatType(getArgType(mb, pci, 2))) {
		sid = *getArgReference_bat(stk, pci, 2);
		if ((sep = BATdescriptor(sid)) == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "aggr.str_group_concat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (sep->ttype == TYPE_str) { /* the separator bat */
			next_argument = 3;
			separator = NULL;
		}
	}

	if (pci->argc >= (next_argument + 1)) {
		if (getArgType(mb, pci, next_argument) == TYPE_bit) {
			assert(pci->argc == (next_argument + 1));
			nil_if_empty = *getArgReference_bit(stk, pci, next_argument);
		} else {
			if (next_argument == 3) {
				bat sid = *getArgReference_bat(stk, pci, next_argument);
				if (!is_bat_nil(sid) && (s = BATdescriptor(sid)) == NULL) {
					BBPunfix(b->batCacheid);
					BBPunfix(sep->batCacheid);
					throw(MAL, "aggr.str_group_concat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
				}
			} else {
				s = sep;
				sep = NULL;
			}
			if (pci->argc >= (next_argument + 2)) {
				assert(pci->argc == (next_argument + 2));
				assert(getArgType(mb, pci, (next_argument + 1)) == TYPE_bit);
				nil_if_empty = * getArgReference_bit(stk, pci, (next_argument + 1));
			}
		}
	}

	assert((separator && !sep) || (!separator && sep));
	r = BATstr_group_concat(ret, b, s, sep, true, true, nil_if_empty, separator);
	BBPunfix(b->batCacheid);
	if (sep)
		BBPunfix(sep->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (r != GDK_SUCCEED)
		return mythrow(MAL, "aggr.str_group_concat", OPERATION_FAILED);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func calc_init_funcs[] = {
#ifdef HAVE_HGE
 pattern("calc", "iszero", CMDvarISZERO, false, "Unary check for zero of V", args(1,2, arg("",bit),arg("v",hge))),
 pattern("calc", "not", CMDvarNOT, false, "Unary bitwise not of V", args(1,2, arg("",hge),arg("v",hge))),
 pattern("calc", "sign", CMDvarSIGN, false, "Unary sign (-1,0,1) of V", args(1,2, arg("",bte),arg("v",hge))),
 pattern("calc", "abs", CMDvarABS, false, "Unary absolute value of V", args(1,2, arg("",hge),arg("v",hge))),
 pattern("calc", "-", CMDvarNEG, false, "Unary negation of V", args(1,2, arg("",hge),arg("v",hge))),
 pattern("calc", "++", CMDvarINCRsignal, false, "Unary V + 1", args(1,2, arg("",hge),arg("v",hge))),
 pattern("calc", "--", CMDvarDECRsignal, false, "Unary V - 1", args(1,2, arg("",hge),arg("v",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",int),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",hge),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",int),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",flt),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",flt),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",int),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",int),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",hge),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",int),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",hge),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",hge),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",hge),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",hge),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "and", CMDvarAND, false, "Return V1 AND V2", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "or", CMDvarOR, false, "Return V1 OR V2", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "xor", CMDvarXOR, false, "Return V1 XOR V2", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",hge))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",hge))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",hge))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",hge))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",hge))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",hge))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",bte))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",sht))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",int))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",lng))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",hge),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",hge))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",int))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",hge))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",int))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",hge))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",hge))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",hge))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",hge))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",bte))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",sht))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",int))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",lng))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",hge))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",flt))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",hge))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",hge))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",hge))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",hge))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",hge))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",bte))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",sht))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",int))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",lng))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",hge))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",flt))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",hge))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",hge))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",int))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",hge))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",int))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",hge),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",hge),arg("nil_matches",bit))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",hge))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",sht),arg("v2",hge))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",int),arg("v2",hge))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",lng),arg("v2",hge))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",hge),arg("v2",bte))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",hge),arg("v2",sht))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",hge),arg("v2",int))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",hge),arg("v2",lng))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",hge),arg("v2",hge))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",hge),arg("v2",flt))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",hge),arg("v2",dbl))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",flt),arg("v2",hge))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",hge))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",hge))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",hge))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",hge))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",hge))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",hge))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",hge))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",void))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",bit))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",bte))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",sht))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",int))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",lng))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",hge))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",flt))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",dbl))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",oid))),
 pattern("calc", "hge", CMDvarCONVERT, false, "Cast VALUE to hge", args(1,2, arg("",hge),arg("v",str))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",hge))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",hge))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",hge))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",hge),batarg("b",msk))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",hge),batarg("b",msk),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",hge),batarg("b",msk),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",hge),batarg("b",msk),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",hge),batarg("b",bte))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",hge),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",hge),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",hge),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",hge),batarg("b",sht))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",hge),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",hge),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",hge),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",hge),batarg("b",int))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",hge),batarg("b",int),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",hge),batarg("b",int),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",hge),batarg("b",int),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",hge),batarg("b",lng))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",hge),batarg("b",lng),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",hge),batarg("b",lng),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",hge),batarg("b",lng),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",hge),batarg("b",hge))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",hge),batarg("b",hge),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",hge),batarg("b",hge),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",hge),batarg("b",hge),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",dbl),batarg("b",hge))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",dbl),batarg("b",hge),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",dbl),batarg("b",hge),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",dbl),batarg("b",hge),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",hge),batarg("b",bte))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",hge),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",hge),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",hge),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",hge),batarg("b",sht))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",hge),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",hge),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",hge),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",hge),batarg("b",int))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",hge),batarg("b",int),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",hge),batarg("b",int),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",hge),batarg("b",int),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",hge),batarg("b",lng))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",hge),batarg("b",lng),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",hge),batarg("b",lng),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",hge),batarg("b",lng),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",hge),batarg("b",hge))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",hge),batarg("b",hge),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",hge),batarg("b",hge),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",hge),batarg("b",hge),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",dbl),batarg("b",hge))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",dbl),batarg("b",hge),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",dbl),batarg("b",hge),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",dbl),batarg("b",hge),batarg("s",oid),arg("nil_if_empty",bit))),
#endif

 pattern("aggr", "avg", CMDBATavg3, false, "Calculate aggregate average of B.", args(3,6, arg("",bte),arg("",lng),arg("",lng),batarg("b",bte),batarg("s",oid),arg("skip_nils",bit))),
 pattern("aggr", "avg", CMDBATavg3, false, "Calculate aggregate average of B.", args(3,6, arg("",sht),arg("",lng),arg("",lng),batarg("b",sht),batarg("s",oid),arg("skip_nils",bit))),
 pattern("aggr", "avg", CMDBATavg3, false, "Calculate aggregate average of B.", args(3,6, arg("",int),arg("",lng),arg("",lng),batarg("b",int),batarg("s",oid),arg("skip_nils",bit))),
 pattern("aggr", "avg", CMDBATavg3, false, "Calculate aggregate average of B.", args(3,6, arg("",lng),arg("",lng),arg("",lng),batarg("b",lng),batarg("s",oid),arg("skip_nils",bit))),
#ifdef HAVE_HGE
 pattern("aggr", "avg", CMDBATavg3, false, "Calculate aggregate average of B.", args(3,6, arg("",hge),arg("",lng),arg("",lng),batarg("b",hge),batarg("s",oid),arg("skip_nils",bit))),
#endif

 pattern("aggr", "avg", CMDBATavg3comb, false, "Average aggregation combiner.", args(1,4, arg("",bte),batarg("b",bte),batarg("r",lng),batarg("c",lng))),
 pattern("aggr", "avg", CMDBATavg3comb, false, "Average aggregation combiner.", args(1,4, arg("",sht),batarg("b",sht),batarg("r",lng),batarg("c",lng))),
 pattern("aggr", "avg", CMDBATavg3comb, false, "Average aggregation combiner.", args(1,4, arg("",int),batarg("b",int),batarg("r",lng),batarg("c",lng))),
 pattern("aggr", "avg", CMDBATavg3comb, false, "Average aggregation combiner.", args(1,4, arg("",lng),batarg("b",lng),batarg("r",lng),batarg("c",lng))),
#ifdef HAVE_HGE
 pattern("aggr", "avg", CMDBATavg3comb, false, "Average aggregation combiner.", args(1,4, arg("",hge),batarg("b",hge),batarg("r",lng),batarg("c",lng))),
#endif

 /* calc ops from json */
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("l",json),arg("r",json))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("l",json),arg("r",json),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("l",json),arg("r",json))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("l",json),arg("r",json),arg("nil_matches",bit))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("l",json),arg("r",json))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("l",json),arg("r",json))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("l",json),arg("r",json))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("l",json),arg("r",json))),
 /* calc ops from uuid */
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("l",uuid),arg("r",uuid))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("l",uuid),arg("r",uuid),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("l",uuid),arg("r",uuid))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("l",uuid),arg("r",uuid),arg("nil_matches",bit))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("l",uuid),arg("r",uuid))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("l",uuid),arg("r",uuid))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("l",uuid),arg("r",uuid))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("l",uuid),arg("r",uuid))),
 /* calc ops from mtime */
 pattern("calc", "==", CMDvarEQ, false, "Equality of two dates", args(1,3, arg("",bit),arg("v",date),arg("w",date))),
 pattern("calc", "==", CMDvarEQ, false, "Equality of two dates", args(1,4, arg("",bit),arg("v",date),arg("w",date),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Equality of two dates", args(1,3, arg("",bit),arg("v",date),arg("w",date))),
 pattern("calc", "!=", CMDvarNE, false, "Equality of two dates", args(1,4, arg("",bit),arg("v",date),arg("w",date),arg("nil_matches",bit))),
 pattern("calc", "<", CMDvarLT, false, "Equality of two dates", args(1,3, arg("",bit),arg("v",date),arg("w",date))),
 pattern("calc", "<=", CMDvarLE, false, "Equality of two dates", args(1,3, arg("",bit),arg("v",date),arg("w",date))),
 pattern("calc", ">", CMDvarGT, false, "Equality of two dates", args(1,3, arg("",bit),arg("v",date),arg("w",date))),
 pattern("calc", ">=", CMDvarGE, false, "Equality of two dates", args(1,3, arg("",bit),arg("v",date),arg("w",date))),
 pattern("calc", "==", CMDvarEQ, false, "Equality of two daytimes", args(1,3, arg("",bit),arg("v",daytime),arg("w",daytime))),
 pattern("calc", "==", CMDvarEQ, false, "Equality of two daytimes", args(1,4, arg("",bit),arg("v",daytime),arg("w",daytime),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Equality of two daytimes", args(1,3, arg("",bit),arg("v",daytime),arg("w",daytime))),
 pattern("calc", "!=", CMDvarNE, false, "Equality of two daytimes", args(1,4, arg("",bit),arg("v",daytime),arg("w",daytime),arg("nil_matches",bit))),
 pattern("calc", "<", CMDvarLT, false, "Equality of two daytimes", args(1,3, arg("",bit),arg("v",daytime),arg("w",daytime))),
 pattern("calc", "<=", CMDvarLE, false, "Equality of two daytimes", args(1,3, arg("",bit),arg("v",daytime),arg("w",daytime))),
 pattern("calc", ">", CMDvarGT, false, "Equality of two daytimes", args(1,3, arg("",bit),arg("v",daytime),arg("w",daytime))),
 pattern("calc", ">=", CMDvarGE, false, "Equality of two daytimes", args(1,3, arg("",bit),arg("v",daytime),arg("w",daytime))),
 pattern("calc", "==", CMDvarEQ, false, "Equality of two timestamps", args(1,3, arg("",bit),arg("v",timestamp),arg("w",timestamp))),
 pattern("calc", "==", CMDvarEQ, false, "Equality of two timestamps", args(1,4, arg("",bit),arg("v",timestamp),arg("w",timestamp),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Equality of two timestamps", args(1,3, arg("",bit),arg("v",timestamp),arg("w",timestamp))),
 pattern("calc", "!=", CMDvarNE, false, "Equality of two timestamps", args(1,4, arg("",bit),arg("v",timestamp),arg("w",timestamp),arg("nil_matches",bit))),
 pattern("calc", "<", CMDvarLT, false, "Equality of two timestamps", args(1,3, arg("",bit),arg("v",timestamp),arg("w",timestamp))),
 pattern("calc", "<=", CMDvarLE, false, "Equality of two timestamps", args(1,3, arg("",bit),arg("v",timestamp),arg("w",timestamp))),
 pattern("calc", ">", CMDvarGT, false, "Equality of two timestamps", args(1,3, arg("",bit),arg("v",timestamp),arg("w",timestamp))),
 pattern("calc", ">=", CMDvarGE, false, "Equality of two timestamps", args(1,3, arg("",bit),arg("v",timestamp),arg("w",timestamp))),
 /* calc ops from 01_calc.mal */
 pattern("calc", "iszero", CMDvarISZERO, false, "Unary check for zero of V", args(1,2, arg("",bit),arg("v",bte))),
 pattern("calc", "iszero", CMDvarISZERO, false, "Unary check for zero of V", args(1,2, arg("",bit),arg("v",sht))),
 pattern("calc", "iszero", CMDvarISZERO, false, "Unary check for zero of V", args(1,2, arg("",bit),arg("v",int))),
 pattern("calc", "iszero", CMDvarISZERO, false, "Unary check for zero of V", args(1,2, arg("",bit),arg("v",lng))),
 pattern("calc", "iszero", CMDvarISZERO, false, "Unary check for zero of V", args(1,2, arg("",bit),arg("v",flt))),
 pattern("calc", "iszero", CMDvarISZERO, false, "Unary check for zero of V", args(1,2, arg("",bit),arg("v",dbl))),
 pattern("calc", "isnil", CMDvarISNIL, false, "Unary check for nil of V", args(1,2, arg("",bit),argany("v",0))),
 pattern("calc", "isnotnil", CMDvarISNOTNIL, false, "Unary check for notnil of V", args(1,2, arg("",bit),argany("v",0))),
 pattern("calc", "not", CMDvarNOT, false, "Return the Boolean inverse", args(1,2, arg("",bit),arg("v",bit))),
 pattern("calc", "not", CMDvarNOT, false, "Unary bitwise not of V", args(1,2, arg("",bte),arg("v",bte))),
 pattern("calc", "not", CMDvarNOT, false, "Unary bitwise not of V", args(1,2, arg("",sht),arg("v",sht))),
 pattern("calc", "not", CMDvarNOT, false, "Unary bitwise not of V", args(1,2, arg("",int),arg("v",int))),
 pattern("calc", "not", CMDvarNOT, false, "Unary bitwise not of V", args(1,2, arg("",lng),arg("v",lng))),
 pattern("calc", "sign", CMDvarSIGN, false, "Unary sign (-1,0,1) of V", args(1,2, arg("",bte),arg("v",bte))),
 pattern("calc", "sign", CMDvarSIGN, false, "Unary sign (-1,0,1) of V", args(1,2, arg("",bte),arg("v",sht))),
 pattern("calc", "sign", CMDvarSIGN, false, "Unary sign (-1,0,1) of V", args(1,2, arg("",bte),arg("v",int))),
 pattern("calc", "sign", CMDvarSIGN, false, "Unary sign (-1,0,1) of V", args(1,2, arg("",bte),arg("v",lng))),
 pattern("calc", "sign", CMDvarSIGN, false, "Unary sign (-1,0,1) of V", args(1,2, arg("",bte),arg("v",flt))),
 pattern("calc", "sign", CMDvarSIGN, false, "Unary sign (-1,0,1) of V", args(1,2, arg("",bte),arg("v",dbl))),
 pattern("calc", "abs", CMDvarABS, false, "Unary absolute value of V", args(1,2, arg("",bte),arg("v",bte))),
 pattern("calc", "abs", CMDvarABS, false, "Unary absolute value of V", args(1,2, arg("",sht),arg("v",sht))),
 pattern("calc", "abs", CMDvarABS, false, "Unary absolute value of V", args(1,2, arg("",int),arg("v",int))),
 pattern("calc", "abs", CMDvarABS, false, "Unary absolute value of V", args(1,2, arg("",lng),arg("v",lng))),
 pattern("calc", "abs", CMDvarABS, false, "Unary absolute value of V", args(1,2, arg("",flt),arg("v",flt))),
 pattern("calc", "abs", CMDvarABS, false, "Unary absolute value of V", args(1,2, arg("",dbl),arg("v",dbl))),
 pattern("calc", "-", CMDvarNEG, false, "Unary negation of V", args(1,2, arg("",bte),arg("v",bte))),
 pattern("calc", "-", CMDvarNEG, false, "Unary negation of V", args(1,2, arg("",sht),arg("v",sht))),
 pattern("calc", "-", CMDvarNEG, false, "Unary negation of V", args(1,2, arg("",int),arg("v",int))),
 pattern("calc", "-", CMDvarNEG, false, "Unary negation of V", args(1,2, arg("",lng),arg("v",lng))),
 pattern("calc", "-", CMDvarNEG, false, "Unary negation of V", args(1,2, arg("",flt),arg("v",flt))),
 pattern("calc", "-", CMDvarNEG, false, "Unary negation of V", args(1,2, arg("",dbl),arg("v",dbl))),
 pattern("calc", "++", CMDvarINCRsignal, false, "Unary V + 1", args(1,2, arg("",bte),arg("v",bte))),
 pattern("calc", "++", CMDvarINCRsignal, false, "Unary V + 1", args(1,2, arg("",sht),arg("v",sht))),
 pattern("calc", "++", CMDvarINCRsignal, false, "Unary V + 1", args(1,2, arg("",int),arg("v",int))),
 pattern("calc", "++", CMDvarINCRsignal, false, "Unary V + 1", args(1,2, arg("",lng),arg("v",lng))),
 pattern("calc", "++", CMDvarINCRsignal, false, "Unary V + 1", args(1,2, arg("",flt),arg("v",flt))),
 pattern("calc", "++", CMDvarINCRsignal, false, "Unary V + 1", args(1,2, arg("",dbl),arg("v",dbl))),
 pattern("calc", "--", CMDvarDECRsignal, false, "Unary V - 1", args(1,2, arg("",bte),arg("v",bte))),
 pattern("calc", "--", CMDvarDECRsignal, false, "Unary V - 1", args(1,2, arg("",sht),arg("v",sht))),
 pattern("calc", "--", CMDvarDECRsignal, false, "Unary V - 1", args(1,2, arg("",int),arg("v",int))),
 pattern("calc", "--", CMDvarDECRsignal, false, "Unary V - 1", args(1,2, arg("",lng),arg("v",lng))),
 pattern("calc", "--", CMDvarDECRsignal, false, "Unary V - 1", args(1,2, arg("",flt),arg("v",flt))),
 pattern("calc", "--", CMDvarDECRsignal, false, "Unary V - 1", args(1,2, arg("",dbl),arg("v",dbl))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",sht),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "+", CMDvarADDsignal, false, "Return V1 + V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "add_noerror", CMDvarADD, false, "Return V1 + V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",sht),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "-", CMDvarSUBsignal, false, "Return V1 - V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "sub_noerror", CMDvarSUB, false, "Return V1 - V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",sht),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",bte),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",int),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",sht),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",lng),arg("v1",int),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",int),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",int),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",flt),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, guarantee no overflow by returning larger type", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",int),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",int),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",int),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",int),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",int),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",flt),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",flt),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",flt),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",flt),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",flt),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",flt),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",flt),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",flt),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",int),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",int),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "*", CMDvarMULsignal, false, "Return V1 * V2, signal error on overflow", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "mul_noerror", CMDvarMUL, false, "Return V1 * V2, overflow results in NIL value", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",lng))),
 command("calc", "+", CMDvarADDstr, false, "Concatenate LEFT and RIGHT", args(1,3, arg("",str),arg("v1",str),arg("v2",str))),
 command("calc", "+", CMDvarADDstrint, false, "Concatenate LEFT and string representation of RIGHT", args(1,3, arg("",str),arg("v1",str),arg("i",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",bte),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",sht),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",int),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",int),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",int),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",int),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",int),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",int),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",int),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",int),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",int),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",lng),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",flt),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",flt),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "/", CMDvarDIVsignal, false, "Return V1 / V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "div_noerror", CMDvarDIV, false, "Return V1 / V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",int),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",int),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",int),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",int),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",int),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",int),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",int),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",int),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",int),arg("v2",flt))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",bte),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",bte),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",sht),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",sht),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",int),arg("v1",lng),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",int),arg("v1",lng),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",flt),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "%", CMDvarMODsignal, false, "Return V1 % V2, signal error on divide by zero", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "mod_noerror", CMDvarMOD, false, "Return V1 % V2, divide by zero results in NIL value", args(1,3, arg("",dbl),arg("v1",dbl),arg("v2",dbl))),
 pattern("mmath", "fmod", CMDvarMODsignal, false, "", args(1,3, arg("",flt),arg("y",flt),arg("x",flt))),
 pattern("mmath", "fmod", CMDvarMODsignal, false, "The fmod(x,y) function computes the remainder of dividing x by y.\nThe return value is x - n * y, where n is the quotient of x / y,\nrounded towards zero to an integer.", args(1,3, arg("",dbl),arg("y",dbl),arg("x",dbl))),
 pattern("calc", "and", CMDvarAND, false, "Return V1 AND V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", "and", CMDvarAND, false, "Return V1 AND V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "and", CMDvarAND, false, "Return V1 AND V2", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "and", CMDvarAND, false, "Return V1 AND V2", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "and", CMDvarAND, false, "Return V1 AND V2", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "or", CMDvarOR, false, "Return V1 OR V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", "or", CMDvarOR, false, "Return V1 OR V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "or", CMDvarOR, false, "Return V1 OR V2", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "or", CMDvarOR, false, "Return V1 OR V2", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "or", CMDvarOR, false, "Return V1 OR V2", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "xor", CMDvarXOR, false, "Return V1 XOR V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", "xor", CMDvarXOR, false, "Return V1 XOR V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "xor", CMDvarXOR, false, "Return V1 XOR V2", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "xor", CMDvarXOR, false, "Return V1 XOR V2", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "xor", CMDvarXOR, false, "Return V1 XOR V2", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",int))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",int))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",lng))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",lng))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "<<", CMDvarLSHsignal, false, "Return V1 << V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "lsh_noerror", CMDvarLSH, false, "Return V1 << V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",bte))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",sht))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",int))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",int))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",sht),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",sht),arg("v1",sht),arg("v2",lng))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",bte))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",sht))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",int))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",int),arg("v1",int),arg("v2",lng))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",int),arg("v1",int),arg("v2",lng))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",bte))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",sht))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",int))),
 pattern("calc", ">>", CMDvarRSHsignal, false, "Return V1 >> V2, raise error on out of range second operand", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "rsh_noerror", CMDvarRSH, false, "Return V1 >> V2, out of range second operand results in NIL value", args(1,3, arg("",lng),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",str),arg("v2",str))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",blob),arg("v2",blob))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",oid),arg("v2",oid))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",int))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",int))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",bte))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",sht))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",int))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",lng))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",flt))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",int))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",int))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "<", CMDvarLT, false, "Return V1 < V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",str),arg("v2",str))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",blob),arg("v2",blob))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",oid),arg("v2",oid))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",int))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",int))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",bte))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",sht))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",int))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",lng))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",flt))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",int))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",int))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "<=", CMDvarLE, false, "Return V1 <= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",str),arg("v2",str))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",blob),arg("v2",blob))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",oid),arg("v2",oid))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",bte))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",sht))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",int))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",lng))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",flt))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",bte))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",sht))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",int))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",lng))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",flt))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",bte))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",sht))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",int))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",lng))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",flt))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",dbl))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",bte))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",sht))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",int))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",lng))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",flt))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",bte))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",sht))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",int))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",lng))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",flt))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",int))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", ">", CMDvarGT, false, "Return V1 > V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",str),arg("v2",str))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",blob),arg("v2",blob))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",oid),arg("v2",oid))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",bte))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",sht))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",int))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",lng))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",flt))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",bte))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",sht))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",int))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",lng))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",flt))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",bte))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",sht))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",int))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",lng))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",flt))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",dbl))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",bte))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",sht))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",int))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",lng))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",flt))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",bte))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",sht))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",int))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",lng))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",flt))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",int))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", ">=", CMDvarGE, false, "Return V1 >= V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",bit),arg("v2",bit),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",str),arg("v2",str))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",str),arg("v2",str),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",blob),arg("v2",blob))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",blob),arg("v2",blob),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",oid),arg("v2",oid))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",oid),arg("v2",oid),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",int))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",int))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",bte))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",sht))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",int))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",lng))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",flt))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",int))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",int))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "==", CMDvarEQ, false, "Return V1 == V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",bit),arg("v2",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",bit),arg("v2",bit),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",str),arg("v2",str))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",str),arg("v2",str),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",blob),arg("v2",blob))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",blob),arg("v2",blob),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",oid),arg("v2",oid))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",oid),arg("v2",oid),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",int))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",bte),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",int))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",sht),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",bte))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",sht))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",int))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",lng))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",flt))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",int),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",int))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",lng),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",int))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",flt),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",bte),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",sht),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",int),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",lng),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",flt),arg("nil_matches",bit))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,3, arg("",bit),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "!=", CMDvarNE, false, "Return V1 != V2", args(1,4, arg("",bit),arg("v1",dbl),arg("v2",dbl),arg("nil_matches",bit))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",bit),arg("v2",bit))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",str),arg("v2",str))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",oid),arg("v2",oid))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",bte))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",sht))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",int))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",lng))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",flt))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",bte),arg("v2",dbl))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",sht),arg("v2",bte))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",sht),arg("v2",sht))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",sht),arg("v2",int))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",sht),arg("v2",lng))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",sht),arg("v2",flt))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",sht),arg("v2",dbl))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",int),arg("v2",bte))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",int),arg("v2",sht))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",int),arg("v2",int))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",int),arg("v2",lng))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",int),arg("v2",flt))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",int),arg("v2",dbl))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",lng),arg("v2",bte))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",lng),arg("v2",sht))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",lng),arg("v2",int))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",lng),arg("v2",lng))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",lng),arg("v2",flt))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",lng),arg("v2",dbl))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",flt),arg("v2",bte))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",flt),arg("v2",sht))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",flt),arg("v2",int))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",flt),arg("v2",lng))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",flt),arg("v2",flt))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",flt),arg("v2",dbl))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",bte))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",sht))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",int))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",lng))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",flt))),
 pattern("calc", "cmp", CMDvarCMP, false, "Return -1/0/1 if V1 </==/> V2", args(1,3, arg("",bte),arg("v1",dbl),arg("v2",dbl))),
 pattern("calc", "between", CMDvarBETWEEN, false, "B between LO and HI inclusive", args(1,9, arg("",bit),argany("b",1),argany("lo",1),argany("hi",1),arg("sym",bit),arg("linc",bit),arg("hinc",bit),arg("nils_false",bit),arg("anti",bit))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",void))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",bit))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",bte))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",sht))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",int))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",lng))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",flt))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",dbl))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",oid))),
 pattern("calc", "void", CMDvarCONVERT, false, "Cast VALUE to void", args(1,2, arg("",void),arg("v",str))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",void))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",bit))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",bte))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",sht))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",int))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",lng))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",flt))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",dbl))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",oid))),
 pattern("calc", "bit", CMDvarCONVERT, false, "Cast VALUE to bit", args(1,2, arg("",bit),arg("v",str))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",void))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",bit))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",bte))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",sht))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",int))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",lng))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",flt))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",dbl))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",oid))),
 pattern("calc", "bte", CMDvarCONVERT, false, "Cast VALUE to bte", args(1,2, arg("",bte),arg("v",str))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",void))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",bit))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",bte))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",sht))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",int))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",lng))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",flt))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",dbl))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",oid))),
 pattern("calc", "sht", CMDvarCONVERT, false, "Cast VALUE to sht", args(1,2, arg("",sht),arg("v",str))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",void))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",bit))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",bte))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",sht))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",int))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",lng))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",flt))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",dbl))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",oid))),
 pattern("calc", "int", CMDvarCONVERT, false, "Cast VALUE to int", args(1,2, arg("",int),arg("v",str))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",void))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",bit))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",bte))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",sht))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",int))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",lng))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",flt))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",dbl))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",oid))),
 pattern("calc", "lng", CMDvarCONVERT, false, "Cast VALUE to lng", args(1,2, arg("",lng),arg("v",str))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",void))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",bit))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",bte))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",sht))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",int))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",lng))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",flt))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",dbl))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",oid))),
 pattern("calc", "flt", CMDvarCONVERT, false, "Cast VALUE to flt", args(1,2, arg("",flt),arg("v",str))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",void))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",bit))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",bte))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",sht))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",int))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",lng))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",flt))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",dbl))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",oid))),
 pattern("calc", "dbl", CMDvarCONVERT, false, "Cast VALUE to dbl", args(1,2, arg("",dbl),arg("v",str))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",void))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",bit))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",bte))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",sht))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",int))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",lng))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",flt))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",dbl))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",oid))),
 pattern("calc", "oid", CMDvarCONVERT, false, "Cast VALUE to oid", args(1,2, arg("",oid),arg("v",str))),
 pattern("calc", "str", CMDvarCONVERT, false, "Cast VALUE to str", args(1,2, arg("",str),argany("v",0))),
 pattern("calc", "min", CALCmin, false, "Return min of V1 and V2", args(1,3, argany("",1),argany("v1",1),argany("v2",1))),
 pattern("calc", "min_no_nil", CALCmin_no_nil, false, "Return min of V1 and V2, ignoring nil values", args(1,3, argany("",1),argany("v1",1),argany("v2",1))),
 pattern("calc", "max", CALCmax, false, "Return max of V1 and V2", args(1,3, argany("",1),argany("v1",1),argany("v2",1))),
 pattern("calc", "max_no_nil", CALCmax_no_nil, false, "Return max of V1 and V2, ignoring nil values", args(1,3, argany("",1),argany("v1",1),argany("v2",1))),
 command("calc", "ptr", CMDvarCONVERTptr, false, "Cast VALUE to ptr", args(1,2, arg("",ptr),arg("v",ptr))),
 pattern("calc", "ifthenelse", CALCswitchbit, false, "If VALUE is true return MIDDLE else RIGHT", args(1,4, argany("",1),arg("b",bit),argany("t",1),argany("f",1))),
 command("calc", "length", CMDstrlength, false, "Length of STRING", args(1,2, arg("",int),arg("s",str))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",bte),batarg("b",msk))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",bte),batarg("b",msk),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",bte),batarg("b",msk),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",bte),batarg("b",msk),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",sht),batarg("b",msk))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",sht),batarg("b",msk),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",sht),batarg("b",msk),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",sht),batarg("b",msk),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",int),batarg("b",msk))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",int),batarg("b",msk),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",int),batarg("b",msk),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",int),batarg("b",msk),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",lng),batarg("b",msk))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",lng),batarg("b",msk),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",lng),batarg("b",msk),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",lng),batarg("b",msk),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",dbl),batarg("b",msk))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",dbl),batarg("b",msk),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",dbl),batarg("b",msk),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",dbl),batarg("b",msk),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",bte),batarg("b",bte))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",bte),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",bte),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",bte),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",sht),batarg("b",bte))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",sht),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",sht),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",sht),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",int),batarg("b",bte))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",int),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",int),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",int),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",lng),batarg("b",bte))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",lng),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",lng),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",lng),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",dbl),batarg("b",bte))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",dbl),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",dbl),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",dbl),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",sht),batarg("b",sht))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",sht),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",sht),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",sht),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",int),batarg("b",sht))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",int),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",int),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",int),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",lng),batarg("b",sht))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",lng),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",lng),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",lng),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",dbl),batarg("b",sht))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",dbl),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",dbl),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",dbl),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",int),batarg("b",int))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",int),batarg("b",int),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",int),batarg("b",int),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",int),batarg("b",int),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",lng),batarg("b",int))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",lng),batarg("b",int),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",lng),batarg("b",int),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",lng),batarg("b",int),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",dbl),batarg("b",int))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",dbl),batarg("b",int),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",dbl),batarg("b",int),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",dbl),batarg("b",int),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",lng),batarg("b",lng))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",lng),batarg("b",lng),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",lng),batarg("b",lng),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",lng),batarg("b",lng),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",dbl),batarg("b",lng))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",dbl),batarg("b",lng),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",dbl),batarg("b",lng),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",dbl),batarg("b",lng),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",flt),batarg("b",flt))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",flt),batarg("b",flt),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",flt),batarg("b",flt),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",flt),batarg("b",flt),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",dbl),batarg("b",flt))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",dbl),batarg("b",flt),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",dbl),batarg("b",flt),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",dbl),batarg("b",flt),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,2, arg("",dbl),batarg("b",dbl))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B.", args(1,3, arg("",dbl),batarg("b",dbl),arg("nil_if_empty",bit))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,3, arg("",dbl),batarg("b",dbl),batarg("s",oid))),
 pattern("aggr", "sum", CMDBATsum, false, "Calculate aggregate sum of B with candidate list.", args(1,4, arg("",dbl),batarg("b",dbl),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",bte),batarg("b",bte))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",bte),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",bte),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",bte),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",sht),batarg("b",bte))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",sht),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",sht),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",sht),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",int),batarg("b",bte))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",int),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",int),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",int),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",lng),batarg("b",bte))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",lng),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",lng),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",lng),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",dbl),batarg("b",bte))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",dbl),batarg("b",bte),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",dbl),batarg("b",bte),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",dbl),batarg("b",bte),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",sht),batarg("b",sht))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",sht),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",sht),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",sht),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",int),batarg("b",sht))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",int),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",int),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",int),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",lng),batarg("b",sht))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",lng),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",lng),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",lng),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",dbl),batarg("b",sht))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",dbl),batarg("b",sht),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",dbl),batarg("b",sht),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",dbl),batarg("b",sht),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",int),batarg("b",int))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",int),batarg("b",int),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",int),batarg("b",int),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",int),batarg("b",int),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",lng),batarg("b",int))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",lng),batarg("b",int),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",lng),batarg("b",int),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",lng),batarg("b",int),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",dbl),batarg("b",int))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",dbl),batarg("b",int),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",dbl),batarg("b",int),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",dbl),batarg("b",int),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",lng),batarg("b",lng))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",lng),batarg("b",lng),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",lng),batarg("b",lng),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",lng),batarg("b",lng),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",dbl),batarg("b",lng))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",dbl),batarg("b",lng),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",dbl),batarg("b",lng),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",dbl),batarg("b",lng),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",flt),batarg("b",flt))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",flt),batarg("b",flt),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",flt),batarg("b",flt),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",flt),batarg("b",flt),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",dbl),batarg("b",flt))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",dbl),batarg("b",flt),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",dbl),batarg("b",flt),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",dbl),batarg("b",flt),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,2, arg("",dbl),batarg("b",dbl))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B.", args(1,3, arg("",dbl),batarg("b",dbl),arg("nil_if_empty",bit))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,3, arg("",dbl),batarg("b",dbl),batarg("s",oid))),
 pattern("aggr", "prod", CMDBATprod, false, "Calculate aggregate product of B with candidate list.", args(1,4, arg("",dbl),batarg("b",dbl),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "str_group_concat", CMDBATstr_group_concat, false, "Calculate aggregate string concatenate of B.", args(1,2, arg("",str),batarg("b",str))),
 pattern("aggr", "str_group_concat", CMDBATstr_group_concat, false, "Calculate aggregate string concatenate of B.", args(1,3, arg("",str),batarg("b",str),arg("nil_if_empty",bit))),
 pattern("aggr", "str_group_concat", CMDBATstr_group_concat, false, "Calculate aggregate string concatenate of B with candidate list.", args(1,3, arg("",str),batarg("b",str),batarg("s",oid))),
 pattern("aggr", "str_group_concat", CMDBATstr_group_concat, false, "Calculate aggregate string concatenate of B with candidate list.", args(1,4, arg("",str),batarg("b",str),batarg("s",oid),arg("nil_if_empty",bit))),
 pattern("aggr", "str_group_concat", CMDBATstr_group_concat, false, "Calculate aggregate string concatenate of B with separator SEP.", args(1,3, arg("",str),batarg("b",str),batarg("sep",str))),
 pattern("aggr", "str_group_concat", CMDBATstr_group_concat, false, "Calculate aggregate string concatenate of B with separator SEP.", args(1,4, arg("",str),batarg("b",str),batarg("sep",str),arg("nil_if_empty",bit))),
 pattern("aggr", "str_group_concat", CMDBATstr_group_concat, false, "Calculate aggregate string concatenate of B with candidate list and separator SEP.", args(1,4, arg("",str),batarg("b",str),batarg("sep",str),batarg("s",oid))),
 pattern("aggr", "str_group_concat", CMDBATstr_group_concat, false, "Calculate aggregate string concatenate of B with candidate list and separator SEP.", args(1,5, arg("",str),batarg("b",str),batarg("sep",str),batarg("s",oid),arg("nil_if_empty",bit))),
 //from sql
 pattern("aggr", "anyequal", CMDvarEQ, false, "", args(1,3, arg("",bit),argany("l",1),argany("r",1))),
 pattern("aggr", "not_anyequal", CMDvarNE, false, "", args(1,3, arg("",bit),argany("l",1),argany("r",1))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_calc_mal)
{ mal_module("calc", NULL, calc_init_funcs); }
