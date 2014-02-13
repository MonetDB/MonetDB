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

#include "monetdb_config.h"
#include "gdk.h"
#include "math.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define calc_export extern __declspec(dllimport)
#else
#define calc_export extern __declspec(dllexport)
#endif
#else
#define calc_export extern
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

calc_export str CMDvarSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcsub(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) == GDK_FAIL)
		return mythrow(MAL, "calc.-", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcsub(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) == GDK_FAIL)
		return mythrow(MAL, "calc.sub_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcadd(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) == GDK_FAIL)
		return mythrow(MAL, "calc.+", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcadd(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) == GDK_FAIL)
		return mythrow(MAL, "calc.add_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarADDstr(str *ret, str *s1, str *s2);

str
CMDvarADDstr(str *ret, str *s1, str *s2)
{
	str s;
	size_t l1;

	if (strNil(*s1) || strNil(*s2)) {
		*ret= GDKstrdup(str_nil);
		return MAL_SUCCEED;
	}
	s = GDKzalloc((l1 = strlen(*s1)) + strlen(*s2) + 1);
	if (s == NULL)
		return mythrow(MAL, "calc.+", MAL_MALLOC_FAIL);
	strcpy(s, *s1);
	strcpy(s + l1, *s2);
	*ret = s;
	return MAL_SUCCEED;
}

calc_export str CMDvarADDstrint(str *ret, str *s1, int *i);

str
CMDvarADDstrint(str *ret, str *s1, int *i)
{
	str s;
	size_t len;

	if (strNil(*s1) || *i == int_nil) {
		*ret= GDKstrdup(str_nil);
		return MAL_SUCCEED;
	}
	len = strlen(*s1) + 16;		/* maxint = 2147483647 which fits easily */
	s = GDKmalloc(len);
	if (s == NULL)
		return mythrow(MAL, "calc.+", MAL_MALLOC_FAIL);
	snprintf(s, len, "%s%d", *s1, *i);
	*ret = s;
	return MAL_SUCCEED;
}

calc_export str CMDvarMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcmul(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) == GDK_FAIL)
		return mythrow(MAL, "calc.*", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcmul(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) == GDK_FAIL)
		return mythrow(MAL, "calc.mul_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcdiv(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) == GDK_FAIL)
		return mythrow(MAL, "calc./", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcdiv(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) == GDK_FAIL)
		return mythrow(MAL, "calc.div_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcmod(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) == GDK_FAIL)
		return mythrow(MAL, "calc.%", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcmod(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) == GDK_FAIL)
		return mythrow(MAL, "calc.modmod", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalclsh(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) == GDK_FAIL)
		return mythrow(MAL, "calc.<<", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalclsh(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) == GDK_FAIL)
		return mythrow(MAL, "calc.lsh_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcrsh(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 1) == GDK_FAIL)
		return mythrow(MAL, "calc.>>", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcrsh(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], 0) == GDK_FAIL)
		return mythrow(MAL, "calc.rsh_noerror", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcand(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.and", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcor(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.or", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcxor(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.xor", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalclt(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.<", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcle(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.<=", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcgt(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.>", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcge(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.>=", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalceq(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.==", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcne(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.!=", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalccmp(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)]) == GDK_FAIL)
		return mythrow(MAL, "calc.cmp", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDvarBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	if (VARcalcbetween(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], &stk->stk[getArg(pci, 2)], &stk->stk[getArg(pci, 3)]) == GDK_FAIL)
		return mythrow(MAL, "calc.between", OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDstrlength(int *ret, str *v);

str
CMDstrlength(int *ret, str *v)
{
	size_t l = strlen(*v);

	if (l > (size_t) GDK_int_max)
		return mythrow(MAL, "calc.length", OPERATION_FAILED);
	*ret = (int) l;
	return MAL_SUCCEED;
}

calc_export str CMDvarCONVERT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarCONVERT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARconvert(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], 1) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarCONVERTptr(ptr *ret, ptr *v);

str
CMDvarCONVERTptr(ptr *ret, ptr *v)
{
	*ret = *v;
	return MAL_SUCCEED;
}

calc_export str CMDvarISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalciszero(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcisnil(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcisnotnil(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcnot(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcabsolute(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcsign(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcnegate(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)]) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarINCRsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarINCRsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcincr(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], 1) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDvarDECRsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDvarDECRsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char buf[20];

	(void) cntxt;
	(void) mb;

	if (VARcalcdecr(&stk->stk[getArg(pci, 0)], &stk->stk[getArg(pci, 1)], 1) == GDK_FAIL) {
		snprintf(buf, sizeof(buf), "%s.%s", pci->modname, pci->fcnname);
		return mythrow(MAL, buf, OPERATION_FAILED);
	}
	return MAL_SUCCEED;
}

calc_export str CMDsetoid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDsetoid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	switch (ATOMstorage(getArgType(mb, pci, 1))) {
	case TYPE_int:
		OIDbase((oid) * (int *) getArgReference(stk, pci, 1));
		break;
	case TYPE_lng:
		OIDbase((oid) * (lng *) getArgReference(stk, pci, 1));
		break;
	default:
		return mythrow(MAL, "calc.setoid", ILLEGAL_ARGUMENT);
	}
	return MAL_SUCCEED;
}

calc_export str CALCbat2int(int *ret, bat *b);

str
CALCbat2int(int *ret, bat *bid)
{
	BAT *b;

	if (*bid == bat_nil) {
		*ret = bat_nil;
		return MAL_SUCCEED;
	}
	b = BATdescriptor(*bid);
	if (b == 0)
		return mythrow(MAL, "calc.getBAT", RUNTIME_OBJECT_MISSING);
	*ret = b->batCacheid;
	BBPkeepref(b->batCacheid);
	return MAL_SUCCEED;
}

calc_export str CALCswitchbit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

str
CALCswitchbit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	ptr p;
	ptr retval = getArgReference(stk, pci, 0);
	bit b = *(bit *) getArgReference(stk, pci, 1);
	int t1 = getArgType(mb, pci, 2);
	int t2 = getArgType(mb, pci, 3);

	(void) cntxt;
	if (t1 != t2)
		return mythrow(MAL, "ifthenelse", SEMANTIC_TYPE_MISMATCH);

	if (b == bit_nil) {
		*(ptr**)retval = p = ATOMnilptr(t1);
		return MAL_SUCCEED;
	}
	if (b) {
		p = getArgReference(stk, pci, 2);
	} else {
		p = getArgReference(stk, pci, 3);
	}
	if (ATOMextern(t1)) {
		*(ptr **) retval = ATOMdup(t1, *(ptr**)p);
	} else if (t1 == TYPE_void) {
		memcpy(retval, p, sizeof(oid));
	} else {
		memcpy(retval, p, ATOMsize(t1));
	}
	return MAL_SUCCEED;
}

calc_export str CALCmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

str
CALCmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int t = getArgType(mb, pci, 1);
	ptr p1 = getArgReference(stk, pci, 1);
	ptr p2 = getArgReference(stk, pci, 2);
	ptr nil;

	(void) cntxt;
	if (t != getArgType(mb, pci, 2))
		return mythrow(MAL, "calc.min", SEMANTIC_TYPE_MISMATCH);
	nil = ATOMnilptr(t);
	if (ATOMcmp(t, p1, nil) == 0 || ATOMcmp(t, p2, nil) == 0)
		p1 = nil;
	else if (ATOMcmp(t, p1, p2) > 0)
		p1 = p2;
	memcpy(getArgReference(stk, pci, 0), p1, ATOMsize(t));
	return MAL_SUCCEED;
}

calc_export str CALCmin_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

str
CALCmin_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int t = getArgType(mb, pci, 1);
	ptr p1 = getArgReference(stk, pci, 1);
	ptr p2 = getArgReference(stk, pci, 2);
	ptr nil;

	(void) cntxt;
	if (t != getArgType(mb, pci, 2))
		return mythrow(MAL, "calc.min", SEMANTIC_TYPE_MISMATCH);
	nil = ATOMnilptr(t);
	if (ATOMcmp(t, p1, nil) == 0 ||
		(ATOMcmp(t, p2, nil) != 0 && ATOMcmp(t, p1, p2) > 0))
		p1 = p2;
	memcpy(getArgReference(stk, pci, 0), p1, ATOMsize(t));
	return MAL_SUCCEED;
}

calc_export str CALCmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

str
CALCmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int t = getArgType(mb, pci, 1);
	ptr p1 = getArgReference(stk, pci, 1);
	ptr p2 = getArgReference(stk, pci, 2);
	ptr nil;

	(void) cntxt;
	if (t != getArgType(mb, pci, 2))
		return mythrow(MAL, "calc.max", SEMANTIC_TYPE_MISMATCH);
	nil = ATOMnilptr(t);
	if (ATOMcmp(t, p1, nil) == 0 || ATOMcmp(t, p2, nil) == 0)
		p1 = nil;
	else if (ATOMcmp(t, p1, p2) < 0)
		p1 = p2;
	memcpy(getArgReference(stk, pci, 0), p1, ATOMsize(t));
	return MAL_SUCCEED;
}

calc_export str CALCmax_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

str
CALCmax_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int t = getArgType(mb, pci, 1);
	ptr p1 = getArgReference(stk, pci, 1);
	ptr p2 = getArgReference(stk, pci, 2);
	ptr nil;

	(void) cntxt;
	if (t != getArgType(mb, pci, 2))
		return mythrow(MAL, "calc.max", SEMANTIC_TYPE_MISMATCH);
	nil = ATOMnilptr(t);
	if (ATOMcmp(t, p1, nil) == 0 ||
		(ATOMcmp(t, p2, nil) != 0 && ATOMcmp(t, p1, p2) < 0))
		p1 = p2;
	memcpy(getArgReference(stk, pci, 0), p1, ATOMsize(t));
	return MAL_SUCCEED;
}

static str
CMDBATsumprod(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
			  gdk_return (*sumprod)(void *, int, BAT *, BAT *, int, int, int),
			  const char *func)
{
	ValPtr ret = &stk->stk[getArg(pci, 0)];
	bat bid = * (bat *) getArgReference(stk, pci, 1);
	BAT *b;
	BAT *s = NULL;
	int nil_if_empty = 1;
	gdk_return r;

	if ((b = BATdescriptor(bid)) == NULL)
		throw(MAL, func, RUNTIME_OBJECT_MISSING);
	if (pci->argc >= 3) {
		if (getArgType(mb, pci, 2) == TYPE_bit) {
			assert(pci->argc == 3);
			nil_if_empty = * (bit *) getArgReference(stk, pci, 2);
		} else {
			bat sid = * (bat *) getArgReference(stk, pci, 2);
			if ((s = BATdescriptor(sid)) == NULL) {
				BBPreleaseref(b->batCacheid);
				throw(MAL, func, RUNTIME_OBJECT_MISSING);
			}
			if (pci->argc >= 4) {
				assert(pci->argc == 4);
				assert(getArgType(mb, pci, 3) == TYPE_bit);
				nil_if_empty = * (bit *) getArgReference(stk, pci, 3);
			}
		}
	}
	if (s == NULL && !BAThdense(b)) {
		/* XXX backward compatibility code: ignore non-dense head, but
		 * only if no candidate list */
		s = BATmirror(BATmark(BATmirror(b), 0));
		BBPreleaseref(b->batCacheid);
		b = s;
		s = NULL;
	}
	r = (*sumprod)(VALget(ret), ret->vtype, b, s, 1, 1, nil_if_empty);
	BBPreleaseref(b->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (r == GDK_FAIL)
		return mythrow(MAL, func, OPERATION_FAILED);
	return MAL_SUCCEED;
}

calc_export str CMDBATsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDBATsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDBATsumprod(mb, stk, pci, BATsum, "aggr.sum");
}

calc_export str CMDBATprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

str
CMDBATprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;

	return CMDBATsumprod(mb, stk, pci, BATprod, "aggr.prod");
}
