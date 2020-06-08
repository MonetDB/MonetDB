/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*
 * @+ Monet client data
 * Portions of the client record can be directly obtained for
 * backward compatibility. The routine clientInfo provides more
 * detailed information.
 */
#ifndef _CALC_H
#define _CALC_H
#include "mal.h"
#include "mal_interpreter.h"

mal_export str CMDvarSUBsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarSUB(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarADDsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarADD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarADDstr(str *ret, str *s1, str *s2);
mal_export str CMDvarADDstrint(str *ret, str *s1, int *i);
mal_export str CMDvarMULsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarMUL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarDIVsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarDIV(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarMODsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarMOD(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarLSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarLSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarRSHsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarRSH(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarAND(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarXOR(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarLT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarLE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarGT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarGE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarEQ(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarNE(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarCMP(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarBETWEEN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDstrlength(int *ret, str *v);
mal_export str CMDvarCONVERT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarCONVERTptr(ptr *ret, ptr *v);
mal_export str CMDvarISZERO(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarISNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarISNOTNIL(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarNOT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarABS(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarSIGN(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarNEG(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarINCRsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDvarDECRsignal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CALCswitchbit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str CALCmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str CALCmin_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str CALCmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str CALCmax_no_nil(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str CMDBATsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDBATprod(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str CMDBATstr_group_concat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
#endif /* _CALC_H */
