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

#ifndef _BAT_H_
#define _BAT_H_

#include <mal.h>
#include <gdk.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define bat5_export extern __declspec(dllimport)
#else
#define bat5_export extern __declspec(dllexport)
#endif
#else
#define bat5_export extern
#endif

bat5_export char *BKCsetRole(int *r, int *bid, char **hname, char **tname);
bat5_export char *BKCdestroyImmediate(signed char *r, int *bid);
bat5_export char *BKCgetAlpha(int *r, int *bid);
bat5_export char *BKCgetDelta(int *r, int *bid);
bat5_export char *BKCinsert_bun(int *r, int *bid, ptr h, ptr t);
bat5_export char *BKCinsert_bun_force(int *r, int *bid, ptr h, ptr t, bit *force);
bat5_export char *BKCdelete(int *r, int *bid, const void *h);
bat5_export char *BKCdelete_bat(int *r, int *bid, int *bnid);
bat5_export char *BKCdelete_bun(int *r, int *bid, ptr h, ptr t);
bat5_export char *BKCdestroy(signed char *r, int *bid);
bat5_export char *BKCbat_append_void_bat_wrap(int *r, int *bid, int *uid);
bat5_export char * BKCbat_append_val_wrap(int *r, int *bid, ptr u);
bat5_export str BKCnewBAT(int *res, int *ht, int *tt, BUN *cap);
bat5_export str BKCattach(int *ret, int *tt, str *heapfile);
bat5_export str BKCdensebat(int *ret, wrd *size);
bat5_export str BKCreverse(int *ret, int *bid);
bat5_export str BKCmirror(int *ret, int *bid);
bat5_export str BKCrevert(int *ret, int *bid);
bat5_export str BKCorder(int *ret, int *bid);
bat5_export str BKCorder_rev(int *ret, int *bid);
bat5_export str BKCinsert_bat(int *r, int *bid, int *sid);
bat5_export str BKCinsert_bat_force(int *r, int *bid, int *sid, bit *force);
bat5_export str BKCreplace_bun(int *r, int *bid, ptr h, ptr t);
bat5_export str BKCreplace_bat(int *r, int *bid, int *sid);
bat5_export str BKCreplace_bun_force(int *r, int *bid, ptr h, ptr t, bit *force);
bat5_export str BKCreplace_bat_force(int *r, int *bid, int *sid, bit *force);
bat5_export str BKCdelete_all(int *r, int *bid);
bat5_export str BKCdelete_bat_bun(int *r, int *bid, int *sid);
bat5_export str BKCdelete_bat(int *r, int *bid, int *sid);
bat5_export str BKCdestroy_bat(bit *r, str *input);
bat5_export str BKCappend_wrap(int *r, int *bid, int *uid);
bat5_export str BKCappend_val_wrap(int *r, int *bid, ptr u);
bat5_export str BKCappend_reverse_val_wrap(int *r, int *bid, ptr u);
bat5_export str BKCappend_force_wrap(int *r, int *bid, int *uid, bit *force);
bat5_export str BKCappend_val_force_wrap(int *r, int *bid, ptr u, bit *force);
bat5_export str BKCbun_inplace(int *r, int *bid, oid *id, ptr t);
bat5_export str BKCbat_inplace(int *r, int *bid, int *rid);
bat5_export str BKCbun_inplace_force(int *r, int *bid, oid *id, ptr t, bit *force);
bat5_export str BKCbat_inplace_force(int *r, int *bid, int *rid, bit *force);
bat5_export str BKCgetCapacity(lng *res, int *bid);
bat5_export str BKCgetHeadType(str *res, int *bid);
bat5_export str BKCgetColumnType(str *res, int *bid);
bat5_export str BKCgetRole(str *res, int *bid);
bat5_export str BKCsetkey(int *res, int *bid, bit *param);
bat5_export str BKCisSorted(bit *res, int *bid);
bat5_export str BKCisSortedReverse(bit *res, int *bid);
bat5_export str BKCgetKey(bit *ret, int *bid);
bat5_export str BKCpersists(int *r, int *bid, bit *flg);
bat5_export str BKCsetPersistent(int *r, int *bid);
bat5_export str BKCisPersistent(bit *res, int *bid);
bat5_export str BKCsetTransient(int *r, int *bid);
bat5_export str BKCisTransient(bit *res, int *bid);
bat5_export str BKCsetAccess(int *res, int *bid, str *param);
bat5_export str BKCgetAccess(str *res, int *bid);
bat5_export str BKCinfo(int *ret1, int *ret2, int *bid);
bat5_export str BKCbatsize(lng *tot, int *bid);
bat5_export str BKCbatvmsize(lng *tot, int *bid);
bat5_export str BKCbatdisksize(lng *tot, int *bid);
bat5_export str BKCgetStorageSize(lng *tot, int *bid);
bat5_export str BKCgetSpaceUsed(lng *tot, int *bid);
bat5_export str BKCgetStorageSize_str(lng *tot, str batname);
bat5_export str BKCisSynced(bit *ret, int *bid1, int *bid2);
bat5_export str BKCsetColumn(int *r, int *bid, str *tname);
bat5_export str BKCsetColumns(int *r, int *bid, str *hname, str *tname);
bat5_export str BKCsetName(int *r, int *bid, str *s);
bat5_export str BKCgetBBPname(str *ret, int *bid);
bat5_export str BKCunload(bit *res, str *input);
bat5_export str BKCisCached(bit *res, int *bid);
bat5_export str BKCload(int *res, str *input);
bat5_export str BKChot(int *res, str *input);
bat5_export str BKCcold(int *res, str *input);
bat5_export str BKCcoldBAT(int *res, int *bid);
bat5_export str BKCheat(int *res, str *input);
bat5_export str BKChotBAT(int *res, int *bid);
bat5_export str BKCsave(bit *res, str *input);
bat5_export str BKCsave2(int *r, int *bid);
bat5_export str BKCmmap(bit *res, int *bid, int *hbns, int *tbns, int *hhp, int *thp);
bat5_export str BKCmmap2(bit *res, int *bid, int *bns);
bat5_export str BKCmadvise(bit *res, int *bid, int *hbns, int *tbns, int *hhp, int *thp);
bat5_export str BKCmadvise2(bit *res, int *bid, int *mode);
bat5_export str BKCaccbuild(int *ret, int *bid, str *acc, ptr *param);
bat5_export str BKCaccbuild_std(int *ret, int *bid, int *acc);
bat5_export str BKCsetHash(bit *ret, int *bid, bit *prop);
bat5_export str BKCsetSequenceBase(int *r, int *bid, oid *o);
bat5_export str BKCsetSequenceBaseNil(int *r, int *bid, oid *o);
bat5_export str BKCgetSequenceBase(oid *r, int *bid);
bat5_export str BKCshrinkBAT(int *ret, int *bid, int *did);
bat5_export str BKCreuseBAT(int *ret, int *bid, int *did);
bat5_export str BKCshrinkBATmap(int *ret, int *bid, int *did);
bat5_export str BKCreuseBATmap(int *ret, int *bid, int *did);

bat5_export str BKCsetWriteMode(int *res, int *bid) ;
bat5_export str BKChasWriteMode(bit *res, int *bid);
bat5_export str BKCsetReadMode(int *res, int *bid) ;
bat5_export str BKChasReadMode(bit *res, int *bid);
bat5_export str BKCsetAppendMode(int *res, int *bid) ;
bat5_export str BKChasAppendMode(bit *res, int *bid);

bat5_export str BKCmergecand(bat *ret, bat *aid, bat *bid);
bat5_export str BKCintersectcand(bat *ret, bat *aid, bat *bid);
#endif /*_BAT_H_*/
