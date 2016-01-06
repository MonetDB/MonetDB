/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
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

bat5_export char *BKCsetRole(void *r, const bat *bid, const char * const *hname, const char * const *tname);
bat5_export str BKCnewBAT(bat *res, const int *tt, const BUN *cap, int role);
bat5_export str BKCattach(bat *ret, const int *tt, const char * const *heapfile);
bat5_export str BKCdensebat(bat *ret, const wrd *size);
bat5_export str BKCmirror(bat *ret, const bat *bid);
bat5_export str BKCdelete(bat *r, const bat *bid, const oid *h);
bat5_export str BKCdelete_multi(bat *r, const bat *bid, const bat *sid);
bat5_export str BKCdelete_all(bat *r, const bat *bid);
bat5_export str BKCappend_wrap(bat *r, const bat *bid, const bat *uid);
bat5_export str BKCappend_val_wrap(bat *r, const bat *bid, const void *u);
bat5_export str BKCappend_reverse_val_wrap(bat *r, const bat *bid, const void *u);
bat5_export str BKCappend_force_wrap(bat *r, const bat *bid, const bat *uid, const bit *force);
bat5_export str BKCappend_val_force_wrap(bat *r, const bat *bid, const void *u, const bit *force);
bat5_export str BKCbun_inplace(bat *r, const bat *bid, const oid *id, const void *t);
bat5_export str BKCbat_inplace(bat *r, const bat *bid, const bat *rid, const bat *uid);
bat5_export str BKCbun_inplace_force(bat *r, const bat *bid, const oid *id, const void *t, const bit *force);
bat5_export str BKCbat_inplace_force(bat *r, const bat *bid, const bat *rid, const bat *uid, const bit *force);
bat5_export str BKCgetCapacity(lng *res, const bat *bid);
bat5_export str BKCgetColumnType(str *res, const bat *bid);
bat5_export str BKCgetRole(str *res, const bat *bid);
bat5_export str BKCsetkey(bat *res, const bat *bid, const bit *param);
bat5_export str BKCisSorted(bit *res, const bat *bid);
bat5_export str BKCisSortedReverse(bit *res, const bat *bid);
bat5_export str BKCgetKey(bit *ret, const bat *bid);
bat5_export str BKCpersists(void *r, const bat *bid, const bit *flg);
bat5_export str BKCsetPersistent(void *r, const bat *bid);
bat5_export str BKCisPersistent(bit *res, const bat *bid);
bat5_export str BKCsetTransient(void *r, const bat *bid);
bat5_export str BKCisTransient(bit *res, const bat *bid);
bat5_export str BKCsetAccess(bat *res, const bat *bid, const char * const *param);
bat5_export str BKCgetAccess(str *res, const bat *bid);
bat5_export str BKCinfo(bat *ret1, bat *ret2, const bat *bid);
bat5_export str BKCgetSize(lng *tot, const bat *bid);
bat5_export str BKCisSynced(bit *ret, const bat *bid1, const bat *bid2);
bat5_export str BKCsetColumn(void *r, const bat *bid, const char * const *tname);
bat5_export str BKCsetColumns(void *r, const bat *bid, const char * const *hname, const char * const *tname);
bat5_export str BKCsetName(void *r, const bat *bid, const char * const *s);
bat5_export str BKCgetBBPname(str *ret, const bat *bid);
bat5_export str BKCsave(bit *res, const char * const *input);
bat5_export str BKCsave2(void *r, const bat *bid);
bat5_export str BKCsetHash(bit *ret, const bat *bid);
bat5_export str BKCsetImprints(bit *ret, const bat *bid);
bat5_export str BKCgetSequenceBase(oid *r, const bat *bid);
bat5_export str BKCshrinkBAT(bat *ret, const bat *bid, const bat *did);
bat5_export str BKCreuseBAT(bat *ret, const bat *bid, const bat *did);
bat5_export str BKCshrinkBATmap(bat *ret, const bat *bid, const bat *did);
bat5_export str BKCreuseBATmap(bat *ret, const bat *bid, const bat *did);
bat5_export str BKCmergecand(bat *ret, const bat *aid, const bat *bid);
bat5_export str BKCintersectcand(bat *ret, const bat *aid, const bat *bid);
#endif /*_BAT_H_*/
