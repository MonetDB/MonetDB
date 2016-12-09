/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _BAT_H_
#define _BAT_H_

#include <mal.h>
#include <gdk.h>

mal_export str BKCnewBAT(bat *res, const int *tt, const BUN *cap, int role);
mal_export str BKCattach(bat *ret, const int *tt, const char * const *heapfile);
mal_export str BKCdensebat(bat *ret, const lng *size);
mal_export str BKCmirror(bat *ret, const bat *bid);
mal_export str BKCdelete(bat *r, const bat *bid, const oid *h);
mal_export str BKCdelete_multi(bat *r, const bat *bid, const bat *sid);
mal_export str BKCdelete_all(bat *r, const bat *bid);
mal_export str BKCappend_wrap(bat *r, const bat *bid, const bat *uid);
mal_export str BKCappend_force_wrap(bat *r, const bat *bid, const bat *uid, const bit *force);
mal_export str BKCappend_cand_wrap(bat *r, const bat *bid, const bat *uid, const bat *sid);
mal_export str BKCappend_cand_force_wrap(bat *r, const bat *bid, const bat *uid, const bat *sid, const bit *force);
mal_export str BKCappend_val_wrap(bat *r, const bat *bid, const void *u);
mal_export str BKCappend_val_force_wrap(bat *r, const bat *bid, const void *u, const bit *force);
mal_export str BKCbun_inplace(bat *r, const bat *bid, const oid *id, const void *t);
mal_export str BKCbat_inplace(bat *r, const bat *bid, const bat *rid, const bat *uid);
mal_export str BKCbun_inplace_force(bat *r, const bat *bid, const oid *id, const void *t, const bit *force);
mal_export str BKCbat_inplace_force(bat *r, const bat *bid, const bat *rid, const bat *uid, const bit *force);
mal_export str BKCgetCapacity(lng *res, const bat *bid);
mal_export str BKCgetColumnType(str *res, const bat *bid);
mal_export str BKCgetRole(str *res, const bat *bid);
mal_export str BKCsetkey(bat *res, const bat *bid, const bit *param);
mal_export str BKCisSorted(bit *res, const bat *bid);
mal_export str BKCisSortedReverse(bit *res, const bat *bid);
mal_export str BKCgetKey(bit *ret, const bat *bid);
mal_export str BKCpersists(void *r, const bat *bid, const bit *flg);
mal_export str BKCsetPersistent(void *r, const bat *bid);
mal_export str BKCisPersistent(bit *res, const bat *bid);
mal_export str BKCsetTransient(void *r, const bat *bid);
mal_export str BKCisTransient(bit *res, const bat *bid);
mal_export str BKCsetAccess(bat *res, const bat *bid, const char * const *param);
mal_export str BKCgetAccess(str *res, const bat *bid);
mal_export str BKCinfo(bat *ret1, bat *ret2, const bat *bid);
mal_export str BKCgetSize(lng *tot, const bat *bid);
mal_export str BKCisSynced(bit *ret, const bat *bid1, const bat *bid2);
mal_export str BKCsetColumn(void *r, const bat *bid, const char * const *tname);
mal_export str BKCsetName(void *r, const bat *bid, const char * const *s);
mal_export str BKCgetBBPname(str *ret, const bat *bid);
mal_export str BKCsave(bit *res, const char * const *input);
mal_export str BKCsave2(void *r, const bat *bid);
mal_export str BKCsetHash(bit *ret, const bat *bid);
mal_export str BKCsetImprints(bit *ret, const bat *bid);
mal_export str BKCgetSequenceBase(oid *r, const bat *bid);
mal_export str BKCshrinkBAT(bat *ret, const bat *bid, const bat *did);
mal_export str BKCreuseBAT(bat *ret, const bat *bid, const bat *did);
mal_export str BKCshrinkBATmap(bat *ret, const bat *bid, const bat *did);
mal_export str BKCreuseBATmap(bat *ret, const bat *bid, const bat *did);
mal_export str BKCmergecand(bat *ret, const bat *aid, const bat *bid);
mal_export str BKCintersectcand(bat *ret, const bat *aid, const bat *bid);
#endif /*_BAT_H_*/
