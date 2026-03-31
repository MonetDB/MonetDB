/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "mal_pipelines.h"
#include "group.h"

static str
LGRPsubgroup2(Client ctx, bat *ngid, bat *next, bat *nhis, const ptr *h, const bat *bid, const bat *gid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	MT_lock_set(&p->p->l);
	res = GRPsubgroup2(ctx, ngid, next, nhis, bid, gid);
	MT_lock_unset(&p->p->l);
	return res;
}

static str
LGRPgroup3(Client ctx, bat *ngid, bat *next, const ptr *h, const bat *bid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	MT_lock_set(&p->p->l);
	res = GRPgroup3(ctx, ngid, next, bid);
	MT_lock_unset(&p->p->l);
	return res;
}

static str
LGRPgroup1(Client ctx, bat *ngid, bat *next, bat *nhis, const ptr *h, const bat *bid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	MT_lock_set(&p->p->l);
	res = GRPgroup1(ctx, ngid, next, nhis, bid);
	MT_lock_unset(&p->p->l);
	return res;
}

#include "mel.h"
mel_func pp_group_init_funcs[] = {
 command("lockedgroup", "group", LGRPgroup1, false, "", args(3,5, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),arg("pipeline", ptr),batargany("b",1))),
 command("lockedgroup", "group", LGRPgroup3, false, "", args(2,4, batarg("groups",oid),batarg("extents",oid),arg("pipeline", ptr),batargany("b",1))),
 command("lockedgroup", "subgroupdone", LGRPsubgroup2, false, "", args(3,6, batarg("groups",oid),batarg("extents",oid),batarg("histo",lng),arg("pipeline", ptr),batargany("b",1),batarg("g",oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pp_group_mal)
{ mal_module("pp_group", NULL, pp_group_init_funcs); }
