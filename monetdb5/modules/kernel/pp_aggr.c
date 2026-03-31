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
#include "aggr.h"

static str
LAGGRsum3_lng(Client ctx, bat *retval, const ptr *h, const bat *bid, const bat *gid, const bat *eid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	MT_lock_set(&p->p->l);
	res = AGGRsum3_lng(ctx, retval, bid, gid, eid);
	MT_lock_unset(&p->p->l);
	return res;
}

#ifdef HAVE_HGE
static str
LAGGRsum3_hge(Client ctx, bat *retval, const ptr *h, const bat *bid, const bat *gid, const bat *eid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	MT_lock_set(&p->p->l);
	res = AGGRsum3_hge(ctx, retval, bid, gid, eid);
	MT_lock_unset(&p->p->l);
	return res;
}
#endif

#include "mel.h"
mel_func pp_aggr_init_funcs[] = {
 command("lockedaggr", "sum", LAGGRsum3_lng, false, "Grouped tail sum on lng", args(1,5, batarg("",lng),arg("pipeline", ptr),batarg("b",lng),batarg("g",oid),batargany("e",1))),
#ifdef HAVE_HGE
 command("lockedaggr", "sum", LAGGRsum3_hge, false, "Grouped tail sum on hge", args(1,5, batarg("",hge),arg("pipeline", ptr),batarg("b",hge),batarg("g",oid),batargany("e",1))),
#endif
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pp_aggr_mal)
{ mal_module("pp_aggr", NULL, pp_aggr_init_funcs); }
