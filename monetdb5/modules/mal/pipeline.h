/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _PIPELINE_H_
#define _PIPELINE_H_
//#include "monetdb_config.h"
//#include "gdk.h"
//#include "gdk_atoms.h"
//#include "gdk_time.h"
//#include "mal_exception.h"
//#include "mal_interpreter.h"
//#include "mal_pipelines.h"
//#include "pp_mem.h"
//#include "pp_hash.h"
//#include "algebra.h"

#define pipeline_lock(p) MT_lock_set(&p->p->l)
#define pipeline_unlock(p) MT_lock_unset(&p->p->l)

#define pipeline_lock1(r) MT_lock_set(&r->batIdxLock)
#define pipeline_unlock1(r) MT_lock_unset(&r->batIdxLock)

#define pipeline_lock2(r) MT_lock_set(&r->theaplock)
#define pipeline_unlock2(r) MT_lock_unset(&r->theaplock)

extern int BATupgrade(BAT *r, BAT *b, bool locked);
extern void BATswap_heaps(BAT *u, BAT *b, Pipeline *p);

#endif /*_PIPELINE_H_*/
