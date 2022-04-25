/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef BAT_UTILS_H
#define BAT_UTILS_H

#include "sql_storage.h"
#include "gdk_logger.h"

/* when returning a log_bid, errors are reported using BID_NIL */
#define BID_NIL 0

#define bat_set_access(b,access) b->batRestricted = access

extern BAT *temp_descriptor(log_bid b);
extern BAT *quick_descriptor(log_bid b);
extern void temp_destroy(log_bid b);
extern log_bid temp_dup(log_bid b);
extern log_bid temp_create(BAT *b);
extern log_bid temp_copy(log_bid b, bool renew, bool temp);

extern void bat_destroy(BAT *b);
extern BAT *bat_new(int tt, BUN size, role_t role);
extern void bat_clear(BAT *b);

extern BAT *ebats[MAXATOMS];

#define isEbat(b) 	(ebats[b->ttype] && ebats[b->ttype] == b)

extern log_bid e_bat(int type);
extern BAT *e_BAT(int type);
extern int bat_utils_init(void);

#endif /* BAT_UTILS_H */
