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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef BAT_UTILS_H
#define BAT_UTILS_H

#include "sql_storage.h"
#include <gdk_logger.h>

#define bat_set_access(b,access) b->P->restricted = access
#define bat_clear(b) bat_set_access(b,BAT_WRITE);BATclear(b);bat_set_access(b,BAT_READ)

extern BAT *temp_descriptor(log_bid b);
extern BAT *quick_descriptor(log_bid b);
extern void temp_destroy(log_bid b);
extern void temp_dup(log_bid b);
extern log_bid temp_create(BAT *b);
extern log_bid temp_copy(log_bid b, int temp);

extern void bat_destroy(BAT *b);
extern BAT *bat_new(int ht, int tt, BUN size);

extern void update_table_bat(BAT *b, BAT *ub);
extern BUN append_inserted(BAT *b, BAT *i );
extern BUN copy_inserted(BAT *b, BAT *i );

extern void leaks(void);

extern BAT *ebats[MAXATOMS];
extern BAT *eubats[MAXATOMS];

#define isEbat(b) 	(ebats[b->ttype] && ebats[b->ttype] == b) 
#define isEUbat(b) 	(eubats[b->ttype] && eubats[b->ttype] == b) 

extern log_bid ebat2real(log_bid b, oid ibase);
extern log_bid e_bat(int type);
extern BAT *e_BAT(int type);
extern log_bid e_ubat(int type);
extern log_bid ebat_copy(log_bid b, oid ibase, int temp);
extern log_bid eubat_copy(log_bid b, int temp);
extern void bat_utils_init(void);

#endif /* BAT_UTILS_H */
