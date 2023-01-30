/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef STORE_SEQ_H
#define STORE_SEQ_H

#include "sql_catalog.h"

extern void* sequences_init(void);
extern void sequences_exit(void);

extern int seq_hash(void *seq);
extern void seq_hash_destroy(sql_hash *h);
extern int seq_get_value(sql_store store, sql_sequence *seq, lng *val);
extern int seq_next_value(sql_store store, sql_sequence *seq, lng *val);
extern int seq_restart(sql_store store, sql_sequence *seq, lng start);

extern void log_store_sequence(sql_store store, void *seq); /* called locked */
extern int seqbulk_next_value(sql_store store, sql_sequence *seq, lng cnt, lng* dest);

extern void sequences_lock(sql_store store);
extern void sequences_unlock(sql_store store);

#endif /* STORE_SEQ_H */
