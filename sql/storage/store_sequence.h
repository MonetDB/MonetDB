/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef STORE_SEQ_H
#define STORE_SEQ_H

#include "sql_catalog.h"

extern void* sequences_init(void);
extern void sequences_exit(void);

extern int seq_get_value(sql_store store, sql_sequence *seq, lng *val);
extern int seq_next_value(sql_store store, sql_sequence *seq, lng *val);
extern int seq_restart(sql_store store, sql_sequence *seq, lng start);

/* for bulk calls, the API is split in 3 parts */

typedef struct seqbulk {
	void *internal_seq;
	sql_sequence *seq;
	BUN cnt;
	int save;
} seqbulk;

extern seqbulk *seqbulk_create(sql_store store, sql_sequence *seq, BUN cnt);
extern int seqbulk_get_value(seqbulk *seq, lng *val);
extern int seqbulk_next_value(seqbulk *seq, lng *val);
extern int seqbulk_restart(sql_store store, seqbulk *seq, lng start);
extern void seqbulk_destroy(sql_store store, seqbulk *seq);

#endif /* STORE_SEQ_H */
