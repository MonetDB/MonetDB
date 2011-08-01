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

#ifndef STORE_SEQ_H
#define STORE_SEQ_H

#include "sql_catalog.h"

extern void sequences_init(void);
extern void sequences_exit(void);

extern int seq_next_value(sql_sequence *seq, lng *val);

/* for bulk next values, the API is split in 3 parts */

typedef struct seqbulk {
	void *internal_seq;
	sql_sequence *seq;
	BUN cnt;
	int save; 
} seqbulk;

extern seqbulk *seqbulk_create(sql_sequence *seq, BUN cnt);
extern int seqbulk_next_value(seqbulk *seq, lng *val);
extern void seqbulk_destroy(seqbulk *seq);

extern int seq_get_value(sql_sequence *seq, lng *val);
extern int seq_restart(sql_sequence *seq, lng start);

#endif /* STORE_SEQ_H */
