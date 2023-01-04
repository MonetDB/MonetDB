/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "store_sequence.h"
#include "sql_storage.h"

void
sequences_lock(sql_store Store)
{
	sqlstore *store = Store;
	MT_lock_set(&store->column_locks[NR_COLUMN_LOCKS-1]);
}

void
sequences_unlock(sql_store Store)
{
	sqlstore *store = Store;
	MT_lock_unset(&store->column_locks[NR_COLUMN_LOCKS-1]);
}

typedef struct store_sequence {
	sqlid seqid;
	lng cur;
	bool intrans;
} store_sequence;

void
log_store_sequence(sql_store Store, void *s)
{
	sqlstore *store = Store;
	store_sequence *seq = s;
	store->logger_api.log_tsequence(store, seq->seqid,  seq->cur);
	seq->intrans = false;
}

int
seq_hash(void *s)
{
	store_sequence *seq = s;
	return seq->seqid;
}

static void
sequence_destroy( void *dummy, store_sequence *s )
{
	(void)dummy;
	_DELETE(s);
}

void
seq_hash_destroy( sql_hash *h )
{
	if (h == NULL || h->sa)
		return ;
	for (int i = 0; i < h->size; i++) {
		sql_hash_e *e = h->buckets[i];

		while (e) {
			sql_hash_e *next = e->chain;

			sequence_destroy(NULL, e->value);
			_DELETE(e);
			e = next;
		}
	}
	_DELETE(h->buckets);
	_DELETE(h);
}

static store_sequence *
sequence_lookup( sql_hash *h, sqlid id)
{
	sql_hash_e *e = h->buckets[id & (h->size-1)];
	while(e) {
		sql_hash_e *next = e->chain;
		store_sequence *s = e->value;

		if (s->seqid == id)
			return s;
		e = next;
	}
	return NULL;
}

/* lock is held */
static store_sequence *
update_sequence(sqlstore *store, store_sequence *s)
{
	if (!s->intrans && !list_append(store->seqchanges, s))
		return NULL;
	s->intrans = true;
	return s;
}

/* lock is held */
static store_sequence *
sequence_create(sqlstore *store, sql_sequence *seq )
{
	lng val = 0;
	store_sequence *s = NULL;
	s = MNEW(store_sequence);
	if(!s)
		return NULL;

	*s = (store_sequence) {
		.seqid = seq->base.id,
		.cur = seq->start,
	};

	if (!isNew(seq) && store->logger_api.get_sequence(store, seq->base.id, &val ))
		s->cur = val;
	if (!hash_add(store->sequences, seq_hash(s), s)) {
		_DELETE(s);
		return NULL;
	}
	return s;
}

int
seq_restart(sql_store Store, sql_sequence *seq, lng start)
{
	store_sequence *s;
	sqlstore *store = Store;

	assert(!is_lng_nil(start));
	sequences_lock(store);
	s = sequence_lookup(store->sequences, seq->base.id);

	if (!s) {
		lng val = 0;

		if (isNew(seq) || !store->logger_api.get_sequence(store, seq->base.id, &val )) {
			sequences_unlock(store);
			return 1;
		} else {
			s = sequence_create(store, seq);
			if (!s) {
				sequences_unlock(store);
				return 0;
			}
		}
	}
	lng ocur = s->cur;
	s->cur = start;
	if (!update_sequence(store, s)) {
		s->cur = ocur;
		sequences_unlock(store);
		return 0;
	}
	sequences_unlock(store);
	return 1;
}

int
seqbulk_next_value(sql_store Store, sql_sequence *seq, lng cnt, lng* dest)
{
	store_sequence *s;
	sqlstore *store = Store;

	// either dest is an array of size cnt or dest is a normal pointer and cnt == 1.

	assert(dest);

	sequences_lock(store);
	s = sequence_lookup(store->sequences, seq->base.id);
	if (!s) {
		s = sequence_create(store, seq);
		if (!s) {
			sequences_unlock(store);
			return 0;
		}
	}

	lng min = seq->minvalue;
	lng max = seq->maxvalue;
	lng cur = s->cur;

	if (!seq->cycle) {
		if ((seq->increment > 0 && s->cur > max) ||
		    (seq->increment < 0 && s->cur < min)) {
			sequences_unlock(store);
			return 0;
		}
	}
	bool store_unlocked = false;
	if (seq->increment > 0) {
		lng inc = seq->increment; // new value = old value + inc;

		if (0 < cnt && !seq->cycle && !(max > 0 && s->cur < 0)) {
			if ((max - s->cur) >= ((cnt-1) * inc)) {
				lng ocur = s->cur;
				s->cur += inc * cnt;

				if (!update_sequence(store, s)) {
					s->cur = ocur;
					sequences_unlock(store);
					return 0;
				}
				sequences_unlock(store);
				store_unlocked = true;
			} else {
				sequences_unlock(store);
				return 0;
			}
		}
		for(lng i = 0; i < cnt; i++) {
			dest[i] = cur;
			if ((GDK_lng_max - inc < cur) || ((cur += inc) > max)) {
				// overflow
				cur = (seq->cycle)?min:lng_nil;
			}
		}
	} else { // seq->increment < 0
		lng inc = -seq->increment; // new value = old value - inc;

		if (0 < cnt && !seq->cycle && !(min < 0 && s->cur > 0)) {
			if ((s->cur - min) >= ((cnt-1) * inc)) {
				lng ocur = s->cur;
				s->cur -= inc * cnt;

				if (!update_sequence(store, s)) {
					s->cur = ocur;
					sequences_unlock(store);
					return 0;
				}
				sequences_unlock(store);
				store_unlocked = true;
			} else {
				sequences_unlock(store);
				return 0;
			}
		}
		for(lng i = 0; i < cnt; i++) {
			dest[i] = cur;
			if ((-GDK_lng_max + inc > cur) || ((cur -= inc)  < min)) {
				// underflow
				cur = (seq->cycle)?max:lng_nil;
			}
		}
	}

	if (!store_unlocked) {
		lng ocur = s->cur;
		s->cur = cur;

		if (!update_sequence(store, s)) {
			s->cur = ocur;
			sequences_unlock(store);
			return 0;
		}
		sequences_unlock(store);
	}
	return 1;
}

int
seq_next_value(sql_store store, sql_sequence *seq, lng *val)
{
	return seqbulk_next_value(store, seq, 1, val);
}

int
seq_get_value(sql_store Store, sql_sequence *seq, lng *val)
{
	store_sequence *s;
	sqlstore *store = Store;

	*val = 0;
	sequences_lock(store);
	s = sequence_lookup(store->sequences, seq->base.id);
	if (!s) {
		s = sequence_create(store, seq);
		if (!s) {
			sequences_unlock(store);
			return 0;
		}
	}
	*val = s->cur;
	sequences_unlock(store);
	return 1;
}
