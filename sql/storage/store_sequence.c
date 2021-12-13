/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
	bool called;
	bool intrans;
} store_sequence;

void
log_store_sequence(sql_store Store, void *s)
{
	sqlstore *store = Store;
	store_sequence *seq = s;
	store->logger_api.log_sequence(store, seq->seqid,  (seq->called)?seq->cur:lng_nil);
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
static void
update_sequence(sqlstore *store, store_sequence *s)
{
	if (!s->intrans)
		list_append(store->seqchanges, s);
	s->intrans = true;
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

	if (!isNew(seq) && store->logger_api.get_sequence(store, seq->base.id, &val )) {
		s->cur = val;
		if (val != lng_nil)
			s->called = 1; /* val is last used value */
	}
	hash_add(store->sequences, seq_hash(s), s);
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
	s->cur = start;
	s->called = 0;
	update_sequence(store, s);
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

	lng start_index = 0;
	bool initial = false;

	if (!s->called) {
		s->cur = seq->start;
		*dest = s->cur;
		start_index = 1;
		s->called = 1;
		initial = 1;
	}

	lng min = seq->minvalue;
	lng max = seq->maxvalue;
	lng cur = s->cur;

	bool store_unlocked = false;
	if (seq->increment > 0) {
		lng inc = seq->increment; // new value = old value + inc;

		if (start_index < cnt && !seq->cycle && !(max > 0 && s->cur < 0)) {
			if ((max -s->cur) / (cnt - start_index) >= inc) {
				s->cur += inc * (cnt - start_index);

				update_sequence(store, s);
				sequences_unlock(store);
				store_unlocked = true;
			} else {
				if (initial)
					s->called = 0;
				sequences_unlock(store);
				return 0;
			}
		}
		for(lng i = start_index; i < cnt; i++) {
			if ((GDK_lng_max - inc < cur) || ((cur += inc) > max)) {
				// overflow
				assert(seq->cycle);
				cur = min;
			}
			dest[i] = cur;
		}
	} else { // seq->increment < 0
		lng inc = -seq->increment; // new value = old value - inc;

		if (start_index < cnt && !seq->cycle && !(min < 0 && s->cur > 0)) {
			if ((s->cur - min) / (cnt - start_index) >= inc) {
				s->cur -= inc * (cnt - start_index);

				update_sequence(store, s);
				sequences_unlock(store);
				store_unlocked = true;
			} else {
				if (initial)
					s->called = 0;
				sequences_unlock(store);
				return 0;
			}
		}
		for(lng i = start_index; i < cnt; i++) {
			if ((-GDK_lng_max + inc > cur) || ((cur -= inc)  < min)) {
				// underflow
				assert(seq->cycle);
				cur = max;
			}
			dest[i] = cur;
		}
	}

	if (!store_unlocked) {
		s->cur = cur;

		update_sequence(store, s);
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
	*val = s->called ? s->cur : lng_nil;

	sequences_unlock(store);
	return 1;
}


int
seq_peak_next_value(sql_store Store, sql_sequence *seq, lng *val)
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

	if (!s->called) {
		*val = seq->start;
		sequences_unlock(store);
		return 1;
	}

	lng min = seq->minvalue;
	lng max = seq->maxvalue;

	*val = s->cur;

	if (seq->increment > 0) {
		lng inc = seq->increment; // new value = old value + inc;
		if ((GDK_lng_max - inc < *val) || ((*val += inc) > max)) {
			// overflow
			if (seq->cycle) {
				*val = min;
			} else {
				sequences_unlock(store);
				return 0;
			}
		}
	} else { // seq->increment < 0
		lng inc = -seq->increment; // new value = old value - inc;
		if ((-GDK_lng_max + inc > *val) || ((*val -= inc)  < min)) {
			// underflow
			if (seq->cycle) {
				*val = max;
			} else {
				sequences_unlock(store);
				return 0;
			}
		}
	}

	sequences_unlock(store);
	return 1;
}
