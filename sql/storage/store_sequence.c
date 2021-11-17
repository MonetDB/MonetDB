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

typedef struct store_sequence {
	sqlid seqid;
	bit called;
	lng cur;
	lng cached;
} store_sequence;

static list *sql_seqs = NULL;

static void
sequence_destroy( void *dummy, store_sequence *s )
{
	(void)dummy;
	_DELETE(s);
}

void*
sequences_init(void)
{
	sql_seqs = list_create( (fdestroy)sequence_destroy );
	return (void*) sql_seqs;
}

void
sequences_exit(void)
{
	if(sql_seqs) {
		list_destroy(sql_seqs);
		sql_seqs = NULL;
	}
}

/* lock is held */
static void
sql_update_sequence_cache(sqlstore *store, sql_sequence *seq, lng cached)
{
	store->logger_api.log_sequence(store, seq->base.id, cached);
}

/* lock is held */
static store_sequence *
sql_create_sequence(sqlstore *store, sql_sequence *seq )
{
	lng id = 0;
	store_sequence *s = NULL;
	s = MNEW(store_sequence);
	if(!s)
		return NULL;

	*s = (store_sequence) {
		.seqid = seq->base.id,
		.cur = seq->start,
		.cached = seq->start,
	};

	if (!isNew(seq) && store->logger_api.get_sequence(store, seq->base.id, &id )) {
		s->cached = id;
	}
	s -> cur = s->cached;
	return s;
}

static inline lng
calculate_new_cached_value(lng cv /*current value*/, lng inc /*sequence increment*/, lng ci /*cache increment*/, lng min, lng max)
{
	lng ncv; // new cached value;
	lng cl; // cache line


	if (inc > 0) {
		cl = inc * ci;
		/* New cached value is the start of the first cacheline that is bigger then the current value
		 * or
		 * it is set to the minimum value of the sequence in case of a GDK or max value overflow.
		 * This computation also requires overflow checks hence its complexity.*/
		if (GDK_lng_max - cl < ((cv / cl)*cl) || (ncv = ((cv / cl)*cl) + cl) > max ) 
			ncv = min;
	}
	else {
		cl = -inc * ci;
		/* New cached value is first cacheline that is smaller then the current value
		 * or
		 * it is set to the maximum value of the sequence in case of a GDK or min value underflow.
		 * This computation also requires underflow checks hence its complexity.*/
		if (-GDK_lng_max + cl > ((cv / cl)*cl) || (ncv = ((cv / cl)*cl) - cl) < min ) 
			ncv = max;
	}
	return ncv;
}

int
seq_restart(sql_store store, sql_sequence *seq, lng start)
{
	node *n = NULL;
	store_sequence *s;

	assert(!is_lng_nil(start));
	store_lock(store);
	for ( n = sql_seqs->h; n; n = n ->next ) {
		s = n->data;
		if (s->seqid == seq->base.id)
			break;
	}
	if (!n) {
		s = sql_create_sequence(store, seq);
		if (!s) {
			store_unlock(store);
			return 0;
		}
		list_append(sql_seqs, s);
	} else {
		s = n->data;
	}
	s->cur = start;
	s->called = 0;
	s->cached = s->cur;
	sql_update_sequence_cache(store, seq, s->cached);
	store_unlock(store);
	return 1;
}

int
seqbulk_next_value(sql_store store, sql_sequence *seq, lng cnt, lng* dest)
{
	node *n = NULL;
	store_sequence *s;

	// either dest is an array of size cnt or dest is a normal pointer and cnt == 1.

	assert(dest);

	store_lock(store);
	for ( n = sql_seqs->h; n; n = n ->next ) {
		s = n->data;
		if (s->seqid == seq->base.id)
			break;
	}
	if (!n) {
		s = sql_create_sequence(store, seq);
		if (!s) {
			store_unlock(store);
			return 0;
		}
		list_append(sql_seqs, s);
	} else {
		s = n->data;
	}

	lng start_index = 0;

	if (!s->called) {
		s->cur = seq->start;
		*dest = s->cur;
		start_index = 1;
		s->called = 1;
	}

	lng min = seq->minvalue;
	lng max = seq->maxvalue;

	if (seq->increment > 0) {
		lng inc = seq->increment; // new value = old value + inc;
		for(lng i = start_index; i < cnt; i++) {
			if ((GDK_lng_max - inc < s->cur) || ((s->cur += inc) > max)) {
				// overflow
				if (seq->cycle) {
					s->cur = min;
				}
				else {
					store_unlock(store);
					return 0;
				}
			}
			dest[i] = s->cur;
		}
	}
	else { // seq->increment < 0
		lng inc = -seq->increment; // new value = old value - inc;		
		for(lng i = start_index; i < cnt; i++) {
			if ((-GDK_lng_max + inc > s->cur) || ((s->cur -= inc)  < min)) {
				// underflow
				if (seq->cycle) {
					s->cur = max;
				}
				else {
					store_unlock(store);
					return 0;
				}
			}
			dest[i] = s->cur;
		}
	}

	lng old_cached = s->cached;
	s->cached = calculate_new_cached_value(s->cur, seq->increment, seq->cacheinc, min, max);

	if (old_cached != s->cached) {
		sql_update_sequence_cache(store, seq, s->cached);
	}

	store_unlock(store);
	return 1;
}

int
seq_next_value(sql_store store, sql_sequence *seq, lng *val)
{
	return seqbulk_next_value(store, seq, 1, val);
}

int
seq_get_value(sql_store store, sql_sequence *seq, lng *val)
{
	node *n = NULL;
	store_sequence *s;

	*val = 0;
	store_lock(store);
	for ( n = sql_seqs->h; n; n = n ->next ) {
		s = n->data;
		if (s->seqid == seq->base.id)
			break;
	}
	if (!n) {
		s = sql_create_sequence(store, seq);
		if (!s) {
			store_unlock(store);
			return 0;
		}
		list_append(sql_seqs, s);
	} else {
		s = n->data;
	}

	*val = s->cur; 
		
	store_unlock(store);
	return 1;
}


int
seq_peak_next_value(sql_store store, sql_sequence *seq, lng *val)
{
	node *n = NULL;
	store_sequence *s;

	*val = 0;
	store_lock(store);
	for ( n = sql_seqs->h; n; n = n ->next ) {
		s = n->data;
		if (s->seqid == seq->base.id)
			break;
	}
	if (!n) {
		s = sql_create_sequence(store, seq);
		if (!s) {
			store_unlock(store);
			return 0;
		}
		list_append(sql_seqs, s);
	} else {
		s = n->data;
	}

	if (!s->called) {
		if (isNew(seq)) {
			*val = seq->start;
			store_unlock(store);
			return 1;
		}
		else {
			*val = s->cached;
			store_unlock(store);
			return 1;
		}
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
			}
			else {
				store_unlock(store);
				return 0;
			}
		}
	}
	else { // seq->increment < 0
		lng inc = -seq->increment; // new value = old value - inc;
		if ((-GDK_lng_max + inc > *val) || ((*val -= inc)  < min)) {
			// underflow
			if (seq->cycle) {
				*val = max;
			}
			else {
				store_unlock(store);
				return 0;
			}
		}
	}

	store_unlock(store);
	return 1;
}
