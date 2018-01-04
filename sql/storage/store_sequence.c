/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "store_sequence.h"
#include "sql_storage.h"
#include "gdk_logger.h"

typedef struct store_sequence {
	sqlid seqid;
	bit called;
	lng cur;
	lng cached;
} store_sequence;

static list *sql_seqs = NULL;

static void 
sequence_destroy( store_sequence *s )
{
	_DELETE(s);
}

void sequences_init(void)
{
	sql_seqs = list_create( (fdestroy)sequence_destroy );
}

void sequences_exit(void)
{
	list_destroy(sql_seqs);
	sql_seqs = NULL;
}

/* lock is held */
static void 
sql_update_sequence_cache(sql_sequence *seq, lng cached) 
{
	logger_funcs.log_sequence(seq->base.id, cached);
}

/* lock is held */
static store_sequence *
sql_create_sequence(sql_sequence *seq ) 
{
	lng id = 0;
	store_sequence *s = NULL;
	s = MNEW(store_sequence);
	if(!s)
		return NULL;

	s -> seqid = seq->base.id;
	s -> called = 0;
	s -> cur = seq->start; 	  
	s -> cached = seq->start;

	if (seq->base.flag == TR_OLD && 
	    logger_funcs.get_sequence(seq->base.id, &id )) 
		s->cached = id;
	s -> cur = s->cached;
	return s;
}

int seq_restart(sql_sequence *seq, lng start)
{
	node *n = NULL;
	store_sequence *s;

	store_lock();
	for ( n = sql_seqs->h; n; n = n ->next ) {
		s = n->data;
		if (s->seqid == seq->base.id)
			break;
	}
	if (!n) {
		s = sql_create_sequence(seq);
		if (!s) {
			store_unlock();
			return 0;
		}
		list_append(sql_seqs, s);
	} else {
		s = n->data;
	}
	s->called = 0;
	s->cur = start;
	s->cached = start;
	/* handle min/max and cycle */
	if ((seq->maxvalue && s->cur > seq->maxvalue) ||
	    (seq->minvalue && s->cur < seq->minvalue))
	{
		/* we're out of numbers */
		store_unlock();
		return 0;
	}
	sql_update_sequence_cache(seq, s->cached);
	store_unlock(); 
	return 1;
}


int
seq_next_value(sql_sequence *seq, lng *val)
{
	lng nr = 0;
	node *n = NULL;
	store_sequence *s;
	int save = 0;

	*val = 0;
	store_lock();
	for ( n = sql_seqs->h; n; n = n ->next ) {
		s = n->data;
		if (s->seqid == seq->base.id)
			break;
	}
	if (!n) {
		s = sql_create_sequence(seq);
		if (!s) {
			store_unlock();
			return 0;
		}
		list_append(sql_seqs, s);
	} else {
		s = n->data;
		if (s->called)
			s->cur += seq->increment;
	}
	/* handle min/max and cycle */
	if ((seq->maxvalue && s->cur > seq->maxvalue) ||
	    (seq->minvalue && s->cur < seq->minvalue))
	{
		if (seq->cycle) {
			/* cycle to the min value again */
			s->cur = seq->minvalue;
			save = 1;
		} else { /* we're out of numbers */
			store_unlock();
			return 0;
		}
	}
	s->called = 1;
	nr = s->cur;
	*val = nr;
	if (save || nr == s->cached) {
		s->cached = nr + seq->cacheinc*seq->increment;
		sql_update_sequence_cache(seq, s->cached);
		store_unlock(); 
		return 1;
	}
	assert(nr<s->cached);
	store_unlock(); 
	return 1;
}

seqbulk *seqbulk_create(sql_sequence *seq, BUN cnt)
{
	seqbulk *sb = MNEW(seqbulk);
	store_sequence *s;
	node *n = NULL;

	if (!sb)
		return NULL;

	store_lock();
	sb->seq = seq;
	sb->cnt = cnt;
	sb->save = 0;

	for ( n = sql_seqs->h; n; n = n ->next ) {
		s = n->data;
		if (s->seqid == seq->base.id)
			break;
	}
	if (!n) {
		s = sql_create_sequence(seq);
		if (!s) {
			_DELETE(sb);
			store_unlock();
			return NULL;
		}
		list_append(sql_seqs, s);
	} else {
		s = n->data;
	}
	sb->internal_seq = s;
	return sb;
}

void seqbulk_destroy(seqbulk *sb)
{
	if (sb->save) {
		sql_sequence *seq = sb->seq;
		store_sequence *s = sb->internal_seq;

		sql_update_sequence_cache(seq, s->cached);
	}
	_DELETE(sb);
	store_unlock();
}

int seqbulk_next_value(seqbulk *sb, lng *val)
{
	lng nr = 0;
	store_sequence *s = sb->internal_seq;
	sql_sequence *seq = sb->seq;

	if (s->called)
		s->cur += seq->increment;

	/* handle min/max and cycle */
	if ((seq->maxvalue && s->cur > seq->maxvalue) ||
	    (seq->minvalue && s->cur < seq->minvalue))
	{
		if (seq->cycle) {
			/* cycle to the min value again */
			s->cur = seq->minvalue;
			sb->save = 1;
		} else { /* we're out of numbers */
			return 0;
		}
	}
	s->called = 1;
	nr = s->cur;
	*val = nr;
	if (nr == s->cached) {
		s->cached = nr + seq->cacheinc*seq->increment;
		sb->save = 1;
		return 1;
	}
	assert(nr<s->cached);
	return 1;
}

int
seq_get_value(sql_sequence *seq, lng *val)
{
	node *n = NULL;
	store_sequence *s;

	*val = 0;
	store_lock();
	for ( n = sql_seqs->h; n; n = n ->next ) {
		s = n->data;
		if (s->seqid == seq->base.id)
			break;
	}
	if (!n) {
		s = sql_create_sequence(seq);
		if (!s) {
			store_unlock();
			return 0;
		}
		list_append(sql_seqs, s);
	} else {
		s = n->data;
	}
	*val = s->cur;
	if (s->called)
		*val += seq->increment;
	/* handle min/max and cycle */
	if ((seq->maxvalue && *val > seq->maxvalue) ||
	    (seq->minvalue && *val < seq->minvalue))
	{
		if (seq->cycle) {
			/* cycle to the min value again */
			*val = seq->minvalue;
		} else { /* we're out of numbers */
			store_unlock();
			return 0;
		}
	}
	store_unlock();
	return 1;
}
