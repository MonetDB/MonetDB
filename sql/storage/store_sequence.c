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

#include "monetdb_config.h"
#include "store_sequence.h"
#include "sql_storage.h"
#include <gdk_logger.h>

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

	s = NEW(store_sequence);
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
	seqbulk *sb = NEW(seqbulk);
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
