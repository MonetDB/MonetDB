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

#ifndef _CRACKERS_PQ_H_
#define _CRACKERS_PQ_H_

#ifdef _DEBUG
#	define PriQue_DEBUG
#endif

#define PriQue_TEST
#undef PriQue_STATS

unsigned int Random (unsigned int const range);

typedef unsigned int PQ_Index;
typedef unsigned int PQ_Key;
typedef char PQ_Shift;
typedef void * PQ_Data;

typedef int (PQ_CompFct) (
				PQ_Index const index_one,
				PQ_Data const data_one,
				PQ_Index const index_two,
				PQ_Data const data_two,
				void * const comp_info);

typedef struct
{
	PQ_Index index;
	PQ_Key normkey;
} PQ_Entry;

typedef struct
{
	char const * name;
	PQ_Index capacity;
	PQ_Key normkey_range;
	PQ_CompFct * comp_fct;
	void * comp_info;

	PQ_Index nodes;
	PQ_Index zero_entrynode;
	PQ_Index zero_sibling;
	PQ_Key run_incr;
	PQ_Shift loss;

#ifdef PriQue_DEBUG
	char defer;
#endif
	char runs;
	PQ_Key nextrun;
	PQ_Key prevkey;

	PQ_Entry * heap;
	PQ_Data * data;

#ifdef PriQue_STATS
	struct
	{
		unsigned long	move, compare, key_compare;
		unsigned long	push, pop, top, replace, erase, empty,
						easy_pass, search_pass;
#ifdef PriQue_DEBUG
		unsigned long	check, map_check, heap_check;
#endif
	} count;
#endif
} PQ_state;

size_t PQ_RequiredSpace (
		PQ_Index const capacity,
		int const save_data);

void PQ_PriorityQueue (
		PQ_state * const m,
		char const * const name,
		void * const space,
		size_t const size,
		PQ_Index const capacity,
		PQ_Key const normkey_range,
		int const find_runs,
		int const save_data,
		PQ_CompFct * const comp_fct,
		void * const comp_arg);
void PQ_xPriorityQueue (PQ_state * const m);

int PQ_empty (PQ_state * const m);
int /* deferred */ PQ_push (
		PQ_state * const m,
		PQ_Index const index,
		PQ_Key const normkey,
		int const defer,
		PQ_Data const data);
PQ_Index PQ_top (
		PQ_state * const m,
		int * const deferred,
		PQ_Data * const data);
PQ_Index PQ_pop (
		PQ_state * const m,
		int * const deferred,
		PQ_Data * const data);

void PQ_erase (
		PQ_state * const m,
		PQ_Index const index,
		PQ_Data * const data);
int /* deferred */ PQ_replace (
		PQ_state * const m,
		PQ_Index const index,
		PQ_Key const normkey,
		int const defer,
		PQ_Data * const data);

void PQ_reset (
		PQ_state * const m,
		int const heap,
		int const stats);
void PQ_drain (PQ_state * const m);

/*
static PQ_Index capacity (PQ_state const * const m) { return m->capacity; }
static int runs (PQ_state const * const m) { return m->runs; }
static PQ_Shift PQ_loss (PQ_state const * const m) { return m->loss; }
*/
#ifdef PriQue_STATS
	void PQ_sizes (PQ_state const * const m);
	void PQ_statistics (PQ_state const * const m, char const header []);
#endif

#ifdef PriQue_TEST
void PQ_selftest (
		int const progress,
		PQ_Index const heapsize,
		size_t const ops,
		PQ_Key const range,
		int test);
#endif
#endif /* _CRACKERS_PQ_H */
