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

/*
 * @f crackers_pq
 * @a Goetz Graefe, Stratos Idreos
 * @d March 2006 - July 2007
 * @* Priority Queue
 *
 *
 * @+ Interface
 *
 * @- Header file
 */
/*
 * @
 *
 * @- Exported signatures
 *
 * @+ Implementation
 *
 */
#include "monetdb_config.h"
#include "crackers.h"

#if defined (_DEBUG)  ||  defined (PriQue_DEBUG)  ||  \
		defined (PriQue_STATS)  ||  defined (PriQue_TEST)
#include <stdlib.h>
#include <stdio.h>
#define tagprintf printf ("%s,%d: ", __FILE__, __LINE__), printf
#define DebugAssert(b) if (!(b)) tagprintf ("assert\n"), exit (1)
#define valid_bool(b)	((b) == 1  ||  (b) == 0)
#endif

unsigned int Random (unsigned int const range)
{
	return (unsigned int) rand () % range;
}

static PQ_Key PQ_minKey () { return (PQ_Key) 0; }
static PQ_Key PQ_maxKey () { return ~ PQ_minKey (); }
static PQ_Index PQ_badIndex () { return (PQ_Index) ~0; }

#ifdef PriQue_DEBUG
static void PQ_AssertConsistency (
		PQ_state * const m,
		int const heap_check,
		int const map_check);
#endif

static void PQ_pass (PQ_state * const m, PQ_Entry * candidate);

static int /* deferred */ PQ_adjust_normkey (
	PQ_state * const m,
	PQ_Entry * candidate,
	int const defer);

static void PQ_reduce_keys (PQ_state * const m);

static PQ_Shift PQ_bitsRunNo () { return 3; }

static PQ_Key PQ_early_sentinel (PQ_Index const index)
{
	return (PQ_Key) index;
}

static PQ_Key PQ_late_sentinel (PQ_Index const index)
{
	return PQ_maxKey () - (PQ_Key) index;
}

static int PQ_is_sentinel (
		PQ_state const * const m,
		PQ_Key const normkey)
{
	return (normkey < m->nodes  ||  normkey > PQ_maxKey () - m->nodes);
}

static inline void PQ_parent (PQ_Index * entry)
{
	(*entry) >>= 1;
}

static inline PQ_Index PQ_leaf (
		PQ_state const * const m,
		PQ_Index const index)
{
	PQ_Index entry = index;
	if (entry < m->zero_sibling)
		entry += m->zero_sibling;
	if (entry >= m->nodes)
		PQ_parent (& entry);
	return entry;
}

static inline PQ_Index PQ_root ()
{
	return (PQ_Index) 0;
}

static void PQ_parent_mask (PQ_Index * mask)
{
	(*mask) &= (*mask - 1);
}

static PQ_Index PQ_leaf_mask (
		PQ_state const * const m,
		PQ_Index const entry)
{
	PQ_Index mask = m->zero_sibling - 1;
	if (entry < m->zero_entrynode)
		PQ_parent_mask (& mask);
	return mask;
}

static int PQ_siblings (
		PQ_Index const left, PQ_Index const right,
		PQ_Index const mask)
{
	return (left & mask) == (right & mask);
}

static int PQ_data_compare (PQ_state * const m,
		PQ_Entry const * const left, PQ_Entry const * const right)
{
	int result;
#ifdef PriQue_STATS
	++ m->count.key_compare;
#endif

	result = m->comp_fct (
			left->index,
			(m->data == NULL ? NULL : m->data [left->index]),
			right->index,
			(m->data == NULL ? NULL :
				m->data [right->index == left->index ?
					m->capacity : right->index]),
			m->comp_info);

	return result;
}

static inline int PQ_compare (PQ_state * const m,
		PQ_Entry const * const left, PQ_Entry const * const right)
{
	int result;
#ifdef PriQue_STATS
	++ m->count.compare;
#endif


	if (left->normkey > right->normkey)
		result = 1;
	else if (left->normkey < right->normkey)
		result = -1;

	else if (m->comp_fct == NULL)
		result = 0;
	else
		result = PQ_data_compare (m, left, right);

	return result;
}

size_t PQ_RequiredSpace (PQ_Index const capacity, int const save_data)
{
	PQ_Index const nodes = capacity + (capacity & 01);
	return nodes * sizeof (PQ_Entry) +
			(save_data ? (capacity + 1) * sizeof (PQ_Data) : 0);
}

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
		void * const comp_info)
{
	PQ_Key max_range;
	(void) size;

	m->name = name;
	m->capacity = capacity;
	m->normkey_range = normkey_range;

	m->comp_fct = comp_fct;
	m->comp_info = comp_info;
	m->runs = find_runs ? 1 : 0;

	m->nodes = capacity + (capacity & 01);
	for (m->zero_sibling = 2;  m->zero_sibling < m->capacity;  )
		m->zero_sibling <<= 1;
	m->zero_sibling >>= 1;
	m->zero_entrynode = m->zero_sibling;

	max_range = PQ_maxKey () - 2 * m->nodes;
	if (m->runs > 0)
		max_range >>= PQ_bitsRunNo ();
	m->loss = 0;
	while ((m->normkey_range >> m->loss) >= max_range)
		++ m->loss;
	m->run_incr = (m->normkey_range >> m->loss) + 1;

	{
		void * address = space;

		m->heap = (PQ_Entry *) address;
		address = (void *) (m->heap + m->nodes);

		m->data = NULL;
		if (save_data)
		{
			m->data = (PQ_Data *) address;
			address = (void *) (m->data + (m->capacity + 1));
		}
	}

	PQ_reset (m, 1, 1);

#ifdef PriQue_STATS
	PQ_sizes (m);
#endif
}

void PQ_xPriorityQueue (PQ_state * const m)
{
(void)m;
#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

#ifdef PriQue_STATS
	PQ_statistics (m, "at destruction");
#endif
}

int PQ_empty (PQ_state * const m)
{
	int result;
#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

#ifdef PriQue_STATS
	++ m->count.empty;
#endif

	for (;;)
	{
		PQ_Entry * const entry = & m->heap [PQ_root ()];
		PQ_Entry candidate;

		if (entry->normkey == PQ_late_sentinel (entry->index))
		{
			result = 1;
			break;
		}

		if (entry->normkey != PQ_early_sentinel (entry->index))
		{
			result = 0;
			break;
		}

		candidate.index = entry->index;
		candidate.normkey = PQ_late_sentinel (entry->index);
		PQ_pass (m, & candidate);
	}

#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

	return result;
}

int /* deferred */ PQ_push (
		PQ_state * const m,
		PQ_Index const index,
		PQ_Key const normkey,
		int const defer,
		PQ_Data const data)
{
	PQ_Entry candidate;
	int deferred;
#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

#ifdef PriQue_STATS
	++ m->count.push;
#endif

	candidate.index = index;
	candidate.normkey = normkey;
	deferred = PQ_adjust_normkey (m, & candidate, defer);
	if (m->data != NULL)
		m->data [index] = data;

	PQ_pass (m, & candidate);

#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

	return deferred;
}

PQ_Index PQ_top (
		PQ_state * const m,
		int * const deferred,
		PQ_Data * const data)
{
	PQ_Index index;
#ifdef PriQue_STATS
	++ m->count.top;
#endif
	if (PQ_empty (m))
		return PQ_badIndex ();
	{
	PQ_Entry const * const root_entry = & m->heap [PQ_root ()];
	index = root_entry->index;
	if (deferred != NULL)
		*deferred = (m->runs != 0  &&  root_entry->normkey >= m->nextrun);
	if (data != NULL)
		*data = m->data [index];

#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif
	}
	return index;
}

PQ_Index PQ_pop (
		PQ_state * const m,
		int * const deferred,
		PQ_Data * const data)
{
	int l_deferred;
	PQ_Index index;
#ifdef PriQue_STATS
	++ m->count.pop;
#endif

	index = PQ_top (m, & l_deferred, data);
	if (index == PQ_badIndex ())
		return PQ_badIndex ();

	m->prevkey = m->heap [PQ_root ()].normkey;
	if (deferred != NULL)
		*deferred = l_deferred;
	if (l_deferred)
	{
#ifdef PriQue_DEBUG
		m->defer = 0;
#endif
		m->runs = 1;

		m->nextrun += m->run_incr;
		if (m->nextrun > PQ_maxKey () - m->nodes - m->run_incr)
			PQ_reduce_keys (m);
	}

	m->heap [PQ_root ()].normkey = PQ_early_sentinel (index);
#ifdef PriQue_DEBUG
	if (m->data != NULL)
		m->data [index] = NULL;
#endif

#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

	return index;
}

void PQ_erase (
		PQ_state * const m,
		PQ_Index const index, PQ_Data * const data)
{
#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

#ifdef PriQue_STATS
	++ m->count.erase;
#endif

	if (data != NULL)
		*data = m->data [index];
#ifdef PriQue_DEBUG
	if (m->data != NULL)
		m->data [index] = NULL;
#endif

	if (m->heap [PQ_root ()].index == index)
	{
		m->heap [PQ_root ()].normkey = PQ_early_sentinel (index);
	}

	else
	{
		PQ_Entry candidate;
		candidate.index = index;
		candidate.normkey = PQ_late_sentinel (index);
		PQ_pass (m, & candidate);
	}

#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif
}

int /* deferred */ PQ_replace (
		PQ_state * const m,
		PQ_Index const index,
		PQ_Key const normkey,
		int const defer,
		PQ_Data * const data)
{
	PQ_Entry candidate;
	int deferred;
#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

#ifdef PriQue_STATS
	++ m->count.replace;
#endif

	candidate.index = index;
	candidate.normkey = normkey;
	deferred = PQ_adjust_normkey (m, & candidate, defer);
	if (m->data != NULL  &&  data != NULL)
		m->data [m->capacity] = * data;

	PQ_pass (m, & candidate);

	if (m->data != NULL  &&  data != NULL)
	{
		* data = m->data [index];
		m->data [index] = m->data [m->capacity];
	}

#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 0);
#endif

	return deferred;
}

void PQ_reset (
		PQ_state * const m,
		int const heap,
		int const stats)
{
	(void)stats;
	m->prevkey = m->nodes - 1;
	m->nextrun = m->nodes + m->run_incr;
	if (m->runs > 1)
		m->runs = 1;
#ifdef PriQue_DEBUG
	m->defer = 0;
#endif

	if (heap)
	{
		PQ_Index index, entry;

		for (index = m->nodes;  index -- > 0;  )
			m->heap [index].index = PQ_badIndex ();

		if (m->nodes > m->capacity)
		{
			m->heap [m->capacity].index = m->capacity;
			m->heap [m->capacity].normkey = PQ_late_sentinel (m->capacity);
		}

		for (index = 0;  index < m->capacity;  ++ index)
		{
			for (entry = PQ_leaf (m, index);
					m->heap [entry].index != PQ_badIndex ();
					PQ_parent (& entry))
			{
				;
			}

			m->heap [entry].index = index;
			m->heap [entry].normkey = PQ_late_sentinel (index);
		}

#ifdef PriQue_DEBUG
		if (m->data != NULL)
		{
			for (index = m->capacity;  index -- > 0;  )
				m->data [index] = NULL;
		}
#endif
	}

#ifdef PriQue_STATS
	if (stats)
	{
		m->count.push = m->count.pop = m->count.top = 0;
		m->count.replace = m->count.erase = m->count.empty = 0;
		m->count.easy_pass = m->count.search_pass = 0;
		m->count.move = m->count.compare = m->count.key_compare = 0;
#ifdef PriQue_DEBUG
		m->count.check = m->count.map_check = m->count.heap_check = 0;
#endif
	}
#endif

#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m, 1, 1);
#endif
}

void PQ_drain (PQ_state * const m)
{
	while (PQ_pop (m, NULL, NULL) != PQ_badIndex ())
		;
}

int /* deferred */ PQ_adjust_normkey (
		PQ_state * const m,
		PQ_Entry * candidate,
		int const defer)
{
	candidate->normkey = candidate->normkey >> m->loss;

	candidate->normkey += m->nextrun - m->run_incr;
#ifdef PriQue_DEBUG
	if (defer > 0)
		m->defer = 1;
#endif

	if (m->runs == 0  ||  defer < 0  ||
			(defer == 0  &&  candidate->normkey >= m->prevkey  &&
			(candidate->normkey > m->prevkey || m->comp_fct == NULL)))
		return 0;
	else
	{
		m->runs = 2;
		candidate->normkey += m->run_incr;

		return 1;
	}
}

void PQ_reduce_keys (PQ_state * const m)
{
	PQ_Key new_nextrun = m->nodes + m->run_incr;
	PQ_Key delta = m->nextrun - new_nextrun;
	PQ_Entry * ptr;
	m->nextrun = new_nextrun;
	m->prevkey -= delta;
	for (ptr = & m->heap [m->nodes];  ptr -- > m->heap;  )
	{
		if ( ! PQ_is_sentinel (m, ptr->normkey))
			ptr->normkey -= delta;
	}

#ifdef PriQue_DEBUG
	PQ_AssertConsistency (m,1,1);
#endif
}

void PQ_pass (PQ_state * const m, PQ_Entry * candidate)
{
	if (candidate->index != m->heap [PQ_root ()].index)
	{
		PQ_Entry * stack [8 * sizeof (void *)];
		PQ_Entry ** top_of_stack = stack;
		PQ_Index entry;
		PQ_Index mask;
#ifdef PriQue_STATS
		++ m->count.search_pass;
#endif

		*top_of_stack = candidate;

		entry = PQ_leaf (m, candidate->index);
		mask = PQ_leaf_mask (m, entry);
		for (  ;  m->heap [entry].index != candidate->index;
				PQ_parent (& entry), PQ_parent_mask (& mask))
			if (PQ_compare (m, & m->heap [entry], * top_of_stack) < 0)
				* ++ top_of_stack = & m->heap [entry];

		if (top_of_stack > stack)
		{
#ifdef PriQue_STATS
			m->count.move += (top_of_stack - stack);
#endif
			m->heap [entry] = ** top_of_stack;
			while (top_of_stack -- > stack)
				* top_of_stack [1] = * top_of_stack [0];
		}

		else
		{
			PQ_Index free = entry;
			PQ_Index free_mask = mask;
			do
			{
				PQ_parent (& entry); PQ_parent_mask (& mask);

				if ( ! PQ_siblings (m->heap [entry].index,
						m->heap [free].index, free_mask))
					continue;

				if (PQ_compare (m, & m->heap [entry], candidate) < 0)
					break;

#ifdef PriQue_STATS
				++ m->count.move;
#endif
				m->heap [free] = m->heap [entry];

				free = entry;
				free_mask = mask;
			} while (entry != PQ_root ());

			m->heap [free] = *candidate;
		}

#ifdef PriQue_STATS
		++ m->count.move;
#endif
	}

	else
	{
		PQ_Entry * stack [8 * sizeof (void *)];
		PQ_Entry ** top_of_stack = stack;
		PQ_Index entry;
		* top_of_stack = candidate;

		for (entry = PQ_leaf (m, candidate->index);
				entry != PQ_root ();  PQ_parent (& entry))
			if (PQ_compare (m, & m->heap [entry], * top_of_stack) < 0)
				* ++ top_of_stack = & m->heap [entry];

#ifdef PriQue_STATS
		++ m->count.easy_pass;
		++ m->count.move;
		m->count.move += (top_of_stack - stack);
#endif

		m->heap [PQ_root ()] = ** top_of_stack;
		while (top_of_stack -- > stack)
			* top_of_stack [1] = * top_of_stack [0];
	}
}

#ifdef PriQue_DEBUG

char const * const PQ_tag (PQ_state const * const m, PQ_Entry const *heap)
{
	return heap->normkey < m->nodes ? "early" :
			heap->normkey > PQ_maxKey () - m->nodes ? "late" :
			heap->normkey < m->nextrun ? "this" : "next";
}

void PQ_print_key (
		PQ_state const * const m,
		char const prefix [],
		PQ_Entry const *heap,
		char const suffix [])
{
	printf ("%s index %ld tag '%s' - normkey %lu",
			prefix, (long) (heap->index),
			PQ_tag (m, heap), (unsigned long) (heap->normkey));

	if (PQ_is_sentinel (m, heap->normkey))
	{
		printf (" %s sentinel",
			heap->normkey == PQ_early_sentinel (heap->index) ?
				"early" : "late");
	}

	else
	{
		PQ_Index const delta = heap->normkey - m->nodes;

		printf (" = run %lu, key %lu",
				(unsigned long) (delta / m->run_incr),
				(unsigned long) (delta % m->run_incr));

		if (m->data != NULL)
			printf (" data " PTRFMT,
					PTRFMTCAST m->data [heap->index]);
	}

	printf ("%s", suffix);
}

void PQ_print (PQ_state const * const m, char const header [])
{
	PQ_Index entry;

	printf ("*** priority heap \"%s\" with %ld entries: %s ***\n",
			m->name, (long) (m->capacity), header);


	for (entry = 0;  entry < m->nodes;  ++ entry)
	{
		printf ("entry %ld", (long) (entry));
		PQ_print_key (m, "", &m->heap [entry], "\n");
	}
}

#endif

#ifdef PriQue_DEBUG

void PQ_AssertConsistency (
		PQ_state * const m,
		int const heap_check,
		int const map_check)
{
	PQ_Index index, entry;
	PQ_Index * map = NULL;

	int const param_check = map_check;
	int const defn_check = map_check;
	int const alloc_check = map_check;

#ifdef PriQue_STATS
	++ m->count.check;
#endif

	if (param_check)
	{
		DebugAssert (m->capacity > 0  &&  m->nodes > 0);
		DebugAssert (m->nodes >= m->capacity);
		DebugAssert (m->nodes <= m->capacity + 1);
		DebugAssert (m->zero_sibling > 0);
		DebugAssert ((m->zero_sibling & (m->zero_sibling - 1)) == 0);

		DebugAssert (m->zero_sibling >= (m->capacity >> 1));
		DebugAssert (m->heap != NULL);
	}

	if (defn_check)
	{
		DebugAssert (PQ_early_sentinel (0) < PQ_late_sentinel (0));
		for (index = 0;  ++ index < m->nodes;  )
		{
			DebugAssert (PQ_early_sentinel (index) <
					PQ_late_sentinel (index));
			DebugAssert (PQ_early_sentinel (index-1) <
					PQ_early_sentinel (index));
			DebugAssert (PQ_late_sentinel (index-1) >
					PQ_late_sentinel (index));
		}
	}

	if (alloc_check)
	{
		void * address;

		DebugAssert (m->heap != NULL);
		address = (m->heap + m->nodes);

		if (m->data != NULL)
		{
			DebugAssert (m->data == address);
			address = (m->data + (m->capacity + 1));
		}

		DebugAssert ((size_t) address - (size_t) m->heap ==
				PQ_RequiredSpace (m->capacity, m->data != NULL));
	}

	if (map_check)
	{
#ifdef PriQue_STATS
		++ m->count.map_check;
#endif

		if (0)
		{
			printf ("=== mapping information ===\n");
			printf ("zero entry %d, zero sibling %d\n",
					(int) (m->zero_entrynode), (int) (m->zero_sibling));
			printf ("  %5s %5s -- %s\n",
					"index", "norm", "entry point + parents ...");
			for (index = 0;  index < m->nodes;  ++ index)
			{
				PQ_Index  up;

				printf ("  %5d --", (int) (index));
				for (up = PQ_leaf (m, index);
						up != PQ_root ();  PQ_parent (& up))
					printf (" %d", (int) (up));
				printf (" %d\n", (int) (PQ_root ()));
			}
		}

		for (index = 0;  index < m->nodes;  ++ index)
		{
			entry = PQ_leaf (m, index);
			DebugAssert (entry > 0);
			DebugAssert (entry >= m->nodes / 2);
			DebugAssert (entry < m->nodes);
		}

		map = (PQ_Index *) malloc (m->nodes * sizeof (PQ_Index));

		for (index = 0;  index < m->nodes;  ++ index)
			map [index] = 0;
		for (index = 0;  index < m->nodes;  ++ index)
			++ map [PQ_leaf (m, index)];

		if (0)
			for (index = 0;  index < m->nodes;  ++ index)
				printf ("  index = %3d, count = %3d\n",
						(int) (index), (int) (map [index]));
		for (index = 0;  index < m->nodes;  ++ index)
			DebugAssert (map [index] <= 2);
		{
			PQ_Index up = 1;
			PQ_parent (& up);
			DebugAssert (up == PQ_root ());
			map [PQ_root ()] = 2;
		}
		for (index = 1;  ++ index < m->nodes;  )
		{
			PQ_Index up = index;
			PQ_parent (& up);
			DebugAssert (up < index);
			DebugAssert (up > PQ_root ());
			++ map [up];
		}

		if (0)
			for (index = 0;  index < m->nodes;  ++ index)
				tagprintf ("  index = %3d, count = %3d\n",
						(int) (index), (int) (map [index]));
		for (index = 0;  index < m->nodes;  ++ index)
		{
			DebugAssert (map [index] == 2);
		}

		free (map);
		map = NULL;
	}

	if (heap_check)
	{
#ifdef PriQue_STATS
		++ m->count.heap_check;
#endif

		for (index = m->nodes;  index -- > 0;  )
			for (entry = PQ_leaf (m, index);
					m->heap [entry].index != index;
					PQ_parent (& entry))
			{
				DebugAssert (entry != PQ_root ());
			}

		map = (PQ_Index *) malloc (m->nodes * sizeof (PQ_Index));
		for (entry = m->nodes;  entry -- > 0;  )
			map [entry] = PQ_badIndex ();

		for (entry = m->nodes;  entry -- > 0;  )
		{
			index = m->heap [entry].index;

			DebugAssert (index < m->nodes);

			if (PQ_is_sentinel (m, m->heap [entry].normkey))
			{
				DebugAssert ( m->heap [entry].normkey ==
							PQ_early_sentinel (index)  ||
						m->heap [entry].normkey ==
							PQ_late_sentinel (index) );
			}

			else
			{
				if (m->runs > 0)
				{
					DebugAssert (m->heap [entry].normkey >=
							m->prevkey);
					DebugAssert (m->defer ?
							(m->heap [entry].normkey <
								m->nextrun + m->run_incr) :
							(m->heap [entry].normkey <=
								m->prevkey + m->run_incr));
				}

				else
				{
					DebugAssert (m->heap [entry].normkey >=
								m->nextrun - m->run_incr);
					DebugAssert (m->heap [entry].normkey < m->nextrun);
				}
			}

			DebugAssert (map [index] == PQ_badIndex ());

			map [index] = entry;
		}

		for (entry = m->nodes;  entry -- > 0;  )
		{
			DebugAssert (map [entry] != PQ_badIndex ());
		}
		free (map);
		map = NULL;

		for (index = m->nodes;  index -- > 0;  )
		{
			PQ_Index mask;
			 PQ_Entry * candidate;
			entry = PQ_leaf (m, index);
			mask = PQ_leaf_mask (m, entry);
			if (0)
				tagprintf ("index %ld, entry %ld, mask %ld\n",
						(long) (index), (long) (entry), (long) (mask));

			for (  ;  m->heap [entry].index != index;
					PQ_parent (& entry), PQ_parent_mask (& mask))
			{
				;
			}

			if (entry == PQ_root ())
				continue;

			candidate = & m->heap [entry];

			while (PQ_parent (& entry), entry != PQ_root ())
				if (PQ_siblings (index, m->heap [entry].index, mask))
					break;

			DebugAssert ( ! (PQ_compare (m, candidate, & m->heap [entry]) < 0));
		}
	}
}

#endif

#ifdef PriQue_STATS

static size_t aligned (void const * const ptr)
{
	size_t const a = (size_t) ptr;
	size_t const b = a & (a - 1);
	size_t const c = (a ^ b);
	return c;
}

void PQ_sizes (PQ_state const * const m)
{
	long bytes = sizeof (PQ_state);
	printf ("sizes of priority queue '%s':", (char *) m->name);

	printf (" %ld+%ld",
			(long) (sizeof (*m) - sizeof (m->count)),
			(long) (sizeof (m->count)));

	printf (" + %ld*%ld",
			(long) (m->nodes), (long) (sizeof (PQ_Entry)));
	bytes += m->nodes * sizeof (PQ_Entry);

	if (m->data != NULL)
	{
		printf (" + data %ld*%d",
				(long) (m->capacity+1), (int) (sizeof (PQ_Data)));
		bytes += (m->capacity+1) * sizeof (PQ_Data);
	}

	printf ("\n    = %ld or %.2f bytes/entry\n",
			(long) (bytes), (float) (bytes) / (float) (m->capacity));

	printf ("  heap aligned to %ld, ", (long) (aligned (m)));
	if (m->data != NULL)
		printf ("data to %ld, ", (long) (aligned (m->data)));
	printf ("pointers to %ld bytes\n", (long) (aligned (m->heap)));
}

static int IsPowerOf2 (size_t const x)
{
	return x > 0 && (x & (x - 1)) == 0;
}

void PQ_statistics (PQ_state const * const m, char const header [])
{
	unsigned long search_compare;
	unsigned long const pmnk_compare =
			m->count.compare - m->count.key_compare;
	double const items = m->count.push + m->count.replace;

	int log2 = 0;
	{
		PQ_Index n = m->nodes;
		for (log2 = 0;  n > 1;  ++ log2, n /= 2) ;
	}
	search_compare = m->count.compare
#ifdef PriQue_DEBUG
			- m->count.heap_check * (m->nodes - 1)
#endif
			- m->count.easy_pass * log2;

	printf ("statistics for heap named \"%s\" %s:\n",
			m->name, header);
	printf ("  %lu push, %lu pop, %lu top operations\n",
			(unsigned long) (m->count.push),
			(unsigned long) (m->count.pop),
			(unsigned long) (m->count.top));
	printf ("  %lu replace, %lu erase, %lu empty operations\n",
			(unsigned long) (m->count.replace),
			(unsigned long) (m->count.erase),
			(unsigned long) (m->count.empty));
	printf ("  %lu=%lu+%lu easy+search passes\n",
			(unsigned long) (m->count.easy_pass + m->count.search_pass),
			(unsigned long) (m->count.easy_pass),
			(unsigned long) (m->count.search_pass));

	printf ("  total: %G=%G+%G comparisons, %G moves\n",
			(float) (m->count.compare), (float) (pmnk_compare),
			(float) (m->count.key_compare), (float) (m->count.move));
	if (items > 0)
		printf ("  per item: %.3f=%.3f+%.3f comparisons, %.3f moves\n",
				(float) (m->count.compare) / (float) (items),
				(float) (pmnk_compare) / (float) (items),
				(float) (m->count.key_compare) / (float) (items),
				(float) (m->count.move) / (float) (items));
	if (m->count.search_pass > 0  &&  IsPowerOf2 (m->nodes))
		printf ("  per search pass: %.3f comparisons\n",
				(float) (search_compare) / (float) (m->count.search_pass));

#ifdef PriQue_DEBUG
	printf ("  %lu %s incl %lu map & %lu heap (%G comparisons)\n",
			(unsigned long) (m->count.check), "consistency checks",
			(unsigned long) (m->count.map_check),
			(unsigned long) (m->count.heap_check),
			(float) (m->count.heap_check * (m->nodes - 1)));
#endif
}

#endif


