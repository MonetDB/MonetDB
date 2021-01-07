/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_stack.h"

sql_stack *
sql_stack_new(sql_allocator *sa, int size)
{
	assert(sa);
	sql_stack *s = SA_NEW(sa, sql_stack);
	if (s == NULL)
		return NULL;

	*s = (sql_stack) {
		.sa = sa,
		.size = size,
		.values = SA_NEW_ARRAY(sa, void*, size),
	};
	if (s->values == NULL) {
		//_DELETE(s);
		return NULL;
	}
	return s;
}

void
sql_stack_push(sql_stack *s, void *v)
{
	if (s->top >= s->size) {
		void **nvalues;
		int osz = s->size;

		s->size *= 2;
		nvalues = SA_RENEW_ARRAY(s->sa, void*, s->values, s->size, osz);
		if (nvalues == NULL) {
			s->size = osz;
			return;
		}
		s->values = nvalues;
	}
	s->values[s->top++] = v;
}

void *
sql_stack_pop(sql_stack *s)
{
	if (s->top == 0)
		return NULL;
	return s->values[--s->top];
}

void *
sql_stack_peek(sql_stack *s, int p)
{
	if (p>=s->top)
		return NULL;
	return s->values[(s->top-1)-p];
}

void *
sql_stack_fetch(sql_stack *s, int p)
{
	if (p>=s->top)
		return NULL;
	return s->values[p];
}

int
sql_stack_top(sql_stack *s)
{
	return s->top;
}

int
sql_stack_empty(sql_stack *s)
{
	return (s->top == 0);
}
