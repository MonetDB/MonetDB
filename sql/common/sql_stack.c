/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_stack.h"

sql_stack *
sql_stack_new(sql_allocator *sa, int size)
{
	sql_stack *s = SA_NEW(sa, sql_stack);
	if (s == NULL)
		return NULL;

	s -> sa = sa;
	s -> size = size;
	s -> top = 0;
	s -> values = SA_NEW_ARRAY(sa, void*, size);
	if (s->values == NULL) {
		_DELETE(s);
		return NULL;
	}
	s -> values[s->top++] = NULL; 
	return s;
}

void 
sql_stack_push(sql_stack *s, void *v)
{
	if (s->top >= s->size) {
		size_t osz = s->size;
		s->size *= 2;
		s->values = SA_RENEW_ARRAY(s->sa, void*, s->values, s->size, osz);
		if (s->values == NULL)
			return;
	}
	s->values[s->top++] = v;
}

void *
sql_stack_pop(sql_stack *s)
{
	return s->values[--s->top];
}
