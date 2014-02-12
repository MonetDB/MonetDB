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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include <sql_stack.h>

sql_stack *
sql_stack_new(sql_allocator *sa, int size)
{
	sql_stack *s = SA_NEW(sa, sql_stack);

	s -> sa = sa;
	s -> size = size;
	s -> top = 0;
	s -> values = SA_NEW_ARRAY(sa, void*, size); 
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
	}
	s->values[s->top++] = v;
}

void *
sql_stack_pop(sql_stack *s)
{
	return s->values[--s->top];
}
