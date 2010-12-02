
#include "sql_config.h"
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
