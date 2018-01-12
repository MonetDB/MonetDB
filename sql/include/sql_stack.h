/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef SQL_STACK_H
#define SQL_STACK_H

/* sql_stack implementation 
 * used by mvc structure for variable stack	
 * 			     trigger stack
 * 			     Multi statement stack (intermediate results)
 * 			     ....
 * 			     stmt generation
 *			     stmt dependency (close to stmt generation)
 */

#include "sql_mem.h"

typedef struct sql_stack {
	sql_allocator *sa;
	int size;
	int top;
	void **values;
} sql_stack;

extern sql_stack *sql_stack_new(sql_allocator *sa, int size);
extern void sql_stack_push(sql_stack *s, void *v);
extern void *sql_stack_pop(sql_stack *s);

#endif /* SQL_STACK_H */
