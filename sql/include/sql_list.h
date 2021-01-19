/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef LIST_H
#define LIST_H

#include "sql_mem.h"
#include "sql_hash.h"

typedef struct node {
	struct node *next;
	void *data;
} node;

typedef void (*fdestroy) (void *gdata, void *ndata); /* gdata is passed to the list_destroy2 function */

typedef struct list {
	sql_allocator *sa;
	sql_hash *ht;
	MT_Lock ht_lock;	/* latch protecting ht */
	fdestroy destroy;
	node *h;
	node *t;
	int cnt;
	int expected_cnt;
} list;

typedef int (*traverse_func) (void *clientdata, int seqnr, void *data);
typedef int (*prop_check_func) (void *data);

extern list *list_create(fdestroy destroy);
sql_export list *sa_list(sql_allocator *sa);
extern list *list_new(sql_allocator *sa, fdestroy destroy);

extern list *sa_list_append( sql_allocator *sa, list *l, void *data);

extern void list_destroy(list *l);
extern void list_destroy2(list *l, void *data);
sql_export int list_length(list *l);
extern int list_empty(list *l);

sql_export list *list_append(list *l, void *data);
extern list *list_append_before(list *l, node *n, void *data);
extern list *list_prepend(list *l, void *data);

extern node *list_remove_node(list *l, void *gdata, node *n);
extern void list_remove_data(list *l, void *gdata, void *data);
extern void list_remove_list(list *l, void *gdata, list *data);
extern void list_move_data(list *l, list *d, void *data);


extern int list_traverse(list *l, traverse_func f, void *clientdata);

extern int list_check_prop_all(list *l, prop_check_func f);

/* the compare function gets one element from the list and a key from the
 * as input from the find function
 * Returns 0 if data and key are equal
 * */
typedef int (*fcmp) (void *data, void *key);
typedef void *(*fcmpvalidate) (void *v1, void *v2, void *extra, int *cmp);
typedef void *(*fvalidate) (void *v1, void *v2);
typedef int (*fcmp2) (void *data, void *v1, void *v2);
typedef void *(*fdup) (void *data);
typedef void *(*freduce) (void *v1, void *v2);
typedef void *(*freduce2) (sql_allocator *sa, void *v1, void *v2);
typedef void *(*fmap) (void *data, void *clientdata);

extern void *list_traverse_with_validate(list *l, void *data, fvalidate cmp);
extern void *list_append_with_validate(list *l, void *data, fvalidate cmp);
extern void *list_append_sorted(list *l, void *data, void *extra, fcmpvalidate cmp);
extern node *list_find(list *l, void *key, fcmp cmp);
extern int  list_position(list *l, void *val);
sql_export void *list_fetch(list *l, int pos);
extern list *list_select(list *l, void *key, fcmp cmp, fdup dup);
extern list *list_order(list *l, fcmp cmp, fdup dup);
extern list *list_distinct(list *l, fcmp cmp, fdup dup);
extern void *list_reduce(list *l, freduce red, fdup dup);
extern void *list_reduce2(list *l, freduce2 red, sql_allocator *sa);
extern list *list_map(list *l, void *data, fmap f);
extern int list_cmp(list *l1, list *l2, fcmp cmp);
/* cmp the lists in link order */
extern int list_match(list *l1, list *l2, fcmp cmp);
/* match the lists (in any order) */
extern list *list_sort(list *l, fkeyvalue key, fdup dup);
/* The sort function sorts the list using the key function, which
 * translates the list item values into integer keyvalues. */
/* sometimes more complex functions are needed to compute a key, then
 * we can pass the keys via an array, to keysort */
extern list *list_keysort(list *l, int *key, fdup dup);

extern list *list_dup(list *l, fdup dup);
extern list *list_merge(list *l, list *data, fdup dup);
extern list *list_merge_destroy(list *l, list *data, fdup dup);

extern list *list_flaten(list *l);

extern void list_hash_delete(list *l, void *data, fcmp cmp);
extern void* list_hash_add(list *l, void *data, fcmp cmp);
extern void list_hash_clear(list *l);

#endif /* LIST_H */
