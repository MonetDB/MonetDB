#ifndef LIST_H
#define LIST_H

#include "sql_mem.h"

typedef struct node {
	struct node *next;
	void *data;
} node;

typedef void (*fdestroy) (void *);

typedef struct list {
	fdestroy destroy;
	node *h;
	node *t;
	int cnt;
} list;

typedef int (*traverse_func) (char *clientdata, int seqnr, void *data);

sqlcommon_export list *list_create(fdestroy destroy);

sqlcommon_export void list_destroy(list *l);
sqlcommon_export int list_length(list *l);

sqlcommon_export list *list_append(list *l, void *data);
sqlcommon_export list *list_prepend(list *l, void *data);

sqlcommon_export node *list_remove_node(list *l, node *n);
sqlcommon_export void list_remove_data(list *l, void *data);
sqlcommon_export void list_move_data(list *l, list *d, void *data);


sqlcommon_export int list_traverse(list *l, traverse_func f, char *clientdata);

/* the compare function gets one element from the list and a key from the
 * as input from the find function 
 * Returns 0 if data and key are equal 
 * */
typedef int (*fcmp) (void *data, void *key);
typedef void *(*fdup) (void *data);
typedef void *(*freduce) (void *v1, void *v2);
typedef void *(*fmap) (void *data, void *clientdata);

sqlcommon_export node *list_find(list *l, void *key, fcmp cmp);
sqlcommon_export list *list_select(list *l, void *key, fcmp cmp, fdup dup);
sqlcommon_export list *list_distinct(list *l, fcmp cmp, fdup dup);
sqlcommon_export void *list_reduce(list *l, freduce red, fdup dup);
sqlcommon_export list *list_map(list *l, void *data, fmap f);
sqlcommon_export int list_cmp(list *l1, list *l2, fcmp cmp);

sqlcommon_export list *list_dup(list *l, fdup dup);
sqlcommon_export list *list_merge(list *l, list *data, fdup dup);

#endif /* LIST_H */
