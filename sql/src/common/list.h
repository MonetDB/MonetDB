#ifndef LIST_H
#define LIST_H

#include "sym.h"

typedef struct node {
	struct node *next;
	symdata data;
} node;

typedef struct list {
	symtype type;
	node *h;
	node *t;
	int cnt;
} list;

typedef int (*traverse_func)(char *clientdata, int seqnr, char *data);
typedef void *(*map_func)(char *clientdata, int seqnr, char *data);

extern list *list_create();
extern void list_destroy(list *l);
extern int list_length(list *l);

extern list *list_append_string(list *l, char *data);
extern list *list_append_list(list *l, list *data);
extern list *list_append_int(list *l, int data);
extern list *list_append_atom(list *l, struct atom *data);
extern list *list_append_statement(list *l, struct statement *data);
extern list *list_append_column(list *l, struct column *data);
extern list *list_append_table(list *l, struct table *data);

extern list *list_prepend_statement(list *l, struct statement *data);

extern node *list_remove(list *l, node *n );

extern list *list_merge(list *l, list *data);
extern int list_traverse(list *l, traverse_func f, char *clientdata );
extern list *list_map(list *l, map_func f, char *clientdata );

#endif /* LIST_H */
