#ifndef LIST_H
#define LIST_H

#include "sym.h"

/*
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
*/

typedef struct node {
	struct node *next;
	void *data;
} node;

typedef void (*fdestroy)(void *);

typedef struct list {
	fdestroy destroy;
	node *h;
	node *t;
	int cnt;
} list;


typedef int (*traverse_func) (char *clientdata, int seqnr, void *data);
typedef void *(*map_func) (char *clientdata, int seqnr, void *data);

extern list *list_create(fdestroy destroy);

extern void list_destroy(list * l);
extern int list_length(list * l);

extern list *list_append(list * l, void *data);
extern list *list_prepend(list * l, void *data);

extern node *list_remove_node(list * l, node * n);
extern void list_remove_data(list * l, void *data);
extern void list_move_data(list * l, list * d, void *data);

extern list *list_merge(list * l, list * data);
extern int list_traverse(list * l, traverse_func f, char *clientdata);
extern list *list_map(list * l, map_func f, char *clientdata);

/* the compare function gets one element from the list and a key from the
 * as input from the find function 
 * Returns 0 if data and key are equal 
 * */
typedef int (*fcmp)(void *data,void *key); 
extern node *list_find(list * l, void *key, fcmp cmp ); 

#endif				/* LIST_H */
