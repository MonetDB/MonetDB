
#include "mem.h"
#include "list.h"

#include "statement.h"
#include "symbol.h"

static node *node_create(void *data)
{
	node *n = NEW(node);
	n->next = NULL;
	n->data = data;
	return n;
}

list *list_create(fdestroy destroy)
{
	list *l = NEW(list);
	l->destroy = destroy;
	l->h = l->t = NULL;
	l->cnt = 0;
	return l;
}

void node_destroy(list *l, node * n)
{
	if (n->data && l->destroy) 
		l->destroy(n->data);
	_DELETE(n);
}

void list_destroy(list * l)
{
	if (l) {
		node *n = l->h;
		while (n) {
			node *t = n;
			n = n->next;
			node_destroy(l, t);
		}
		_DELETE(l);
	}
}

int list_length(list * l)
{
	return l->cnt;
}

list *list_append(list * l, void * data )
{
	node *n = node_create(data);
	if (l->cnt) {
		l->t->next = n;
	} else {
		l->h = n;
	}
	l->t = n;
	l->cnt++;
	return l;
}

list *list_prepend(list * l, void * data)
{
	node *n = node_create(data);
	if (!l->cnt) {
		l->t = n;
	}
	n->next = l->h;
	l->h = n;
	l->cnt++;
	return l;
}

list *list_merge(list * l, list * data)
{
	if (data) {
		node *n = data->h;
		while (n) {
			list_append(l, n->data);
			n = n->next;
		}
	}
	return l;
}


node *list_remove_node(list * l, node * n)
{
	node *p = l->h;
	if (p != n)
		while (p && p->next != n)
			p = p->next;
	if (p == n) {
		l->h = n->next;
		p = NULL;
	} else {
		p->next = n->next;
	}
	if (n == l->t)
		l->t = p;
	node_destroy(l, n);
	l->cnt--;
	return p;
}

void list_remove_data(list * s, void *data)
{
	node *n;
	/* maybe use compare func */
	for (n = s->h; n; n = n->next) {
		if (n->data == data) {
			n->data = NULL;
			list_remove_node(s, n);
			break;
		}
	}
}

void list_move_data(list * s, list * d, void *data)
{
	node *n;
	for (n = s->h; n; n = n->next) {
		if (n->data == data) {
			n->data = NULL;
			list_remove_node(s, n);
			break;
		}
	}
	list_append(d, data);
}

int list_traverse(list * l, traverse_func f, char *clientdata)
{
	int res = 0, seqnr = 0;
	node *n = l->h;
	while (n && !res) {
		res = f(clientdata, seqnr++, n->data);
		n = n->next;
	}
	return res;
}

list *list_map(list * l, map_func f, char *clientdata)
{
	list *res = list_create(NULL);
	int seqnr = 0;

	node *n = l->h;
	while (n) {
		void *v = f(clientdata, seqnr++, n->data);
		list_append(res, v);
		n = n->next;
	}
	return res;
}

node *list_find(list * l, void *key, fcmp cmp)
{
	node *n = NULL;
	if (key){
       		for ( n	= l->h;	n; n = n->next){
			if (cmp(n->data,key) == 0){
				return n;
			}
		}
	}
	return NULL;
}

#ifdef TEST
#include <stdio.h>
#include <string.h>

void print_data(void *dummy, void *data)
{
	printf("%s ", (char*)data);
}

void destroy_data(void *dummy, void *data)
{
	_DELETE(data);
}

int main()
{
	list *l = list_create(NULL);

	printf("0 list_length %d\n", list_length(l));
	list_append_string(l, _strdup("niels"));
	printf("1 list_length %d\n", list_length(l));
	list_append_string(l, _strdup("nes"));
	printf("1 list_length %d\n", list_length(l));
	list_append_string(l, _strdup("lilian"));
	printf("1 list_length %d\n", list_length(l));
	list_append_string(l, _strdup("nes"));
	printf("1 list_length %d\n", list_length(l));
	list_append_string(l, _strdup("max"));
	printf("1 list_length %d\n", list_length(l));
	list_append_string(l, _strdup("nes"));
	printf("1 list_length %d\n", list_length(l));
	list_traverse(l, print_data, NULL);
	printf("\n");

	list_traverse(l, destroy_data, NULL);
	list_destroy(l);
}
#endif
