
#include "mem.h"
#include "list.h"

#include "statement.h"
#include "symbol.h"

static node *node_create(){
	node *n = NEW(node); 
	n->next = NULL;
	n->data.sval = NULL;
	return n;
}

static node *node_create_string( char *data ){
	node *n = node_create();
	n->data.sval = data;
	return n;
}
static node *node_create_list( list *data ){
	node *n = node_create();
	n->data.lval = data;
	return n;
}
static node *node_create_int( int data ){
	node *n = node_create();
	n->data.ival = data;
	return n;
}
static node *node_create_atom( atom *data ){
	node *n = node_create();
	n->data.aval = data;
	return n;
}
static node *node_create_statement( statement *data ){
	node *n = node_create();
	if (data ){
		n->data.stval = data; data->refcnt++;
	}
	return n;
}
static node *node_create_column( column *data ){
	node *n = node_create();
	n->data.cval = data;
	return n;
}
static node *node_create_table( table *data ){
	node *n = node_create();
	n->data.tval = data;
	return n;
}

list *list_create(){
	list *l = NEW(list); 
	l->h = l->t = NULL;
	l->cnt = 0;
	l->type = 0;
	return l;
}

void node_destroy( node *n, int type ){
	if (n->data.sval){
		if (type == type_statement)
			statement_destroy( n->data.stval );
		else if (type == type_atom)
			atom_destroy( n->data.aval );
	}
	_DELETE(n);
}

void list_destroy(list *l){
	if (l){
	    node *n = l->h;
	    while(n){
		node *t = n;
		n = n->next;
		node_destroy(t,l->type);
	    }
	    _DELETE(l);
	}
}

int list_length(list *l){
	return l->cnt;
}

list *list_append_default(list *l, node *n){
	if (l->cnt){
		l->t->next = n;
	} else {
		l->h = n;
	}
	l->t = n;
	l->cnt++;
	return l;
}
list *list_append_string(list *l, char *data){
	node *n = node_create_string(data);
	l->type = type_string;
	return list_append_default(l,n);
}
list *list_append_list(list *l, list *data){
	node *n = node_create_list(data);
	l->type = type_list;
	return list_append_default(l,n);
}
list *list_append_int(list *l, int data){
	node *n = node_create_int(data);
	l->type = type_int;
	return list_append_default(l,n);
}
list *list_append_atom(list *l, atom *data){
	node *n = node_create_atom(data);
	l->type = type_atom;
	return list_append_default(l,n);
}
list *list_append_statement(list *l, statement *data){
	node *n = node_create_statement(data);
	l->type = type_statement;
	return list_append_default(l,n);
}
list *list_append_column(list *l, column *data){
	node *n = node_create_column(data);
	l->type = type_column;
	return list_append_default(l,n);
}
list *list_append_table(list *l, table *data){
	node *n = node_create_table(data);
	l->type = type_table;
	return list_append_default(l,n);
}

list *list_merge(list *l, list *data){
	if (data){
		node *n = data->h;
	 	switch(l->type){
		case type_string:
			while(n){
				list_append_string(l, n->data.sval); 
				n = n->next;
			} break;
		case type_list:
			while(n){
				list_append_list(l, n->data.lval); 
				n = n->next;
			} break;
		case type_int:
			while(n){
				list_append_int(l, n->data.ival); 
				n = n->next;
			} break;
		case type_atom:
			while(n){
				list_append_atom(l, n->data.aval); 
				n = n->next;
			} break;
		case type_statement:
			while(n){
				list_append_statement(l, n->data.stval); 
				n = n->next;
			} break;
		case type_column:
			while(n){
				list_append_column(l, n->data.cval); 
				n = n->next;
			} break;
		case type_table:
			while(n){
				list_append_table(l, n->data.tval); 
				n = n->next;
			} break;
		case type_type:
		case type_func:
		case type_aggr:
		case type_symbol:
			break;
		}
	}
	return l;
}

node *list_remove( list *l, node *n){
	node *p = l->h;
	if (p != n) while(p && p->next != n) p = p->next;
	if (p == n){
		l->h = n->next;
		p = NULL;
	} else {
		p->next = n->next;
	}
	if (n == l->t) l->t = p;
	node_destroy(n,l->type);
	l->cnt--;
	return p;
}

int list_traverse(list *l, traverse_func f, char *clientdata ){
	int res = 0, seqnr = 0;
	node *n = l->h;
	while(n && !res){
		res = f(clientdata, seqnr++, n->data.sval);
		n = n->next;
	}
	return res;
}

list *list_map(list *l, map_func f, char *clientdata ){
	list *res = list_create();
	int seqnr = 0;

	node *n = l->h;
	while(n){
		void *v = f(clientdata, seqnr++, n->data.sval);
		list_append_string(res, (char*)v);
		n = n->next;
	}
	return res;
}
#ifdef TEST
#include <stdio.h>
#include <string.h>

void print_data( char *dummy, char *data){
	printf("%s ", data);
}	
void destroy_data( char *dummy, char *data){
	_DELETE(data);
}	
int main(){
	list *l= list_create();

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
	list_traverse( l, print_data, NULL ); printf("\n");

	list_traverse( l, destroy_data, NULL ); 
	list_destroy(l);
}
#endif
