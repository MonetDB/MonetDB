
#include <mem.h>
#include "catalog.h"

#define OIDRANGE 100

void catalog_initoid( catalog *cat ){
	cat->nr = OIDRANGE;
	cat->nextid = cat->cc_oidrange( cat, cat->nr );
}

static
long catalog_getoid( catalog *cat ){
	if (cat->nr <= 0){
		/* obtain new range from server */
		cat->nr = OIDRANGE;
		cat->nextid = cat->cc_oidrange( cat, cat->nr );
	} 
	cat->nr--;
	return cat->nextid++;
}

static
schema *catalog_schema_create( catalog *cat, long id, char *name, char *auth){
	schema *s = NEW(schema);
	s->id = id;
	if (id == 0) s->id = catalog_getoid( cat );
	s->name = _strdup(name);
	s->auth = _strdup(auth);
	s->tables = list_create();
	return s;
}

static type *catalog_resolve_type( catalog *cat, char *name);

static
table *catalog_table_create( catalog *cat, long id, schema *s, char *name, 
		int temp, char *sql){
	table *t = NEW(table);
	t->id = id;
	if (id == 0) t->id = catalog_getoid( cat );
	t->name = _strdup(name); 
	t->schema = s;
	t->temp = temp; 
	t->columns = list_create();
	t->sql = NULL;
	if (sql) t->sql = _strdup(sql);

	list_append_string( s->tables, (char*) t );
	return t;
}

static
int column_destroy( char *clientdata, int colnr, char *col ){
	column *c = (column*)col;

	_DELETE(c->name);
	_DELETE(c);
	return 0;
}

static
int table_destroy( char *clientdata, int tbnr, char *tbl ){
	table *t = (table*)tbl;

	list_traverse(t->columns, &column_destroy, NULL);
	list_destroy(t->columns); 
	_DELETE(t->name);
	_DELETE(t);
	return 0;
}

static
void schema_destroy( schema *s ){
	list_traverse(s->tables, &table_destroy, NULL);
	list_destroy(s->tables); 
	_DELETE(s->name);
	_DELETE(s->auth);
	_DELETE(s);
}

static
void catalog_schema_destroy( catalog *cat ){
	if (cat->cur_schema) 
		schema_destroy( cat->cur_schema );
	cat->cur_schema = NULL;
}

static
void catalog_table_destroy( catalog *cat, schema *s, char *name ){
	node *n = s->tables->h;
	while(n){
		table *t = (table*)n->data.sval;
		if (strcmp(t->name, name) == 0){
			list_remove(s->tables, n);
			break;
		}
		n = n->next;
	}
}

static
column *catalog_column_create( catalog *cat, long id, table *t, char *colname, char *sqltype, char *def, int null_check, int colnr ){
	node *n = NEW(node), *cur, *p = NULL;
	column *c = NEW(column);
	type *tpe = catalog_resolve_type( cat, sqltype );

	assert(c && t);

	c->id = id;
	if (id == 0) c->id = catalog_getoid( cat );
	c->name = _strdup(colname);
	c->tpe = tpe;
	c->table = t;
	c->default_value = def;
	c->null = null_check;
	c->colnr = colnr;
	c->s = NULL; /* not a view */

	n->data.cval = c;
	n->next = NULL;
	/*
	list_append_string(t->columns, (char*)c );
	*/

	for(cur=t->columns->h; cur && c->colnr > cur->data.cval->colnr; 
	    cur=cur->next){
		p = cur;
	}

	if (p){
		n->next = p->next;
		p->next = n;
		if (n->next == NULL) t->columns->t = n;
	} else if (cur){
		t->columns->h = n;
		n->next = cur;
	} else {
		t->columns->h = n;
		t->columns->t = n;
	}
	t->columns->cnt++;
	return c;
}

static
type *catalog_type_create( catalog *cat, char *sqlname, char *name, char *cast, int nr ){
	type *t = NEW(type);

	t->sqlname = _strdup(sqlname);
	t->name = _strdup(name);
	t->cast = NULL;
	if (strlen(cast) > 0)
		t->cast = catalog_resolve_type( cat, cast );
	t->nr = nr;
	list_append_string(cat->types, (char*)t );
	return t;
}

static
void type_destroy( type *t ){
	_DELETE( t->sqlname );
	_DELETE( t->name );
	_DELETE(t);
}

static
aggr *catalog_aggr_create( catalog *cat, char *name, int nr ){
	aggr *t = NEW(aggr);

	t->name = _strdup(name);
	t->nr = nr;
	list_append_string(cat->aggrs, (char*)t );
	return t;
}

static
void aggr_destroy( aggr *t ){
	_DELETE( t->name );
	_DELETE(t);
}

static
func *catalog_func_create( catalog *cat, char *name, int nr ){
	func *t = NEW(func);

	t->name = _strdup(name);
	t->nr = nr;
	list_append_string(cat->funcs, (char*)t );
	return t;
}

static
void func_destroy( func *t ){
	_DELETE( t->name );
	_DELETE(t);
}

static
schema *catalog_resolve_schema( catalog *cat, char *name ){
	node *n = cat->schemas->h;
	while(n){
		schema *t = (schema*)n->data.sval;
		if (strcmp(t->name, name) == 0)
			return t;
		n = n->next;
	}
	return NULL;
}

static
table *catalog_resolve_table( catalog *cat, schema *s, char *name ){
	node *n = s->tables->h;
	while(n){
		table *t = (table*)n->data.sval;
		if (strcmp(t->name, name) == 0)
			return t;
		n = n->next;
	}
	return NULL;
}

static
column *catalog_resolve_column( catalog *cat, table *t, char *colname){
	node *n = t->columns->h;
	while(n){
		column *c = (column*)n->data.sval;
		if (strcmp(c->name, colname) == 0)
			return c;
		n = n->next;
	}
	return NULL;
}

static
type *catalog_resolve_type( catalog *cat, char *sqlname){
	node *n = cat->types->h;
	while(n){
		type *t = (type*)n->data.sval;
		if (strcmp(t->sqlname, sqlname) == 0)
			return t;
		n = n->next;
	}
	return NULL;
}

static
aggr *catalog_resolve_aggr( catalog *cat, char *name){
	node *n = cat->aggrs->h;
	while(n){
		aggr *t = (aggr*)n->data.sval;
		if (strcmp(t->name, name) == 0)
			return t;
		n = n->next;
	}
	return NULL;
}

static
func *catalog_resolve_func( catalog *cat, char *name){
	node *n = cat->funcs->h;
	while(n){
		func *t = (func*)n->data.sval;
		if (strcmp(t->name, name) == 0)
			return t;
		n = n->next;
	}
	return NULL;
}

catalog *default_catalog_create(){
	catalog *c = NEW(catalog);

	c->cc = NULL;
	c->cur_schema = NULL;

	c->bind_schema = catalog_resolve_schema;
	c->create_schema = catalog_schema_create;
	c->destroy_schema = catalog_schema_destroy;

	c->bind_table = catalog_resolve_table;
	c->create_table = catalog_table_create;
	c->destroy_table = catalog_table_destroy;

	c->bind_column = catalog_resolve_column;
	c->create_column = catalog_column_create;

	c->bind_type = catalog_resolve_type;
	c->create_type = catalog_type_create;

	c->bind_aggr = catalog_resolve_aggr;
	c->create_aggr = catalog_aggr_create;

	c->bind_func = catalog_resolve_func;
	c->create_func = catalog_func_create;

	return c;
}

void catalog_destroy( catalog *cat ){
	node *n = cat->schemas->h;

	if (cat->cc && cat->cc_destroy)
		cat->cc_destroy( cat );
	while(n){
		schema *s = n->data.schema;
		schema_destroy(s);
		n = n->next;
	}
	list_destroy(cat->schemas);

	n = cat->aggrs->h;
	while(n){
		aggr *t = n->data.aggrval;
		aggr_destroy(t);
		n = n->next;
	}
	list_destroy(cat->aggrs);

	n = cat->funcs->h;
	while(n){
		func *t = n->data.funcval;
		func_destroy(t);
		n = n->next;
	}
	list_destroy(cat->funcs);

	n = cat->types->h;
	while(n){
		type *t = n->data.typeval;
		type_destroy(t);
		n = n->next;
	}
	list_destroy(cat->types);

	_DELETE(cat);
}
