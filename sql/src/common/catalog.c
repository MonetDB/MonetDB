
#include <mem.h>
#include <string.h>
#include "catalog.h"
#include "statement.h"

#define OIDRANGE 100

static int cat_debug = 0;

static void key_destroy(key *k)
{
	if (k->columns)
		list_destroy(k->columns);
	if (k->name)
		_DELETE(k->name);
	_DELETE(k);
}

static void column_destroy(column *c)
{
	assert(c && c->tpe);
	if (c->name)
		_DELETE(c->name);
	if (c->s)
		stmt_destroy(c->s);
	_DELETE(c->tpe);
	_DELETE(c);
}

static void table_destroy(table *t)
{
	list_destroy(t->columns);
	if (t->name)
		_DELETE(t->name);
	if (t->sql)
		_DELETE(t->sql);
	if (t->s)
		stmt_destroy(t->s);
	if (t->keys){
		list_destroy(t->keys);
	}
	_DELETE(t);
}

void schema_destroy(schema * s)
{
	list_destroy(s->tables);
	_DELETE(s->name);
	_DELETE(s->auth);
	_DELETE(s);
}

schema *cat_create_schema(catalog * cat, long id, char *name, char *auth)
{
	schema *s = NEW(schema);
	s->id = id;
	s->name = _strdup(name);
	s->auth = _strdup(auth);
	s->tables = list_create((fdestroy)&table_destroy);
	if (cat_debug)
		printf("cat_create_schema, %ld %s %s\n", id, name, auth);
	return s;
}

table *cat_create_table(catalog * cat, long id, schema * s, char *name,
			int type, char *sql)
{
	table *t = NEW(table);
	t->id = id;
	t->name = (name) ? _strdup(name) : NULL;
	t->schema = s;
	t->type = type;
	t->columns = list_create((fdestroy)&column_destroy);
	t->sql = NULL;
	t->pkey = NULL;
	t->keys = NULL;
	t->s = NULL;
	if (sql)
		t->sql = _strdup(sql);

	list_append(s->tables, t);
	if (cat_debug)
		printf("cat_create_table, %ld %s %d %s\n", id, name, type, sql);
	return t;
}

key *cat_table_add_key(table *t, key_type kt, char *name, key *fk)
{
	key *k = NEW(key);
	k->id = 0;
	k->type = kt;
	k->t = t;
	k->name = (name) ? _strdup(name) : NULL;
	if (!t->keys)
		t->keys = list_create((fdestroy)&key_destroy);
	list_append(t->keys, k);
	if (kt == pkey)
		t->pkey = k;
	k->columns = list_create(NULL); 
	if (fk){
		k->rkey = fk;
	}
	return k;
}

key *cat_key_add_column( key *k, column *c ){
	list_append(k->columns, c);
	return k;
}

int uniqueKey(key *k){
	return (k->type == pkey || k->type == ukey);
}

key *cat_table_bind_ukey( table *t, list *colnames )
{
	node *cn;
	node *cur;
	key * res = NULL;
	int len = list_length(colnames);

	if (t->keys) for ( cur = t->keys->h; cur; cur = cur -> next ){ 
		node *cc;
		key *k = cur->data;
		if (uniqueKey(k) && list_length(k->columns) == len){
			res = k;
		   	for ( cc = k->columns->h, cn = colnames->h; 
				cc && cn; cc = cc -> next, cn = cn -> next ){
				column *c = cc->data;
				char *n = cn->data;
				if (strcmp(c->name, n) != 0){
					res = NULL;
					break;
				}
			}
			if (res) 
				break;
		}
	}
	return res;
}

key *cat_table_bind_sukey( table *t, char *cname)
{
	node *cur;

	if (t->keys) for ( cur = t->keys->h; cur; cur = cur -> next ){ 
		key *k = cur->data;
		if (uniqueKey(k) && list_length(k->columns) == 1){
			column *c = k->columns->h->data;
			if (strcmp(c->name, cname) == 0){
				return k;
			}
		}
	}
	return NULL;
}

void cat_drop_schema(schema * s)
{
	schema_destroy(s);
}

void cat_drop_table(catalog * cat, schema * s, char *name)
{
	node *n = s->tables->h;
	while (n) {
		table *t = n->data;
		if (t->name && strcmp(t->name, name) == 0) {
			list_remove_node(s->tables, n);
			break;
		}
		n = n->next;
	}
}

column *cat_create_column(catalog * cat, long id, table * t, char *colname,
			  sql_subtype *tpe, char *def, int null_check)
{
	column *c = NEW(column);

	assert(c && t);

	c->id = id;
	c->name = _strdup(colname);
	c->tpe = tpe;
	c->table = t;
	c->default_value = def;
	c->null = null_check;
	c->colnr = list_length(t->columns);
	c->s = NULL;
	list_append(t->columns, c );
	if (cat_debug)
		printf("cat_create_column, %ld %s %s %s %d\n", 
			id, colname, tpe->type->sqlname, def, null_check);
	return c;
}

schema *cat_bind_schema(catalog * cat, char *name)
{
	node *n = cat->schemas->h;
	while (n) {
		schema *t = n->data;
		if (strcmp(t->name, name) == 0)
			return t;
		n = n->next;
	}
	return NULL;
}

table *cat_bind_table(catalog * cat, schema * s, char *name)
{
	node *n = s->tables->h;
	while (n) {
		table *t = n->data;
		if (t->name && strcmp(t->name, name) == 0)
			return t;
		n = n->next;
	}
	return NULL;
}

column *cat_bind_column(catalog * cat, table * t, char *colname)
{
	node *n = t->columns->h;
	while (n) {
		column *c = n->data;
		if (strcmp(c->name, colname) == 0)
			return c;
		n = n->next;
	}
	return NULL;
}

catalog *default_catalog_create()
{
	catalog *c = NEW(catalog);

	c->cc = NULL;
	c->schemas = list_create((fdestroy)&schema_destroy);
	c->cur_schema = NULL;

	return c;
}

void catalog_destroy(catalog * cat)
{
	if (cat->cc && cat->cc_destroy)
		cat->cc_destroy(cat);
	list_destroy(cat->schemas);
	_DELETE(cat);
}

