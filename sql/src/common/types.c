
#include <mem.h>
#include <string.h>
#include "types.h"
#include "sql.h"

static int sql_debug = 0;

list *aliases = NULL;
list *types = NULL;
list *aggrs = NULL;
list *funcs = NULL;

void sql_create_alias( char *org, char *alias ){
	sql_alias *a = NEW(sql_alias);
	a->org = toLower(org);
	a->alias = toLower(alias);

	list_append(aliases, a);
}

static void alias_destroy(sql_alias * a)
{
	_DELETE(a->org);
	_DELETE(a->alias);
	_DELETE(a);
}

static char *sql_bind_alias( char *org ){
	node *n;
	for (n = aliases->h; n; n = n->next){
		sql_alias *a = n->data;
		if (strcmp(a->org, org) == 0){
			return a->alias;
		}
	}
	return NULL;
}

/* later types + alias should add a keyword to the keyword table */

sql_subtype *sql_create_subtype( sql_type *t, int digits, int scale )
{
	sql_subtype *res = NEW(sql_subtype);
	res->type = t;
	res->digits = digits;
	res->scale = scale;
	return res;
}

sql_subtype *sql_dup_subtype( sql_subtype *t )
{
	sql_subtype *res = NEW(sql_subtype);
	*res = *t;
	return res;
}

static sql_type *sql_bind_type_(char *sqlname)
{
	node *n = types->h;
	while (n) {
		sql_type *t = n->data;
		if (strcmp(t->sqlname, sqlname) == 0){
			return t;
		}
		n = n->next;
	}
	return NULL;
}

sql_type *sql_bind_type(char *sqlname)
{
	char *name = toLower(sqlname);
	sql_type *res = sql_bind_type_(name);

	if (!res){
		char *alias = sql_bind_alias(name);
		if (alias)
			res = sql_bind_type_(alias);
	}
	_DELETE(name);
	return res;
}

static int type_cmp( sql_type *t1, sql_type *t2)
{
	if (!t1 || !t2){	
		return -1;
	}
	return strcmp(t1->sqlname, t2->sqlname);
}

int subtype_cmp( sql_subtype *t1, sql_subtype *t2 )
{
	int res = type_cmp( t1->type, t2->type);
	return res;
}


sql_aggr *sql_bind_aggr(char *sqlname, sql_subtype *type)
{
	char *name = toLower(sqlname);
	node *n = aggrs->h;
	while (n) {
		sql_aggr *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    (!t->tpe
		     || (type && subtype_cmp(t->tpe, type) == 0))){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_func *sql_bind_func(char *sqlname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3)
{
	char *name = toLower(sqlname);
	node *n = funcs->h;
	while (n) {
		sql_func *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    ((tp1 && t->tpe1 && subtype_cmp(t->tpe1, tp1) == 0) 
		     || (!tp1 && !t->tpe1)) && 
		    ((tp2 && t->tpe2 && subtype_cmp(t->tpe2, tp2) == 0)
		     || (!tp2 && !t->tpe2)) && 
		    ((tp3 && t->tpe3 && subtype_cmp(t->tpe3, tp3) == 0)
		     || (!tp3 && !t->tpe3))){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_func *sql_bind_func_result(char *sqlname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res)
{
	char *name = toLower(sqlname);
	node *n = funcs->h;
	while (n) {
		sql_func *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    subtype_cmp(t->tpe1, tp1) == 0 &&
		    ((tp2 && t->tpe2 && subtype_cmp(t->tpe2, tp2) == 0)
		     || (!tp2 && !t->tpe2)) && ((tp3 && t->tpe3
			 && subtype_cmp(t->tpe3, tp3) == 0)
					|| (!tp3 && !t->tpe3))
		        	&& subtype_cmp(t->res, res) == 0){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}


sql_type *sql_create_type(char *sqlname, char *name)
{
	sql_type *t = NEW(sql_type);

	t->sqlname = toLower(sqlname);
	t->name = _strdup(name);
	t->nr = list_length(types);
	list_append(types, t);
	return t;
}

static void type_destroy(sql_type * t)
{
	_DELETE(t->sqlname);
	_DELETE(t->name);
	_DELETE(t);
}

sql_aggr *sql_create_aggr(char *name, char *imp, char *tpe, char *res )
{
	sql_aggr *t = NEW(sql_aggr);

	t->name = toLower(name);
	t->imp = _strdup(imp);
	if (strlen(tpe)) {
		t->tpe = sql_create_subtype(sql_bind_type(tpe), 0, 0);
		assert(t->tpe);
		assert(t->tpe->type);
	} else {
		t->tpe = NULL;
	}
	t->res = sql_create_subtype(sql_bind_type(res), 0, 0);
	assert(t->res);
	assert(t->res->type);
	t->nr = list_length(aggrs);
	list_append(aggrs, t);
	return t;
}

static void aggr_destroy(sql_aggr * t)
{
	_DELETE(t->name);
	_DELETE(t->imp);
	if (t->tpe) _DELETE(t->tpe);
	_DELETE(t->res);
	_DELETE(t);
}

sql_func *sql_create_func(char *name, char *imp, char *tpe1,
		      char *tpe2, char *tpe3, char *res )
{
	sql_func *t = NEW(sql_func);

	t->name = toLower(name);
	t->imp = _strdup(imp);
	if (strlen(tpe1)) {
		t->tpe1 = sql_create_subtype(sql_bind_type(tpe1), 0, 0);
		assert(t->tpe1);
		assert(t->tpe1->type);
	} else {
		t->tpe1 = NULL;
	}
	if (strlen(tpe2)) {
		t->tpe2 = sql_create_subtype(sql_bind_type(tpe2), 0, 0);
		assert(t->tpe2);
		assert(t->tpe2->type);
	} else {
		t->tpe2 = NULL;
	}
	if (strlen(tpe3)) {
		t->tpe3 = sql_create_subtype(sql_bind_type(tpe3), 0, 0);
		assert(t->tpe3);
		assert(t->tpe3->type);
	} else {
		t->tpe3 = NULL;
	}
	t->res = sql_create_subtype(sql_bind_type(res), 0, 0);
	assert(t->res);
	assert(t->res->type);
	t->nr = list_length(funcs);
	list_append(funcs, t);
	return t;
}

static void func_destroy(sql_func * t)
{
	_DELETE(t->name);
	_DELETE(t->imp);
	if (t->tpe1) _DELETE(t->tpe1);
	if (t->tpe2) _DELETE(t->tpe2);
	if (t->tpe3) _DELETE(t->tpe3);
	_DELETE(t->res);
	_DELETE(t);
}

void types_init(int debug){
	sql_debug = debug;

	aliases = list_create((fdestroy)&alias_destroy);
	types = list_create((fdestroy)&type_destroy);
	aggrs = list_create((fdestroy)&aggr_destroy);
	funcs = list_create((fdestroy)&func_destroy);
}

void types_exit(){
	list_destroy(aggrs);
	list_destroy(funcs);
	list_destroy(types);
	list_destroy(aliases);
}

void sql_new_type( char *sqlname, char *name ){
	(void)sql_create_type( sqlname, name );
}
void sql_new_aggr( char *name, char *imp, char *tpe, char *res ){
	(void)sql_create_aggr( name, imp, tpe, res );
}
void sql_new_func( char *name, char *imp,
		      char *tpe1, char *tpe2, char *tpe3, char *res ){
	(void)sql_create_func( name, imp, tpe1, tpe2, tpe3, res );
}
