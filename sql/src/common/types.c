
#include <mem.h>
#include <string.h>
#include "types.h"
#include "sql.h"

static int sql_debug = 0;

list *types = NULL;
list *aggrs = NULL;
list *funcs = NULL;

sql_type *sql_bind_type(char *sqlname)
{
	char *name = toLower(sqlname);
	node *n = types->h;
	while (n) {
		sql_type *t = n->data;
		if (strcmp(t->sqlname, name) == 0){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_aggr *sql_bind_aggr(char *sqlname, char *type)
{
	char *name = toLower(sqlname);
	node *n = aggrs->h;
	while (n) {
		sql_aggr *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    (!t->tpe
		     || (type && strcmp(t->tpe->sqlname, type) == 0))){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_func *sql_bind_func(char *sqlname, char *tp1, char *tp2, char *tp3)
{
	char *name = toLower(sqlname);
	node *n = funcs->h;
	while (n) {
		sql_func *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    strcmp(t->tpe1->sqlname, tp1) == 0 &&
		    ((tp2 && t->tpe2 && strcmp(t->tpe2->sqlname, tp2) == 0)
		     || (!tp2 && !t->tpe2)) && ((tp3 && t->tpe3
						 && strcmp(t->tpe3->
							   sqlname,
							   tp3) == 0)
						|| (!tp3 && !t->tpe3))){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_func *sql_bind_func_result(char *sqlname,
			   char *tp1, char *tp2, char *tp3, char *res)
{
	char *name = toLower(sqlname);
	node *n = funcs->h;
	while (n) {
		sql_func *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    strcmp(t->tpe1->sqlname, tp1) == 0 &&
		    ((tp2 && t->tpe2 && strcmp(t->tpe2->sqlname, tp2) == 0)
		     || (!tp2 && !t->tpe2)) && ((tp3 && t->tpe3
						 && strcmp(t->tpe3->
							   sqlname,
							   tp3) == 0)
						|| (!tp3 && !t->tpe3))
		    && strcmp(t->res->sqlname, res) == 0){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}


sql_type *sql_create_type(char *sqlname, char *name, char *cast )
{
	sql_type *t = NEW(sql_type);

	t->sqlname = toLower(sqlname);
	t->name = _strdup(name);
	t->cast = NULL;
	if (strlen(cast) > 0)
		t->cast = sql_bind_type(cast);
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
		t->tpe = sql_bind_type(tpe);
		assert(t->tpe);
	} else {
		t->tpe = NULL;
	}
	t->res = sql_bind_type(res);
	assert(t->res);
	t->nr = list_length(aggrs);
	list_append(aggrs, t);
	return t;
}

static void aggr_destroy(sql_aggr * t)
{
	_DELETE(t->name);
	_DELETE(t->imp);
	_DELETE(t);
}

sql_func *sql_create_func(char *name, char *imp, char *tpe1,
		      char *tpe2, char *tpe3, char *res )
{
	sql_func *t = NEW(sql_func);

	t->name = toLower(name);
	t->imp = _strdup(imp);
	t->tpe1 = sql_bind_type(tpe1);
	assert(t->tpe1);
	if (strlen(tpe2)) {
		t->tpe2 = sql_bind_type(tpe2);
		assert(t->tpe2);
	} else {
		t->tpe2 = NULL;
	}
	if (strlen(tpe3)) {
		t->tpe3 = sql_bind_type(tpe3);
		assert(t->tpe3);
	} else {
		t->tpe3 = NULL;
	}
	t->res = sql_bind_type(res);
	assert(t->res);
	t->nr = list_length(funcs);
	list_append(funcs, t);
	return t;
}

static void func_destroy(sql_func * t)
{
	_DELETE(t->name);
	_DELETE(t->imp);
	_DELETE(t);
}

void types_init(int debug){
	sql_debug = debug;

	types = list_create((fdestroy)&type_destroy);
	aggrs = list_create((fdestroy)&aggr_destroy);
	funcs = list_create((fdestroy)&func_destroy);
}

void types_exit(){
	list_destroy(aggrs);
	list_destroy(funcs);
	list_destroy(types);
}

void types_export(stream *s){
	char buf[BUFSIZ];
	node *n;
	int i;

	i = snprintf(buf, BUFSIZ, "%d\n", list_length(types) );
	s->write(s, buf, i, 1);
	for (n = types->h; n; n = n->next){
		sql_type *t = n->data;
		i = snprintf(buf, BUFSIZ, "%s,%s,%s\n", 
			t->sqlname, t->name, (t->cast)?t->cast->sqlname:"" );
		s->write(s, buf, i, 1);
	}
	i = snprintf(buf, BUFSIZ, "%d\n", list_length(aggrs) );
	s->write(s, buf, i, 1);
	for (n = aggrs->h; n; n = n->next){
		sql_aggr *a = n->data;
		i = snprintf(buf, BUFSIZ, "%s,%s,%s,%s\n", a->name, a->imp, 
			(a->tpe)?a->tpe->sqlname:"", a->res->sqlname	);
		s->write(s, buf, i, 1);
	}
	i = snprintf(buf, BUFSIZ, "%d\n", list_length(funcs) );
	s->write(s, buf, i, 1);
	for (n = funcs->h; n; n = n->next){
		sql_func *f = n->data;
		int i = snprintf(buf, BUFSIZ, "%s,%s,%s,%s,%s,%s\n", f->name, f->imp, 
			(f->tpe1)?f->tpe1->sqlname:"", (f->tpe2)?f->tpe2->sqlname:"", 
			(f->tpe3)?f->tpe3->sqlname:"", f->res->sqlname	);
		s->write(s, buf, i, 1);
	}
	s->flush(s);
}

void sql_new_type( char *sqlname, char *name, char *cast ){
	(void)sql_create_type( sqlname, name, cast );
}
void sql_new_aggr( char *name, char *imp, char *tpe, char *res ){
	(void)sql_create_aggr( name, imp, tpe, res );
}
void sql_new_func( char *name, char *imp,
		      char *tpe1, char *tpe2, char *tpe3, char *res ){
	(void)sql_create_func( name, imp, tpe1, tpe2, tpe3, res );
}
