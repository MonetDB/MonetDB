
#include <mem.h>
#include <string.h>
#include "types.h"
#include "sqlscan.h"

#include <gdk.h>

static int sql_debug = 0;

list *types = NULL;
static list *aggrs = NULL;
static list *funcs = NULL;

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

sql_subtype *sql_bind_subtype( char *name, int digits, int scale )
	/* todo add approximate info 
	 * if digits/scale == 0 and no approximate with digits/scale == 0
	 * exits we could return the type with largest digits 
	 *
	 * returning the largest when no exact match is found is now the
	 * (wrong?) default
	 */
{
	/* assumes the types are ordered on name,digits,scale where is always
	 * 0 > n
	 */
	node *m, *n;
	for ( n = types->h; n; n = n->next ) {
		sql_type *t = n->data;
		if (strcasecmp(t->sqlname, name) == 0){
			if ((digits && t->digits >= digits) || 
					(digits == t->digits)){
				return sql_create_subtype(t, digits, scale);
			}
			for (m = n->next; m; m = m->next ) {
				t = m->data;
				if (strcasecmp(t->sqlname, name) != 0){
					break;
				}
				n = m;
				if ((digits && t->digits >= digits) || 
					(digits == t->digits)){
					return sql_create_subtype(t, digits, scale);
				}
			}
			t = n->data;
			return sql_create_subtype(t, digits, scale);
		}
	}
	return NULL;
}

void sql_subtype_destroy(sql_subtype * t)
{
	_DELETE(t);
}

sql_subtype *sql_bind_localtype( char *name )
{
	node *n = types->h;
	while (n) {
		sql_type *t = n->data;
		if (strcmp(t->name, name) == 0){
			return sql_create_subtype(t, 0, 0);
		}
		n = n->next;
	}
	return NULL;
}

static int type_cmp( sql_type *t1, sql_type *t2)
{
	if (!t1 || !t2){	
		return -1;
	}
	/* types are only equal if they map onto the same systemtype */
	return strcmp(t1->name, t2->name);
}

int subtype_cmp( sql_subtype *t1, sql_subtype *t2 )
{
	int res = type_cmp( t1->type, t2->type);
	return res;
}


sql_aggr *sql_bind_aggr(char *sqlaname, sql_subtype *type)
{
	char *name = toLower(sqlaname);
	node *n = aggrs->h;
	while (n) {
		sql_aggr *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    (!t->tpe
		     || (type && strcmp(t->tpe, type->type->name) == 0))){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_func *sql_bind_func(char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3)
{
	char *name = toLower(sqlfname);
	node *n = funcs->h;
	while (n) {
		sql_func *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    ((tp1 && t->tpe1 && strcmp(t->tpe1, tp1->type->name) == 0) 
		     || (!tp1 && !t->tpe1)) && 
		    ((tp2 && t->tpe2 && strcmp(t->tpe2, tp2->type->name) == 0)
		     || (!tp2 && !t->tpe2)) && 
		    ((tp3 && t->tpe3 && strcmp(t->tpe3, tp3->type->name) == 0)
		     || (!tp3 && !t->tpe3))){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}

sql_func *sql_bind_func_result(char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res)
{
	char *name = toLower(sqlfname);
	node *n = funcs->h;
	while (n) {
		sql_func *t = n->data;
		if (strcmp(t->name, name) == 0 &&
		    strcmp(t->tpe1, tp1->type->name) == 0 &&
		    ((tp2 && t->tpe2 && strcmp(t->tpe2, tp2->type->name) == 0)
		     || (!tp2 && !t->tpe2)) && ((tp3 && t->tpe3
			 && strcmp(t->tpe3, tp3->type->name) == 0)
					|| (!tp3 && !t->tpe3))
		        	&& strcmp(t->res, res->type->name) == 0){
			_DELETE(name);
			return t;
		}
		n = n->next;
	}
	_DELETE(name);
	return NULL;
}


sql_type *sql_create_type(char *sqlname, int digits, int scale, int radix, char *name)
{
	sql_type *t = NEW(sql_type);

	t->sqlname = toLower(sqlname);
	t->digits = digits;
	t->scale = scale;
	t->radix = radix;
	t->name = _strdup(name);
	t->nr = list_length(types);
	if (!keyword_exists(t->sqlname))
		keywords_insert(t->sqlname, TYPE);
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
		t->tpe = _strdup(tpe);
	} else {
		t->tpe = NULL;
	}
	t->res = _strdup(res);
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
		t->tpe1 = _strdup(tpe1);
	} else {
		t->tpe1 = NULL;
	}
	if (strlen(tpe2)) {
		t->tpe2 = _strdup(tpe2);
	} else {
		t->tpe2 = NULL;
	}
	if (strlen(tpe3)) {
		t->tpe3 = _strdup(tpe3);
	} else {
		t->tpe3 = NULL;
	}
	t->res = _strdup(res);
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

void parser_init(int debug)
{
	sql_debug = debug;

	init_keywords();
	types = list_create((fdestroy)&type_destroy);
	aggrs = list_create((fdestroy)&aggr_destroy);
	funcs = list_create((fdestroy)&func_destroy);
}

void parser_exit()
{
	list_destroy(aggrs);
	list_destroy(funcs);
	list_destroy(types);

	exit_keywords();
}
