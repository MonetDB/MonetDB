
#include <string.h>
#include "mem.h"
#include "scope.h"

static void cvar_destroy(cvar *cv) 
{
	--cv->refcnt;
	if (cv->refcnt <= 0) {
		if (cv->s)
			stmt_destroy(cv->s);
		if (cv->tname)
			_DELETE(cv->tname);
		if (cv->cname)
			_DELETE(cv->cname);
		_DELETE(cv);
	}
}

void tvar_destroy(tvar *v){
	--v->refcnt;
	if (v->refcnt <= 0) {
		list_destroy(v->columns);
		if (v->s)
			stmt_destroy(v->s);
		if (v->tname)
			_DELETE(v->tname);
		_DELETE(v);
	}
}

static void var_destroy(var * v)
{
	--v->refcnt;
	if (v->refcnt <= 0) {
		if (v->s)
			stmt_destroy(v->s);
		if (v->name)
			_DELETE(v->name);
		_DELETE(v);
	}
}

scope *scope_open(scope * p)
{
	scope *s = NEW(scope);

	s->ptable = stmt_ptable();
	s->basetables = list_create( (fdestroy)&stmt_destroy );
	s->outers = list_create( (fdestroy)&stmt_destroy );

	s->tables = list_create((fdestroy)&tvar_destroy);
	s->aliases = list_create((fdestroy)&var_destroy);
	s->p = p;
	return s;
}

scope *scope_close(scope * s)
{
	scope *p = s->p;

	stmt_destroy(s->ptable);
	list_destroy(s->basetables);
	list_destroy(s->outers);

	list_destroy(s->tables);
	list_destroy(s->aliases);
	_DELETE(s);
	return p;
}

static stmt *find_pivot( scope *scp, stmt *t )
{
	node *n;
	for (n = ptable_ppivots(scp->ptable)->h; n; n = n->next)
	{
		stmt *p = n->data;
		if (p->t == t)
			return p;
	}
	assert(0);
	return NULL;
}


stmt *scope_find_pivot(scope *scp, stmt *t){
	stmt *p  = find_pivot(scp, t);
	if (p) return stmt_dup(p);
	assert(0);
	return NULL;
}
stmt *scope_first_pivot(scope *scp){
	node *n = ptable_ppivots(scp->ptable)->h;
	if (n) return stmt_dup(n->data);
	assert(0);
	return NULL;
}

static int stmt_cmp(stmt *s1, stmt *s2)
{
	return (int)(s1-s2);
}

static void scope_add_table_(scope * scp, stmt *s, int outer ){
	
	list_append(scp->basetables, stmt_dup(s));
	if (outer) {
		if (list_find(scp->outers, s, (fcmp)&stmt_cmp) == NULL)
			list_append(scp->outers, stmt_dup(s));
	}
	stmt_destroy(stmt_pivot(stmt_dup(s), scp->ptable));
}

cvar *table_add_column(tvar * t, stmt * s, 
		char *tname, char *cname)
{
	cvar *v = NEW(cvar);
	v->s = s; 
	v->table = t;
	assert((!tname || strlen(tname)));

	v->tname = (tname) ? _strdup(tname) : NULL;
	v->cname = _strdup(cname);
	v->refcnt = 1;
	list_append(t->columns, v);
	return v;
}

tvar *scope_add_table(scope * scp, stmt *s, char *tname)
{
	tvar *v = NEW(tvar);
	v->s = s; 
	v->columns = list_create((fdestroy)&cvar_destroy);
	v->tname = (tname)?_strdup(tname):NULL;
	v->refcnt = 1;
	list_append(scp->tables, v);

	scope_add_table_(scp, s, 0);
	return v;
}

var *scope_add_alias(scope * scp, stmt * s, char *name)
{
	var *v = NEW(var);
	v->s = s; 
	v->name = _strdup(name);
	v->refcnt = 1;
	list_append(scp->aliases, v);
	return v;
}

tvar *scope_bind_table(scope * scp, char *name )
{
	for (; scp; scp = scp->p) {
		node *n = scp->tables->h;
		for (; n; n = n->next) {
			tvar *v = n->data;
			if (v->tname && strcmp(v->tname, name) == 0) {
				return v;
			}
		}
	}
	return NULL;
}

static cvar *bind_column(list * columns, char *cname)
{
	node *n = columns->h;
	for (; n; n = n->next) {
		cvar *c = n->data;
		if (strcmp(c->cname, cname) == 0) {
			return c;
		}
	}
	return NULL;
}

static cvar *bind_table_column(list * columns, char *tname, char *cname)
{
	node *n = columns->h;
	for (; n; n = n->next) {
		cvar *c = n->data;
		if (	strcmp(c->tname, tname) == 0 &&
			strcmp(c->cname, cname) == 0) {
			return c;
		}
	}
	return NULL;
}

cvar *scope_bind_column(scope * scp, char *tname, char *cname )
{
	cvar *cv = NULL;
	if (!tname){
		node *n = scp->tables->h;
		for (; n; n = n->next) {
			tvar *tv = n->data;
			if ( (cv = bind_column(tv->columns, cname)) != NULL) {
				return cv;
			}
		}
	} else {
		node *n = scp->tables->h;
		for (; n; n = n->next) {
			tvar *tv = n->data;
			if (tv->tname && strcmp(tv->tname, tname) == 0 &&
			   (cv = bind_column(tv->columns, cname)) != NULL) {
				return cv;
			} else if (!tv->tname &&
			   (cv = bind_table_column(tv->columns, 
						   tname, cname)) != NULL) {
				return cv;
			}
		}
	}
	return NULL;
}

static stmt *scope_bind_column_(scope * scp, char *tname, char *cname )
{
	scope *start = scp;
	cvar *cv = NULL;
	if (!tname){ /* TODO: return NULL, if name exists more the once */ 
		for (; scp; scp = scp->p) {
			node *n = scp->tables->h;
			for (; n; n = n->next) {
				tvar *tv = n->data;
				if ( (cv = bind_column(tv->columns, cname)) != NULL) {
					if (start != scp){
						stmt *s = find_pivot(scp, tv->s);
						/* add outer ref */
						scope_add_table_(start, s, 1);
						return stmt_ibat(stmt_join(stmt_dup(s),
							stmt_dup(cv->s), cmp_equal), stmt_dup(s));
					}
					return cv->s;
				}
			}
		}
		return NULL;
	}

	/* tname != NULL */
	for (; scp; scp = scp->p) {
		node *n = scp->tables->h;
		for (; n; n = n->next) {
			tvar *tv = n->data;
			if (tv->tname && strcmp(tv->tname, tname) == 0 &&
			   (cv = bind_column(tv->columns, cname)) != NULL) {
				if (start != scp){
					stmt *s = find_pivot(scp, tv->s);
					/* add outer ref */
					scope_add_table_(start, s, 1);
					return stmt_ibat(stmt_join(stmt_dup(s),
							stmt_dup(cv->s), cmp_equal), stmt_dup(s));
				}
				return cv->s;
			} else if (!tv->tname &&
			   (cv = bind_table_column(tv->columns, 
						   tname, cname)) != NULL) {
				if (start != scp){
					stmt *s = find_pivot(scp, tv->s);
					/* add outer ref */
					scope_add_table_(start, s, 1);
					return stmt_ibat(stmt_join(stmt_dup(s),
							stmt_dup(cv->s), cmp_equal), stmt_dup(s));
				}
				return cv->s;
			}
		}
	}
	return NULL;
}

static var *scope_bind_alias(scope * scp, char *name )
{
	for (; scp; scp = scp->p) {
		node *n = scp->aliases->h;
		for (; n; n = n->next) {
			var *v = n->data;
			if (strcmp(v->name, name) == 0)
				return v;
		}
	}
	return NULL;
}

stmt *scope_bind(scope * scp, char *tname, char *cname )
{
	stmt *s = scope_bind_column_( scp, tname, cname );
	if (!s && !tname){
		var *a = scope_bind_alias( scp, cname );
		if (a) return stmt_dup(a->s);
		return NULL;
	}
	if (s) return stmt_dup(s);
	return NULL;
}

cvar *scope_first_column(scope * scp)
{
	if (scp->tables && list_length(scp->tables)) {
		node *n = scp->tables->h;
		tvar *tv = n->data;
		if (list_length(tv->columns)){
			node *m = tv->columns->h;
			return m->data;
		}
	}
	return NULL;
}

/* first and only table */
tvar *scope_first_table(scope * scp)
{
	for (; scp; scp = scp->p) {
		node *n = scp->tables->h;
		for (; n; n = n->next) {
			tvar *tv = n->data;
			return tv;
		}
	}
	return NULL;
}


void scope_dump(scope * scp)
{
	for (; scp; scp = scp->p) {
		node *n = scp->tables->h;
		printf("\t-> tables: \n");
		for (; n; n = n->next) {
			tvar *tv = n->data;
			node *m = tv->columns->h;
			printf("\t\t(%s)", tv->tname?tv->tname:"");
			for (; m; m = m->next) {
				cvar *cv = m->data;
				printf("(%s.%s)", cv->tname?cv->tname:"", 
						cv->cname);
			}
			printf("\n");
		}
	}
}
