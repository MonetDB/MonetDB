
#include <string.h>
#include "mem.h"
#include "statement.h"

/* todo make proper traversal operations */

static stmt *stmt_create()
{
	stmt *s = NEW(stmt);
	s->type = st_none;
	s->op1.sval = NULL;
	s->op2.sval = NULL;
	s->op3.sval = NULL;
	s->flag = 0;
	s->nrcols = 0;
	s->key = 0;
	s->nr = 0;
	s->h = NULL;
	s->t = NULL;
	s->refcnt = 1;
	return s;
}

static stmt *stmt_ext( stmt *grp )
{
	stmt *ns = stmt_create();

	ns->type = st_group_ext;
	ns->op1.stval = grp;
	ns->nrcols = grp->nrcols;
	ns->key = 1;
	ns->t = stmt_dup(grp->t);
	return ns;
}

static stmt *stmt_group(stmt * s)
{
	stmt *ns = stmt_create();
	ns->type = st_group;
	ns->op1.stval = s;
	ns->nrcols = s->nrcols;
	ns->key = 0;
	ns->t = stmt_dup(s->t);
	return ns;
}

static stmt *stmt_derive(stmt * s, stmt * t)
{
	stmt *ns = stmt_create();
	ns->type = st_derive;
	ns->op1.stval = s;
	ns->op2.stval = t;
	ns->nrcols = s->nrcols;
	ns->key = 0;
	ns->t = stmt_dup(s->t);
	return ns;
}

void grp_reset( group * g)
{
	stmt_reset(g->grp);
	stmt_reset(g->ext);
}

void grp_destroy( group * g)
{
	assert(g->refcnt > 0);
	if (--g->refcnt == 0) {
		stmt_destroy(g->grp);
		stmt_destroy(g->ext);
		_DELETE(g);
	}
}

group *grp_dup( group * g) 
{
	if (g) g->refcnt++;
	return g;
}

group *grp_create( stmt *s, group *og )
{
	group *g = NEW(group);
	if (og){
		g->grp = stmt_derive(stmt_dup(og->grp), s);
		grp_destroy(og);
	} else {
		g->grp = stmt_group(s);
	}
	g->ext = stmt_ext(stmt_dup(g->grp));
	g->refcnt = 1;
	return g;
}

static stmt *stmt_semijoin_tail( stmt *op1, stmt *op2 )
{
	return stmt_reverse(stmt_semijoin(stmt_reverse(op1), op2));
}

group *grp_semijoin( group *og, stmt *s )
{
	group *g = NEW(group);
	g->grp = stmt_semijoin_tail(stmt_dup(og->grp),s);
	g->ext = stmt_semijoin(stmt_dup(og->ext),stmt_dup(s));
	g->refcnt = 1;
	grp_destroy(og);
	return g;
}

void stmt_destroy(stmt * s)
{
	assert(s->refcnt > 0);
	if (--s->refcnt == 0) {
		switch (s->type) {
			/* stmt_destroy  op1 */
		case st_column: case st_create_column:
		case st_table: case st_create_table: 

		case st_not_null:
		case st_reverse:
		case st_count:
		case st_group:
		case st_group_ext:
		case st_order:
		case st_unop:
		case st_output:
		case st_result:
			stmt_destroy(s->op1.stval);
			break;
		case st_alias:
		case st_column_alias:
		case st_drop_table:
			stmt_destroy(s->op1.stval);
			_DELETE(s->op2.sval);
			if (s->op3.sval) _DELETE(s->op3.sval);
			break;
		case st_exists:
			stmt_destroy(s->op1.stval);
			list_destroy(s->op2.lval);
			break;
			/* stmt_destroy  op1 and op2 */
		case st_default:
		case st_like:
		case st_semijoin:
		case st_diff:
		case st_intersect:
		case st_union:
		case st_join:
		case st_outerjoin:
		case st_const:
		case st_derive:
		case st_ordered:
		case st_reorder:
		case st_binop:
		case st_insert:
		case st_replace:
			stmt_destroy(s->op1.stval);
			stmt_destroy(s->op2.stval);
			break;
		case st_create_key:
			if (s->op2.stval)
				stmt_destroy(s->op2.stval);
			break;
		case st_delete:
			if (s->op2.stval)
				stmt_destroy(s->op2.stval);
			break;
		case st_mark:
		case st_unique:
			stmt_destroy(s->op1.stval);
			if (s->op2.gval)
				grp_destroy(s->op2.gval);
			break;
		case st_select:
		case st_select2:
			stmt_destroy(s->op1.stval);
			stmt_destroy(s->op2.stval);
			if (s->op3.stval)
				stmt_destroy(s->op3.stval);
			break;
		case st_aggr:
			stmt_destroy(s->op1.stval);
			if (s->op3.gval)
				grp_destroy(s->op3.gval);
			break;
		case st_set:
		case st_sets:
		case st_list:
		case st_triop:
			list_destroy(s->op1.lval);
			break;
		case st_atom:
			atom_destroy(s->op1.aval);
			break;

		case st_bat:
		case st_ubat:

		case st_schema: case st_create_schema: case st_drop_schema:

		case st_none:
			break;
		case st_release:
			if (s->op1.sval) _DELETE(s->op1.sval);
			break;
		case st_commit:
		case st_rollback:
			if (s->op2.sval) _DELETE(s->op2.sval);
			break;
		case st_copyfrom:
			if (s->op2.lval) list_destroy(s->op2.lval);
			if (s->op3.lval) list_destroy(s->op3.lval);
			break;
		case st_create_role:
		case st_drop_role:
		case st_grant_role:
		case st_revoke_role:
			_DELETE(s->op1.sval);
			if (s->op2.sval) _DELETE(s->op2.sval);
			break;
		}
		if (s->h) 
			stmt_destroy(s->h);
		if (s->t) 
			stmt_destroy(s->t);
		_DELETE(s);
	}
}

void stmt_reset( stmt *s ){
    	node *n;

	if (s->nr == 0) return;

	s->nr = 0;
	switch(s->type){
		/* stmt_reset  op1 */
	case st_not_null: case st_reverse: case st_count: 
	case st_group: case st_group_ext: 
	case st_order: case st_unop: case st_alias: case st_column_alias:
	case st_output: case st_result: case st_exists: 
	case st_table: case st_create_table: case st_drop_table: 

		stmt_reset(s->op1.stval);
		break;

	case st_default: case st_like: case st_semijoin: 
	case st_diff: case st_intersect: case st_union: case st_join: 
	case st_outerjoin: 
	case st_const: case st_derive: case st_ordered: case st_reorder: 
	case st_binop: case st_insert: case st_replace: 

		stmt_reset(s->op1.stval);
		stmt_reset(s->op2.stval);
		break;
	case st_delete: 

		if (s->op2.stval)
			stmt_reset(s->op2.stval);
		break;
	case st_mark: case st_unique: 
		stmt_reset(s->op1.stval);
		if (s->op2.gval)
			grp_reset(s->op2.gval);
		break;
	case st_create_key:
		if (s->op2.stval)
			stmt_reset(s->op2.stval);
		break;
	case st_select: case st_select2: 

		stmt_reset(s->op1.stval);
		stmt_reset(s->op2.stval);
		if (s->op3.stval)
			stmt_reset(s->op3.stval);
		break;
	case st_aggr: 
		stmt_reset(s->op1.stval);
		if (s->op3.gval)
			grp_reset(s->op3.gval);
		break;
	case st_set: case st_list: case st_triop: 
		for (n = s->op1.lval->h; n; n = n->next ){
			stmt_reset( n->data );
		}
		break;
	case st_sets: {
		for(n = s->op1.lval->h; n; n = n->next ){
			list *l = n->data;
			node *m = l->h;
			while(m){
				stmt_reset( m->data );
			}
		}
	} break;

	case st_column: case st_create_column: 
		stmt_reset( s->op1.stval );
		if (s->op2.cval->s){
			stmt_reset( s->op2.cval->s );
		}
		break;
	case st_bat:
		if (s->op1.cval->s){
			stmt_reset( s->op1.cval->s );
		}
		break;
	case st_schema: case st_drop_schema: case st_create_schema: 
	case st_release: case st_commit: case st_rollback:
	case st_atom: case st_none: case st_copyfrom:
		break;
	}
}


stmt *stmt_release(char *name)
{
	stmt *s = stmt_create();
	s->type = st_release;
	if (name)
		s->op1.sval = _strdup(name);
	return s;
}

stmt *stmt_commit(int chain, char *name)
{
	stmt *s = stmt_create();
	s->type = st_commit;
	s->op1.ival = chain;
	if (name)
		s->op2.sval = _strdup(name);
	return s;
}

stmt *stmt_rollback(int chain, char *name)
{
	stmt *s = stmt_create();
	s->type = st_rollback;
	s->op1.ival = chain;
	if (name)
		s->op2.sval = _strdup(name);
	return s;
}

stmt *stmt_bind_schema(schema * sc)
{
	stmt *s = stmt_create();
	s->type = st_schema;
	s->op1.schema = sc;
	return s;
}

stmt *stmt_bind_table(stmt *schema, table * t)
{
	stmt *s = stmt_create();
	s->type = st_table;
	s->op1.stval = schema;
	s->op2.tval = t;
	return s;
}

/* split in two parts */
stmt *stmt_bind_column(stmt *table, column * c)
{
	stmt *s = c->s;
	if (s){
	} else {
		s = stmt_create();
		s->type = st_column;
		s->op1.stval = table;
		s->op2.cval = c;
	}
	return s;
}

stmt *stmt_bind_key(stmt *table, key * k)
{
	stmt *s = stmt_create();
	s->type = st_key;
	s->op1.stval = table;
	s->op2.kval = k;
	return s;
}

stmt *stmt_create_schema(schema * schema)
{
	stmt *s = stmt_create();
	s->type = st_create_schema;
	s->op1.schema = schema;
	return s;
}

stmt *stmt_drop_schema(schema * schema, int dropaction)
{
	stmt *s = stmt_create();
	s->type = st_drop_schema;
	s->op1.schema = schema;
	s->flag = dropaction;
	return s;
}

stmt *stmt_create_table(stmt *schema, table * t)
{
	stmt *s = stmt_create();
	s->type = st_create_table;
	s->op1.stval = schema;
	s->op2.tval = t;
	return s;
}

stmt *stmt_drop_table(stmt * schema, char *name, int drop_action)
{
	stmt *s = stmt_create();
	s->type = st_drop_table;
	s->op1.stval = schema;
	s->op2.sval = _strdup(name);
	s->flag = drop_action;
	return s;
}

stmt *stmt_create_column(stmt *table, column * c)
{
	stmt *s = stmt_create();
	s->type = st_create_column;
	s->op1.stval = table;
	s->op2.cval = c;
	return s;
}

stmt *stmt_not_null(stmt * col)
{
	stmt *s = stmt_create();
	s->type = st_not_null;
	s->op1.stval = col;
	return s;
}

stmt *stmt_default(stmt * col, stmt * def)
{
	stmt *s = stmt_create();
	s->type = st_default;
	s->op1.stval = col;
	s->op2.stval = def;
	return s;
}

stmt *stmt_key( key *k, stmt *rk )
{
	stmt *s = stmt_create();
	s->type = st_create_key;
	s->op1.kval = k; 
	if (rk){
		s->op2.stval = rk; 
	}
	return s;
}

stmt *stmt_create_role(char *name, int admin)
{
	stmt *s = stmt_create();
	s->type = st_create_role;
	s->op1.sval = _strdup(name);
	s->flag = admin; 
	return s;
}

stmt *stmt_drop_role(char *name ) 
{
	stmt *s = stmt_create();
	s->type = st_drop_role;
	s->op1.sval = _strdup(name);
	return s;
}

stmt *stmt_grant_role(char *authid, char *role)
{
	stmt *s = stmt_create();
	s->type = st_grant_role;
	s->op1.sval = _strdup(authid);
	s->op2.sval = _strdup(role);
	return s;
}
stmt *stmt_revoke_role(char *authid, char *role)
{
	stmt *s = stmt_create();
	s->type = st_revoke_role;
	s->op1.sval = _strdup(authid);
	s->op2.sval = _strdup(role);
	return s;
}

stmt *stmt_basetable( table *t )
{
	stmt *s = stmt_create();
	s->type = st_basetable;
	s->op1.tval = t;
	return s;
}

stmt *stmt_cbat(column * op1, stmt * basetable, int access, int type)
{
	stmt *s = stmt_create();
	s->type = type;
	s->op1.cval = op1;
	s->nrcols = 1;
	s->flag = access;
	s->h = basetable;
	return s;
}

stmt *stmt_tbat(table * t, int access, int type)
{
	stmt *s = stmt_create();
	s->type = type;
	s->nrcols = 0;
	s->flag = access;
	s->op1.tval = t;
	return s;
}

stmt *stmt_count(stmt * s)
{
	stmt *ns = stmt_create();
	ns->type = st_count;
	ns->op1.stval = s;
	ns->nrcols = 0;
	ns->t = stmt_dup(s->t);
	return ns;
}

stmt *stmt_const(stmt * s, stmt * val)
{
	stmt *ns = stmt_create();
	ns->type = st_const;
	ns->op1.stval = s;
	ns->op2.stval = val;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->h = stmt_dup(s->h);
	return ns;
}

stmt *stmt_mark(stmt * s, int id)
{
	stmt *ns = stmt_create();
	ns->type = st_mark;
	ns->op1.stval = s;
	ns->flag = id;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->t = stmt_dup(s->t);
	return ns;
}

stmt *stmt_remark(stmt * s, stmt * t, int id)
{
	stmt *ns = stmt_create();
	ns->type = st_mark;
	ns->op1.stval = s;
	ns->op2.stval = t;
	ns->flag = id;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->t = stmt_dup(s->t);
	return ns;
}

stmt *stmt_reverse(stmt * s)
{
	stmt *ns = stmt_create();
	ns->type = st_reverse;
	ns->op1.stval = s;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->h = stmt_dup(s->t);
	ns->t = stmt_dup(s->h);
	return ns;
}


stmt *stmt_unique(stmt * s, group * g)
{
	stmt *ns = stmt_create();
	ns->type = st_unique;
	ns->op1.stval = s;
	if (g) {
		ns->op2.gval = g;
	}
	ns->nrcols = s->nrcols;
	ns->key = 1; /* ?? maybe change key to unique ? */
	ns->t = stmt_dup(s->t);
	return ns;
}

stmt *stmt_order(stmt * s, int direction)
{
	stmt *ns = stmt_create();
	ns->type = st_order;
	ns->op1.stval = s;
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->t = stmt_dup(s->t);
	return ns;
}

stmt *stmt_reorder(stmt * s, stmt * t, int direction)
{
	stmt *ns = stmt_create();
	ns->type = st_reorder;
	ns->op1.stval = s;
	ns->op2.stval = t;
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->t = stmt_dup(s->t);
	return ns;
}

stmt *stmt_atom(atom * op1)
{
	stmt *s = stmt_create();
	s->type = st_atom;
	s->op1.aval = op1;
	s->key = 1; /* values are also unique */
	return s;
}

stmt *stmt_select(stmt * op1, stmt * op2, comp_type cmptype)
{
	stmt *s = stmt_create();
	s->type = st_select;
	s->op1.stval = op1;
	s->op2.stval = op2;
	assert(cmptype >= cmp_equal && cmptype <= cmp_gte);
	s->flag = cmptype;
	s->nrcols = 1;
	s->h = stmt_dup(s->op1.stval->h);
	return s;
}

stmt *stmt_select2(stmt * op1, stmt * op2,
			     stmt * op3, int cmp)
{
	stmt *s = stmt_create();
	s->type = st_select2;
	s->op1.stval = op1;
	s->op2.stval = op2;
	s->op3.stval = op3;
	s->flag = cmp;
	s->nrcols = 1;
	s->h = stmt_dup(s->op1.stval->h);
	return s;
}

stmt *stmt_like(stmt * op1, stmt * a)
{
	stmt *s = stmt_create();
	s->type = st_like;
	s->op1.stval = op1;
	s->op2.stval = a;
	s->nrcols = 1;
	s->h = stmt_dup(s->op1.stval->h);
	s->t = stmt_dup(s->op1.stval->t);
	return s;
}

stmt *stmt_join(stmt * op1, stmt * op2, comp_type cmptype)
{
	stmt *s = stmt_create();
	s->type = st_join;
	s->op1.stval = op1;
	s->op2.stval = op2;
	s->flag = cmptype;
	s->nrcols = 2;
	s->h = stmt_dup(op1->h);
	s->t = stmt_dup(op2->t);
	return s;
}

stmt *stmt_outerjoin(stmt * op1, stmt * op2, comp_type cmptype)
{
	stmt *s = stmt_create();
	s->type = st_outerjoin;
	s->op1.stval = op1;
	s->op2.stval = op2;
	s->flag = cmptype;
	s->nrcols = 2;
	s->h = stmt_dup(op1->h);
	s->t = stmt_dup(op2->t);
	return s;
}

stmt *stmt_semijoin(stmt * op1, stmt * op2)
{
	stmt *s = stmt_create();
	s->type = st_semijoin;
	s->op1.stval = op1;
	s->op2.stval = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->h = stmt_dup(op1->h);
	s->t = stmt_dup(op1->t);
	return s;
}

stmt *stmt_diff(stmt * op1, stmt * op2)
{
	stmt *s = stmt_create();
	s->type = st_diff;
	s->op1.stval = op1;
	s->op2.stval = op2;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->h = stmt_dup(op1->h);
	s->t = stmt_dup(op1->t);
	return s;
}

stmt *stmt_intersect(stmt * op1, stmt * op2)
{
	stmt *s = stmt_create();
	s->type = st_intersect;
	s->op1.stval = op1;
	if (op1->h != op2->h){
		s->op2.stval = stmt_reverse(op2);
	} else {
		s->op2.stval = op2;
	}
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->h = stmt_dup(op1->h);
	s->t = stmt_dup(op1->t);
	return s;
}

stmt *stmt_union(stmt * op1, stmt * op2)
{
	stmt *s = stmt_create();
	s->type = st_union;
	s->op1.stval = op1;
	s->op2.stval = op2;
	s->nrcols = op1->nrcols;
	s->h = stmt_dup(op1->h);
	s->t = stmt_dup(op1->t);
	return s;
}

stmt *stmt_copyfrom(table * t, list * files, char * tsep, char * rsep, int nr )
{
	stmt *s = stmt_create();
	s->type = st_copyfrom;
	s->op1.tval = t;
	s->op2.lval = list_create(&free);
	s->op3.lval = files;
	list_append( s->op2.lval, _strdup(tsep));
	list_append( s->op2.lval, _strdup(rsep));
	s->flag = nr; 
	return s;
}

list *stmt_copyfrom_files( stmt * s)
{
	while(s && s->type != st_copyfrom){
		s = s->op1.stval;
	}
	if (s){
		return s->op3.lval;
	}
	return NULL;
}

stmt *stmt_list(list * l)
{
	stmt *s = stmt_create();
	s->type = st_list;
	s->op1.lval = l;
	return s;
}

/* consuming stmt's */
stmt *stmt_ordered(stmt * order, stmt * res)
{
	stmt *ns = stmt_create();
	ns->type = st_ordered;
	ns->op1.stval = order;
	ns->op2.stval = res;
	ns->nrcols = res->nrcols;
	ns->key = res->key;
	ns->t = stmt_dup(res->t);
	return ns;
}

stmt *stmt_set(stmt * s1)
{
	stmt *s = stmt_create();
	s->type = st_set;
	s->op1.lval = list_append(list_create((fdestroy)&stmt_destroy),s1);
	return s;
}

stmt *stmt_output(stmt * l)
{
	stmt *s = stmt_create();
	s->type = st_output;
	s->op1.stval = l;
	return s;
}

stmt *stmt_result(stmt * l)
{
	stmt *s = stmt_create();
	s->type = st_result;
	s->op1.stval = l;
	return s;
}

stmt *stmt_sets(list * l1)
{
	stmt *s = stmt_create();
	s->type = st_sets;
	s->op1.lval = l1;
	return s;
}

stmt *stmt_insert(stmt * c, stmt * a, int unique_oids)
{
	stmt *s = stmt_create();
	s->type = st_insert;
	s->op1.stval = c;
	s->op2.stval = a;
	s->flag = unique_oids;
	s->h = stmt_dup(c->h);
	s->t = stmt_dup(c->t);
	return s;
}

stmt *stmt_replace(stmt * c, stmt * b)
{
	stmt *s = stmt_create();
	s->type = st_replace;
	s->op1.stval = c;
	s->op2.stval = b;
	s->nrcols = 1;
	return s;
}


stmt *stmt_delete(table * t, stmt * where)
{
	stmt *s = stmt_create();
	s->type = st_delete;
	s->op1.tval = t;
	s->op2.stval = where;
	return s;
}

stmt *stmt_unop(stmt * op1, sql_func * op)
{
	stmt *s = stmt_create();
	s->type = st_unop;
	s->op1.stval = op1;
	s->op2.funcval = op;
	s->h = stmt_dup(op1->h);
	s->nrcols = op1->nrcols;
	return s;
}

stmt *stmt_binop(stmt * op1, stmt * op2, sql_func * op)
{
	stmt *s = stmt_create();
	s->type = st_binop;
	s->op1.stval = op1;
	s->op2.stval = op2;
	s->op3.funcval = op;
	if (op1->nrcols > op2->nrcols)
		s->h = stmt_dup(op1->h);
	else
		s->h = stmt_dup(op2->h);
	s->nrcols = (op1->nrcols >= op2->nrcols) ? op1->nrcols : op2->nrcols;
	return s;
}

stmt *stmt_triop(stmt * op1, stmt * op2, stmt * op3, sql_func * op)
{
	stmt *s = stmt_create();
	s->type = st_triop;
	s->op1.lval = list_create((fdestroy)&stmt_destroy);
	list_append(s->op1.lval, op1); 
	list_append(s->op1.lval, op2); 
	list_append(s->op1.lval, op3); 
	s->op2.funcval = op;
	if (op1->nrcols > op2->nrcols)
		s->h = stmt_dup(op1->h);
	else
		s->h = stmt_dup(op2->h);
	s->nrcols = (op1->nrcols >= op2->nrcols) ? op1->nrcols : op2->nrcols;
	return s;
}

stmt *stmt_aggr(stmt * op1, sql_aggr * op, group * grp)
{
	stmt *s = stmt_create();
	s->type = st_aggr;
	s->op1.stval = op1;
	s->op2.aggrval = op;
	if (grp) {
		s->op3.gval = grp; 
		s->nrcols = 1;
		s->h = stmt_dup(grp->grp->h);
		s->key = 1;
	} else {
		s->nrcols = 1;	/* aggr's again lead to tables, 
				   with a single value */
		s->key = 1;
		s->h = stmt_dup(op1->h);
	}
	return s;
}

stmt *stmt_exists(stmt * op1, list * l)
{
	stmt *s = stmt_create();
	s->type = st_exists;
	s->op1.stval = op1;
	s->op2.lval = l;
	s->h = stmt_dup(op1->h);
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	return s;
}

stmt *stmt_alias(stmt * op1, char *alias)
{
	stmt *s = stmt_create();
	s->type = st_alias;
	s->op1.stval = op1;
	s->op2.sval = _strdup(alias);
	s->h = stmt_dup(op1->h);
	s->t = stmt_dup(op1->t);
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	return s;
}

stmt *stmt_column(stmt * op1, stmt *t, char *tname, char *cname)
{
	stmt *s = stmt_create();
	s->type = st_column_alias;
	s->op1.stval = op1;
	s->op2.sval = (tname)? _strdup(tname):NULL;
	s->op3.sval = (cname)? _strdup(cname):NULL;
	s->h = t;
	s->nrcols = 1; 
	s->key = op1->key;
	return s;
}

stmt *stmt_dup( stmt * s)
{
	if (s) s->refcnt++;
	return s;
}


stmt *stmt_push_down_head(stmt * join, stmt * select){
	if (join->type == st_join){
		stmt *op1 = stmt_semijoin( stmt_dup(join->op1.stval) ,select);
		stmt *res = stmt_join(op1, stmt_dup(join->op2.stval) , join->flag);
		stmt_destroy(join);
		return res;
	} else if (join->type == st_intersect){
		stmt *res = stmt_intersect(
			stmt_push_down_head( stmt_dup(join->op1.stval), select),
			stmt_push_down_head( stmt_dup(join->op2.stval), stmt_dup(select))
				);
		stmt_destroy(join);
		return res;
	} else if (join->type == st_reverse){
		stmt *res = stmt_reverse(
			stmt_push_down_tail( stmt_dup(join->op1.stval), 
				stmt_reverse(select)));
		stmt_destroy(join);
		return res;
	} else {
		printf("todo push down head %d\n", join->type);
	}
	stmt_destroy(select);
	return join;
}

stmt *stmt_push_down_tail(stmt * join, stmt * select){
	if (join->type == st_join){
		stmt *tail = stmt_reverse( stmt_dup(join->op2.stval));
		stmt *op2 = stmt_reverse( stmt_semijoin(tail, select));
		stmt *res = stmt_join( stmt_dup(join->op1.stval), op2, join->flag);
		stmt_destroy(join);
		return res;
	} else if (join->type == st_intersect){
		stmt *res = stmt_intersect(
			stmt_push_down_tail( stmt_dup(join->op1.stval), select),
			stmt_push_down_tail( stmt_dup(join->op2.stval), stmt_dup(select))
				);
		stmt_destroy(join);
		return res;
	} else if (join->type == st_reverse){
		stmt *res = stmt_reverse(
			stmt_push_down_head( stmt_dup(join->op1.stval), 
				stmt_reverse(select)));
		stmt_destroy(join);
		return res;
	} else {
		printf("todo push down tail %d\n", join->type);
	}
	stmt_destroy(select);
	return join;
}

stmt *stmt_push_join_head(stmt * s, stmt * join){
	if (s->type == st_join){
		stmt *op1 = stmt_join( join, stmt_dup(s->op1.stval), cmp_equal);
		stmt *res = stmt_join( op1, stmt_dup(s->op2.stval), s->flag );
		stmt_destroy(s);
		return res;
	} else if (s->type == st_reverse){
		stmt *res = stmt_reverse(
			stmt_push_join_tail( stmt_dup(s->op1.stval), stmt_reverse(join)));
		stmt_destroy(s);
		return res;
	} else if (s->type == st_intersect){
		stmt *res = stmt_intersect(
			stmt_push_join_head( stmt_dup(s->op1.stval), join),
			stmt_push_join_head( stmt_dup(s->op2.stval), stmt_dup(join))
				);
		stmt_destroy(s);
		return res;
	} else {
		printf("todo push join head %d\n", s->type);
	}
	stmt_destroy(join);
	return s;
}

stmt *stmt_push_join_tail(stmt * s, stmt * join){
	if (s->type == st_join){
		stmt *op2 = stmt_join( stmt_dup(s->op2.stval), join, cmp_equal);
		stmt *res = stmt_join( stmt_dup(s->op1.stval), op2, s->flag );
		stmt_destroy(s);
		return res;
	} else if (s->type == st_reverse){
		stmt *res = stmt_reverse(
			stmt_push_join_head(stmt_dup(s->op1.stval), stmt_reverse(join)));
		stmt_destroy(s);
		return res;
	} else if (s->type == st_intersect){
		stmt *res = stmt_intersect(
			stmt_push_join_tail(stmt_dup(s->op1.stval), join),
			stmt_push_join_tail(stmt_dup(s->op2.stval), stmt_dup(join))
				);
		stmt_destroy(s);
		return res;
	} else {
		printf("todo push join tail %d\n", s->type);
	}
	stmt_destroy(join);
	return s;
}

stmt *stmt_join2select(stmt * j){
	if (j->type == st_join){
		stmt *res = stmt_select(stmt_dup(j->op1.stval), 
				stmt_reverse(stmt_dup(j->op2.stval)), j->flag);
		stmt_destroy(j);
		return res;
	} else if (j->type == st_reverse){
		stmt *res = stmt_join2select(stmt_dup(j->op1.stval));
		stmt_destroy(j);
		return res;
	} else if (j->type == st_intersect){
		stmt * res = stmt_semijoin(
			stmt_join2select(stmt_dup(j->op1.stval)),
			stmt_join2select(stmt_dup(j->op2.stval)));
		stmt_destroy(j);
		return res;
	} else {
		printf("todo join2select %d\n", j->type);
	}
	return j;
}


sql_subtype *tail_type(stmt * st) 
{
	switch (st->type) {
	case st_const:
	case st_join:
	case st_outerjoin:
		return tail_type(st->op2.stval);

	case st_select:
	case st_select2:
	case st_unique:
	case st_union:
	case st_replace:
	case st_mark:
	case st_alias:
	case st_column_alias:
		return tail_type(st->op1.stval);

	case st_bat:
		return st->op1.cval->tpe;
	case st_reverse:
		return head_type(st->op1.stval);

	case st_aggr:
		return st->op2.aggrval->res;
	case st_unop:
		return st->op2.funcval->res;
	case st_binop:
		return st->op3.funcval->res;
	case st_triop:
		return st->op2.funcval->res;
	case st_atom:
		return atom_type(st->op1.aval);

	default:
		fprintf(stderr, "missing tail type %d\n", st->type );
		return NULL;
	}
}

sql_subtype *head_type(stmt * st)
{
	switch (st->type) {
	case st_aggr:
	case st_unop:
	case st_binop:
	case st_triop:
	case st_unique:
	case st_union:
	case st_alias:
	case st_column_alias:
	case st_join:
	case st_outerjoin:
	case st_semijoin:
	case st_select:
	case st_select2:
		return head_type(st->op1.stval);

	case st_mark:
	case st_bat:
		return NULL;	/* oid */

	case st_reverse:
		return tail_type(st->op1.stval);
	case st_atom:
		return atom_type(st->op1.aval);
	default:
		fprintf(stderr, "missing head type %d\n", st->type );
		return NULL;
	}
}

stmt *tail_column(stmt * st)
{
	switch (st->type) {
	case st_join:
	case st_outerjoin:
	case st_derive:
	case st_intersect:
		return tail_column(st->op2.stval);

	case st_mark:
	case st_unop:
	case st_binop:

	case st_like:
	case st_select:
	case st_select2:
	case st_semijoin:
	case st_atom:
	case st_alias:
	case st_column_alias:
	case st_group:
	case st_group_ext:
	case st_union:
	case st_unique:
		return tail_column(st->op1.stval);

	case st_bat:
		return st;

	case st_reverse:
		return head_column(st->op1.stval);

	case st_triop:
		return head_column(st->op1.lval->h->data );
	default:
		fprintf(stderr, "missing base column %d\n", st->type );
		assert(0);
		return NULL;
	}
}

stmt *head_column(stmt * st)
{
	switch (st->type) {
	case st_atom:
	case st_mark:
	case st_alias:
	case st_column_alias:
	case st_union:
	case st_unique:
	case st_aggr:
	case st_unop:
	case st_binop:
	case st_join:
	case st_outerjoin:
	case st_intersect:
	case st_semijoin:
	case st_like:
	case st_select:
	case st_select2:
		return head_column(st->op1.stval);
	case st_bat:
		return st;

	case st_group:
	case st_group_ext:
	case st_reverse:
		return tail_column(st->op1.stval);

	case st_derive:
		return tail_column(st->op2.stval);

	case st_triop:
		return head_column(st->op1.lval->h->data );
	default:
		fprintf(stderr, "missing base column %d\n", st->type );
		assert(0);
		return NULL;
	}
}

static char *aggr_name(char *n1, char *n2)
{
	int l1 = strlen(n1);
	int l2 = strlen(n2);
	char *ns = NEW_ARRAY(char, l1 + l2 + 2), *s = ns;
	strncpy(ns, n1, l1);
	ns += l1;
	*ns++ = '_';
	strncpy(ns, n2, l2);
	ns += l2;
	*ns = '\0';
	return s;
}

char *column_name(stmt * st)
{
	switch (st->type) {
	case st_reverse:
		return column_name(head_column(st->op1.stval));
	case st_join:
	case st_outerjoin:
		return column_name(st->op2.stval);
	case st_union:
	case st_mark:
	case st_select:
	case st_select2:
		return column_name(st->op1.stval);

	case st_bat:
		return st->op1.cval->name;
	case st_aggr:
		return aggr_name(st->op2.aggrval->name,
				 column_name(st->op1.stval));
	case st_alias:
		return st->op2.sval;
	case st_column_alias:
		return st->op3.sval;
	case st_unique:
		return column_name(st->op1.stval);
	default:
		fprintf(stderr, "missing name %d\n", st->type );
		return NULL;
	}
}
char *table_name(stmt * st)
{
	switch (st->type) {
	case st_reverse:
		return table_name(head_column(st->op1.stval));
	case st_join:
	case st_outerjoin:
		return table_name(st->op2.stval);
	case st_union:
	case st_mark:
	case st_select:
	case st_select2:
		return table_name(st->op1.stval);

	case st_bat:
		return st->op1.cval->table->name;

	case st_aggr:
		return table_name(st->op1.stval);
	case st_alias:
		return "unknown";

	case st_column_alias:
		return st->op2.sval;

	case st_unique:
		return table_name(st->op1.stval);
	default:
		fprintf(stderr, "missing name %d\n", st->type );
		return NULL;
	}
}

column *basecolumn(stmt * st)
{
	switch (st->type) {
	case st_reverse:
		return basecolumn(head_column(st->op1.stval));

	case st_bat:
		return st->op1.cval;

	default:
		fprintf(stderr, "missing column %d\n", st->type );
		return NULL;
	}
}

int stmt_cmp_nrcols( stmt *s, int *nr ){
	if (s->nrcols == *nr){
		return 0;
	}
	return -1;
}
