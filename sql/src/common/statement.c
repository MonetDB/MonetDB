
#include <string.h>
#include "mem.h"
#include "statement.h"

/* todo make proper traversal operations */

void st_attache(stmt * st, stmt * user)
{
	if (user)
		list_append(st->uses, user);
	st->refcnt++;
}


void st_detach(stmt * st, stmt * user)
{
	if (user)
		list_remove_data(st->uses, user);
	stmt_destroy(st);
}

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
	s->uses = list_create((fdestroy)&stmt_destroy);
	return s;
}

static stmt *stmt_ext( stmt *grp )
{
	stmt *ns = stmt_create();

	ns->type = st_group_ext;
	ns->op1.stval = grp;
	st_attache(grp, ns);
	ns->nrcols = grp->nrcols;
	ns->key = 1;
	ns->t = grp->t;
	return ns;
}

static stmt *stmt_group(stmt * s)
{
	stmt *ns = stmt_create();
	ns->type = st_group;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->nrcols = s->nrcols;
	ns->key = 0;
	ns->t = s->t;
	return ns;
}

static stmt *stmt_derive(stmt * s, stmt * t)
{
	stmt *ns = stmt_create();
	ns->type = st_derive;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->op2.stval = t;
	st_attache(t, ns);
	ns->nrcols = s->nrcols;
	ns->key = 0;
	ns->t = s->t;
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
		st_detach(g->grp, NULL);
		st_detach(g->ext, NULL);
		_DELETE(g);
	}
}

group *grp_create( stmt *s, group *og )
{
	group *g = NEW(group);
	if (og){
		g->grp = stmt_derive(og->grp, s);
		st_attache(g->grp, NULL);
	} else {
		g->grp = stmt_group(s);
		st_attache(g->grp, NULL);
	}
	g->ext = stmt_ext(g->grp);
	st_attache(g->ext, NULL);
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
	g->grp = stmt_semijoin_tail(og->grp,s);
	st_attache(g->grp, NULL);
	g->ext = stmt_semijoin(og->ext,s);
	st_attache(g->ext, NULL);
	g->refcnt = 1;
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
		case st_name:
		case st_output:
		case st_result:
			st_detach(s->op1.stval, s);
			break;
		case st_drop_table:
			st_detach(s->op1.stval, s);
			_DELETE(s->op2.sval);
			break;
		case st_exists:
			st_detach(s->op1.stval, s);
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
		case st_add_col:
			st_detach(s->op1.stval, s);
			st_detach(s->op2.stval, s);
			break;
		case st_create_key:
			st_detach(s->op1.stval, s);
			if (s->op2.stval)
				st_detach(s->op2.stval, s);
			break;
		case st_delete:
			if (s->op2.stval)
				st_detach(s->op2.stval, s);
			break;
		case st_mark:
		case st_unique:
			st_detach(s->op1.stval, s);
			if (s->op2.gval)
				grp_destroy(s->op2.gval);
			break;
		case st_select:
		case st_select2:
			st_detach(s->op1.stval, s);
			st_detach(s->op2.stval, s);
			if (s->op3.stval)
				st_detach(s->op3.stval, s);
			break;
		case st_aggr:
			st_detach(s->op1.stval, s);
			if (s->op3.gval)
				grp_destroy(s->op3.gval);
			break;
		case st_set:
		case st_list:
		case st_triop:
			list_destroy(s->op1.lval);
			break;
		case st_sets:
			{
				node *n = s->op1.lval->h;
				while (n) {
					list_destroy(n->data);
					n = n->next;
				}
			}
			break;
		case st_atom:
			atom_destroy(s->op1.aval);
			break;

		case st_schema: case st_create_schema: case st_drop_schema:
		case st_bat:

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
			break;
		}
		list_destroy(s->uses);
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
	case st_order: case st_unop: case st_name: case st_output: 
	case st_result: case st_exists: 
	case st_table: case st_create_table: case st_drop_table: 

		stmt_reset(s->op1.stval);
		break;

	case st_default: case st_like: case st_semijoin: 
	case st_diff: case st_intersect: case st_union: case st_join: 
	case st_outerjoin: 
	case st_const: case st_derive: case st_ordered: case st_reorder: 
	case st_binop: case st_insert: case st_add_col:
	case st_replace: 

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
		stmt_reset(s->op1.stval);
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
	for (n = s->uses->h; n; n = n->next)
		stmt_reset( n->data );
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
	st_attache(schema, s);
	s->op2.tval = t;
	return s;
}

stmt *stmt_bind_column(stmt *table, column * c)
{
	stmt *s = c->s;
	if (s){
		st_attache(s, NULL);
	} else {
		s = stmt_create();
		s->type = st_column;
		s->op1.stval = table;
		st_attache(table, s);
		s->op2.cval = c;
	}
	return s;
}

stmt *stmt_bind_key(stmt *table, key * k)
{
	stmt *s = stmt_create();
	s->type = st_key;
	s->op1.stval = table;
	st_attache(table, s);
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

stmt *stmt_drop_schema(schema * schema)
{
	stmt *s = stmt_create();
	s->type = st_drop_schema;
	s->op1.schema = schema;
	return s;
}

stmt *stmt_create_table(stmt *schema, table * t)
{
	stmt *s = stmt_create();
	s->type = st_create_table;
	s->op1.stval = schema;
	st_attache(schema, s);
	s->op2.tval = t;
	return s;
}

stmt *stmt_drop_table(stmt * schema, char *name, int drop_action)
{
	stmt *s = stmt_create();
	s->type = st_drop_table;
	s->op1.stval = schema;
	st_attache(schema, s);
	s->op2.sval = _strdup(name);
	s->flag = drop_action;
	return s;
}

stmt *stmt_create_column(stmt *table, column * c)
{
	stmt *s = stmt_create();
	s->type = st_create_column;
	s->op1.stval = table;
	st_attache(table, s);
	s->op2.cval = c;
	return s;
}

stmt *stmt_not_null(stmt * col)
{
	stmt *s = stmt_create();
	s->type = st_not_null;
	s->op1.stval = col;
	st_attache(col, s);
	return s;
}

stmt *stmt_default(stmt * col, stmt * def)
{
	stmt *s = stmt_create();
	s->type = st_default;
	s->op1.stval = col;
	st_attache(col, s);
	s->op2.stval = def;
	st_attache(def, s);
	return s;
}

stmt *stmt_cbat(column * op1, tvar * basetable, int access, int type)
{
	stmt *s = stmt_create();
	s->type = type;
	s->op1.cval = op1;
	s->nrcols = 1;
	s->flag = access;

	if (basetable) {
		s->h = basetable;
		basetable->refcnt++;
	}
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

stmt *stmt_reverse(stmt * s)
{
	stmt *ns = stmt_create();
	ns->type = st_reverse;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->h = s->t;
	ns->t = s->h;
	return ns;
}

stmt *stmt_count(stmt * s)
{
	stmt *ns = stmt_create();
	ns->type = st_count;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->nrcols = 0;
	ns->t = s->t;
	return ns;
}

stmt *stmt_const(stmt * s, stmt * val)
{
	stmt *ns = stmt_create();
	ns->type = st_const;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->op2.stval = val;
	st_attache(val, ns);
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->h = s->h;
	return ns;
}

stmt *stmt_mark(stmt * s, int id)
{
	stmt *ns = stmt_create();
	ns->type = st_mark;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->flag = id;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->t = s->t;
	return ns;
}

stmt *stmt_remark(stmt * s, stmt * t, int id)
{
	stmt *ns = stmt_create();
	ns->type = st_mark;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->op2.stval = t;
	st_attache(t, ns);
	ns->flag = id;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->t = s->t;
	return ns;
}

stmt *stmt_unique(stmt * s, group * g)
{
	stmt *ns = stmt_create();
	ns->type = st_unique;
	ns->op1.stval = s;
	st_attache(s, ns);
	if (g) {
		ns->op2.gval = g;
		g->refcnt++;
	}
	ns->nrcols = s->nrcols;
	ns->key = 1; /* ?? maybe change key to unique ? */
	ns->t = s->t;
	return ns;
}

stmt *stmt_ordered(stmt * order, stmt * res)
{
	stmt *ns = stmt_create();
	ns->type = st_ordered;
	ns->op1.stval = order;
	st_attache(order, ns);
	ns->op2.stval = res;
	st_attache(res, ns);
	ns->nrcols = res->nrcols;
	ns->key = res->key;
	ns->t = res->t;
	return ns;
}

stmt *stmt_order(stmt * s, int direction)
{
	stmt *ns = stmt_create();
	ns->type = st_order;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->t = s->t;
	return ns;
}

stmt *stmt_reorder(stmt * s, stmt * t, int direction)
{
	stmt *ns = stmt_create();
	ns->type = st_reorder;
	ns->op1.stval = s;
	st_attache(s, ns);
	ns->op2.stval = t;
	st_attache(t, ns);
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->key = s->key;
	ns->t = s->t;
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
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	assert(cmptype >= cmp_equal && cmptype <= cmp_gte);
	s->flag = cmptype;
	s->nrcols = 1;
	s->h = s->op1.stval->h;
	return s;
}

stmt *stmt_select2(stmt * op1, stmt * op2,
			     stmt * op3, int cmp)
{
	stmt *s = stmt_create();
	s->type = st_select2;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	s->op3.stval = op3;
	st_attache(op3, s);
	s->flag = cmp;
	s->nrcols = 1;
	s->h = s->op1.stval->h;
	return s;
}

stmt *stmt_like(stmt * op1, stmt * a)
{
	stmt *s = stmt_create();
	s->type = st_like;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = a;
	st_attache(a, s);
	s->nrcols = 1;
	s->h = s->op1.stval->h;
	s->t = s->op1.stval->t;
	return s;
}

stmt *stmt_join(stmt * op1, stmt * op2, comp_type cmptype)
{
	stmt *s = stmt_create();
	s->type = st_join;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	s->flag = cmptype;
	s->nrcols = 2;
	s->h = op1->h;
	s->t = op2->t;
	return s;
}

stmt *stmt_outerjoin(stmt * op1, stmt * op2, comp_type cmptype)
{
	stmt *s = stmt_create();
	s->type = st_outerjoin;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	s->flag = cmptype;
	s->nrcols = 2;
	s->h = op1->h;
	s->t = op2->t;
	return s;
}

stmt *stmt_semijoin(stmt * op1, stmt * op2)
{
	stmt *s = stmt_create();
	s->type = st_semijoin;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

stmt *stmt_push_down_head(stmt * join, stmt * select){
	if (join->type == st_join){
		stmt *op1 = stmt_semijoin(join->op1.stval,select);
		return stmt_join(op1,join->op2.stval, join->flag);
	} else if (join->type == st_intersect){
		return stmt_intersect(
			stmt_push_down_head(join->op1.stval, select),
			stmt_push_down_head(join->op2.stval, select)
				);
	} else if (join->type == st_reverse){
		return stmt_reverse(
			stmt_push_down_tail(join->op1.stval, 
				stmt_reverse(select)));
	} else {
		printf("todo push down head %d\n", join->type);
	}
	return join;
}

stmt *stmt_push_down_tail(stmt * join, stmt * select){
	if (join->type == st_join){
		stmt *tail = stmt_reverse(join->op2.stval);
		stmt *op2 = stmt_reverse(stmt_semijoin(tail,select));
		return stmt_join(join->op1.stval,op2, join->flag);
	} else if (join->type == st_intersect){
		return stmt_intersect(
			stmt_push_down_tail(join->op1.stval, select),
			stmt_push_down_tail(join->op2.stval, select)
				);
	} else if (join->type == st_reverse){
		return stmt_reverse(
			stmt_push_down_head(join->op1.stval, 
				stmt_reverse(select)));
	} else {
		printf("todo push down tail %d\n", join->type);
	}
	return join;
}

stmt *stmt_push_join_head(stmt * s, stmt * join){
	if (s->type == st_join){
		stmt *op1 = stmt_join(join,s->op1.stval,cmp_equal);
		return stmt_join( op1, s->op2.stval, s->flag );
	} else if (s->type == st_reverse){
		return stmt_reverse(
			stmt_push_join_tail(s->op1.stval, stmt_reverse(join)));
	} else if (s->type == st_intersect){
		return stmt_intersect(
			stmt_push_join_head(s->op1.stval, join),
			stmt_push_join_head(s->op2.stval, join)
				);
	} else {
		printf("todo push join head %d\n", s->type);
	}
	return s;
}

stmt *stmt_push_join_tail(stmt * s, stmt * join){
	if (s->type == st_join){
		stmt *op2 = stmt_join(s->op2.stval,join,cmp_equal);
		return stmt_join( s->op1.stval, op2, s->flag );
	} else if (s->type == st_reverse){
		return stmt_reverse(
			stmt_push_join_head(s->op1.stval, stmt_reverse(join)));
	} else if (s->type == st_intersect){
		return stmt_intersect(
			stmt_push_join_tail(s->op1.stval, join),
			stmt_push_join_tail(s->op2.stval, join)
				);
	} else {
		printf("todo push join tail %d\n", s->type);
	}
	return s;
}

stmt *stmt_join2select(stmt * j){
	if (j->type == st_join){
		return stmt_select(j->op1.stval, 
				stmt_reverse(j->op2.stval), j->flag);
	} else if (j->type == st_reverse){
		return stmt_join2select(j->op1.stval);
	} else if (j->type == st_intersect){
		return stmt_semijoin(
			stmt_join2select(j->op1.stval),
			stmt_join2select(j->op2.stval));
	} else {
		printf("todo join2select %d\n", j->type);
	}
	return j;
}

stmt *stmt_diff(stmt * op1, stmt * op2)
{
	stmt *s = stmt_create();
	s->type = st_diff;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

stmt *stmt_intersect(stmt * op1, stmt * op2)
{
	stmt *s = stmt_create();
	s->type = st_intersect;
	if (op1->h != op2->h){
		op2 = stmt_reverse(op2);
	}
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

stmt *stmt_union(stmt * op1, stmt * op2)
{
	stmt *s = stmt_create();
	s->type = st_union;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	s->nrcols = op1->nrcols;
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

stmt *stmt_copyfrom(table * t, char * file, char * tsep, char * rsep, int nr )
{
	stmt *s = stmt_create();
	s->type = st_copyfrom;
	s->op1.tval = t;
	s->op2.lval = list_create(&free);
	list_append( s->op2.lval, _strdup(file));
	list_append( s->op2.lval, _strdup(tsep));
	list_append( s->op2.lval, _strdup(rsep));
	s->flag = nr; 
	return s;
}

char *stmt_copyfrom_file( stmt * s)
{
	while(s && s->type != st_copyfrom){
		s = s->op1.stval;
	}
	if (s){
		return s->op2.lval->h->data;
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

stmt *stmt_output(stmt * l)
{
	stmt *s = stmt_create();
	s->type = st_output;
	s->op1.stval = l;
	st_attache(l, s);
	return s;
}

stmt *stmt_result(stmt * l)
{
	stmt *s = stmt_create();
	s->type = st_result;
	s->op1.stval = l;
	st_attache(l, s);
	return s;
}

stmt *stmt_dup( stmt * s)
{
	st_attache(s, NULL);
	return s;
}

stmt *stmt_set(stmt * s1)
{
	stmt *s = stmt_create();
	s->type = st_set;
	s->op1.lval = list_append(list_create((fdestroy)&stmt_destroy),s1);
	st_attache(s1,NULL);
	return s;
}

stmt *stmt_sets(list * l1)
{
	stmt *s = stmt_create();
	s->type = st_sets;
	s->op1.lval = l1;
	return s;
}

stmt *stmt_insert(stmt * c, stmt * a)
{
	stmt *s = stmt_create();
	s->type = st_insert;
	s->op1.stval = c;
	st_attache(c, s);
	s->op2.stval = a;
	st_attache(a, s);
	s->h = c->h;
	s->t = c->t;
	return s;
}

stmt *stmt_replace(stmt * c, stmt * b)
{
	stmt *s = stmt_create();
	s->type = st_replace;
	s->op1.stval = c;
	st_attache(c, s);
	s->op2.stval = b;
	st_attache(b, s);
	s->h = NULL;
	s->nrcols = 1;
	return s;
}


stmt *stmt_delete(table * t, stmt * where)
{
	stmt *s = stmt_create();
	s->type = st_delete;
	s->op1.tval = t;
	s->op2.stval = where;
	st_attache(where, s);
	return s;
}

stmt *stmt_unop(stmt * op1, sql_func * op)
{
	stmt *s = stmt_create();
	s->type = st_unop;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.funcval = op;
	s->h = op1->h;
	s->nrcols = op1->nrcols;
	return s;
}

stmt *stmt_binop(stmt * op1, stmt * op2, sql_func * op)
{
	stmt *s = stmt_create();
	s->type = st_binop;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.stval = op2;
	st_attache(op2, s);
	s->op3.funcval = op;
	if (op1->nrcols > op2->nrcols)
		s->h = op1->h;
	else
		s->h = op2->h;
	s->nrcols =
	    (op1->nrcols >= op2->nrcols) ? op1->nrcols : op2->nrcols;
	return s;
}

stmt *stmt_triop(stmt * op1, stmt * op2, stmt * op3, sql_func * op)
{
	stmt *s = stmt_create();
	s->type = st_triop;
	s->op1.lval = list_create((fdestroy)&stmt_destroy);
	list_append(s->op1.lval, op1); st_attache(op1,NULL);
	list_append(s->op1.lval, op2); st_attache(op2,NULL);
	list_append(s->op1.lval, op3); st_attache(op3,NULL);
	s->op2.funcval = op;
	if (op1->nrcols > op2->nrcols)
		s->h = op1->h;
	else
		s->h = op2->h;
	s->nrcols =
	    (op1->nrcols >= op2->nrcols) ? op1->nrcols : op2->nrcols;
	return s;
}

stmt *stmt_aggr(stmt * op1, sql_aggr * op, group * grp)
{
	stmt *s = stmt_create();
	s->type = st_aggr;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.aggrval = op;
	if (grp) {
		s->op3.gval = grp; grp->refcnt++;
		s->nrcols = 1;
		s->h = grp->grp->h;
		s->key = 1;
	} else {
		s->nrcols = 1;	/* aggr's again lead to tables, 
				   with a single value */
		s->key = 1;
		s->h = op1->h;
	}
	return s;
}

stmt *stmt_exists(stmt * op1, list * l)
{
	stmt *s = stmt_create();
	s->type = st_exists;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.lval = l;
	s->h = op1->h;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	return s;
}

stmt *stmt_key( stmt * t, key_type kt, stmt *rk )
{
	stmt *s = stmt_create();
	s->type = st_create_key;
	s->op1.stval = t; 
	st_attache(t, s);
	if (rk){
		s->op2.stval = rk; 
		st_attache(rk, s);
	}
	s->flag = kt;
	return s;
}

stmt *stmt_key_add_column( stmt * key, stmt *col )
{
	stmt *s = stmt_create();
	s->type = st_add_col;
	s->op1.stval = key;
	st_attache(key, s);
	s->op2.stval = col;
	st_attache(col, s);
	s->flag = ukey;
	return s;
}

stmt *stmt_name(stmt * op1, char *name)
{
	stmt *s = stmt_create();
	s->type = st_name;
	s->op1.stval = op1;
	st_attache(op1, s);
	s->op2.sval = _strdup(name);
	s->h = op1->h;
	s->t = op1->t;
	s->nrcols = op1->nrcols;
	s->key = op1->key;
	return s;
}

sql_type *tail_type(stmt * st) 
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
	case st_name:
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

sql_type *head_type(stmt * st)
{
	switch (st->type) {
	case st_aggr:
	case st_unop:
	case st_binop:
	case st_triop:
	case st_unique:
	case st_union:
	case st_name:
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
	case st_name:
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
	case st_name:
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
	case st_name:
		return st->op2.sval;
	case st_unique:
		return column_name(st->op1.stval);
	default:
		fprintf(stderr, "missing name %d\n", st->type );
		return NULL;
	}
}

int stmt_cmp_nrcols( stmt *s, int *nr ){
	if (s->nrcols == *nr){
		return 0;
	}
	return -1;
}
