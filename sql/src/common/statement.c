
#include <string.h>
#include "mem.h"
#include "statement.h"

void st_attache( statement *st, statement *user ){
	if (user) list_append_statement(st->uses, user);
	st->refcnt++;
}


void st_detach( statement *st, statement *user ){
	if (user) list_remove_statement(st->uses, user);
	statement_destroy(st);
}

static
statement *statement_create(){
	statement *s = NEW(statement);
	s->type = st_none;
	s->op1.sval = NULL;
	s->op2.sval = NULL;
	s->op3.sval = NULL;
	s->flag = 0;
	s->nrcols = 0;
	s->nr = 0;
	s->h = NULL;
	s->t = NULL;
	s->refcnt = 1;
	s->uses = list_create();
	s->v.data = NULL;
	s->v.destroy = NULL;
	return s;
}

#define cr(c,v) case c: return v

const char *statement2string( statement *s ){
	if (!s) return "NULL";
	switch(s->type){
		cr(st_none,"NOP");
		cr(st_create_schema,"create_schema");
		cr(st_drop_schema,"drop_schema");
		cr(st_create_table,"create_table");
		cr(st_drop_table,"drop_table");
		cr(st_create_column,"create_column");
		cr(st_not_null,"not_null");
		cr(st_default,"default");
		cr(st_column,"column");
		cr(st_reverse,"reverse");
		cr(st_atom,"atom");
		cr(st_join,"join");
		cr(st_semijoin,"semijoin");
		cr(st_diff,"diff");
		cr(st_intersect,"intersect");
		cr(st_union,"union");
		cr(st_select,"select");
		cr(st_select2,"select2");
		cr(st_insert,"insert");
		cr(st_insert_column,"insert_column");
		cr(st_like,"like");
		cr(st_update,"update");
		cr(st_replace,"replace");
		cr(st_delete,"delete");
		cr(st_count,"count");
		cr(st_const,"const");
		cr(st_mark,"mark");
		cr(st_group,"group");
		cr(st_derive,"derive");
		cr(st_unique,"unique");
		cr(st_ordered,"ordered");
		cr(st_order,"order");
		cr(st_reorder,"reorder");
		cr(st_unop,"unop");
		cr(st_binop,"binop");
		cr(st_triop,"triop");
		cr(st_aggr,"aggr");
		cr(st_exists,"exists");
		cr(st_name,"name");
		cr(st_set,"set");
		cr(st_sets,"sets");
		cr(st_begin,"begin");
		cr(st_commit,"commit");
		cr(st_rollback,"rollback");
		cr(st_list,"list");
		cr(st_output,"output");
	}
	return "unknown";
}

void statement_destroy( statement *s ){
	assert (s->refcnt > 0);
	if (--s->refcnt == 0){
		switch(s->type){
			/* statement_destroy  op1 */
		case st_not_null: 
		case st_reverse: 
		case st_count: 
		case st_group: 
		case st_order: 
		case st_unop: 
		case st_name: 
		case st_output: 
			st_detach( s->op1.stval, s );
			break;
		case st_exists:
			st_detach( s->op1.stval, s );
			list_destroy( s->op2.lval );
			break;
			/* statement_destroy  op1 and op2 */
		case st_replace: 
		case st_default: 
		case st_like: 
		case st_semijoin: 
		case st_diff: 
		case st_intersect: 
		case st_union: 
		case st_join: 
		case st_const: 
		case st_derive: 
		case st_ordered: 
		case st_reorder: 
		case st_binop: 
		case st_insert_column: 
			st_detach( s->op1.stval, s );
			st_detach( s->op2.stval, s );
			break;
		case st_update: 
		case st_delete: 
			if (s->op2.stval)
				st_detach( s->op2.stval, s );
			break;
		case st_mark: 
		case st_unique: 
			st_detach( s->op1.stval, s );
			if (s->op2.stval)
				st_detach( s->op2.stval, s );
			break;
		case st_select: 
		case st_select2: 
			st_detach( s->op1.stval, s );
			st_detach( s->op2.stval, s );
			if (s->op3.stval)
				st_detach( s->op3.stval, s );
			break;
		case st_aggr: 
			st_detach( s->op1.stval, s );
			if (s->op3.stval)
				st_detach( s->op3.stval, s );
			break;
		case st_set: 
		case st_list: 
		case st_triop: 
			list_destroy( s->op1.lval );
			break;
		case st_insert: 
			list_destroy( s->op2.lval );
			break;
		case st_sets: {
			node *n = s->op1.lval->h;
			while(n){
				list_destroy( n->data.lval );
				n = n->next;
			}
		}	break;
		case st_atom: 
			atom_destroy( s->op1.aval );
			break;

		case st_create_schema: 
		case st_drop_schema: 

		case st_create_table: 
		case st_drop_table: 

		case st_begin:
		case st_commit:
		case st_rollback:
		case st_create_column: 
		case st_column:
		case st_none:
			break;
		}
		list_destroy(s->uses);
		_DELETE(s);
	}
}

statement *statement_begin( ){
	statement *s = statement_create();
	s->type = st_begin;
	return s;
}

statement *statement_commit( ){
	statement *s = statement_create();
	s->type = st_commit;
	return s;
}
statement *statement_rollback( ){
	statement *s = statement_create();
	s->type = st_rollback;
	return s;
}

statement *statement_create_schema( schema *schema ){
	statement *s = statement_create();
	s->type = st_create_schema;
	s->op1.schema = schema;
	return s;
}

statement *statement_drop_schema( schema *schema ){
	statement *s = statement_create();
	s->type = st_drop_schema;
	s->op1.schema = schema;
	return s;
}

statement *statement_create_table( table *t ){
	statement *s = statement_create();
	s->type = st_create_table;
	s->op1.tval = t;
	return s;
}

statement *statement_drop_table( table *t, int drop_action ){
	statement *s = statement_create();
	s->type = st_drop_table;
	s->op1.tval = t;
	s->flag = drop_action;
	return s;
}

statement *statement_create_column( column *c ){
	statement *s = statement_create();
	s->type = st_create_column;
	s->op1.cval = c;
	return s;
}

statement *statement_not_null( statement *col ){
	statement *s = statement_create();
	s->type = st_not_null;
	s->op1.stval = col; st_attache(col,s);
	return s;
}

statement *statement_default( statement *col, statement *def ){
	statement *s = statement_create();
	s->type = st_default;
	s->op1.stval = col; st_attache(col,s);
	s->op2.stval = def; st_attache(def,s);
	return s;
}

statement *statement_column( column *op1, var *basetable ){
	statement *s = statement_create();
	s->type = st_column;
	s->op1.cval = op1;
	s->nrcols = 1;

	if (basetable){
		s->h = basetable; basetable->refcnt++;
	}
	return s;
}

statement *statement_reverse( statement *s ){
	statement *ns = statement_create();
	ns->type = st_reverse;
	ns->op1.stval = s; st_attache(s,ns);
	ns->nrcols = s->nrcols;
	ns->h = s->t;
	ns->t = s->h;
	return ns;
}

statement *statement_count( statement *s ){
	statement *ns = statement_create();
	ns->type = st_count;
	ns->op1.stval = s; st_attache(s,ns);
	ns->nrcols = 0;
	ns->t = s->t;
	return ns;
}

statement *statement_const( statement *s, statement *val ){
	statement *ns = statement_create();
	ns->type = st_const;
	ns->op1.stval = s; st_attache(s,ns);
	ns->op2.stval = val; st_attache(val,ns);
	ns->nrcols = s->nrcols;
	ns->h = s->h;
	return ns;
}

statement *statement_mark( statement *s, int id ){
	statement *ns = statement_create();
	ns->type = st_mark;
	ns->op1.stval = s; st_attache(s,ns);
	ns->flag = id;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_remark( statement *s, statement *t, int id ){
	statement *ns = statement_create();
	ns->type = st_mark;
	ns->op1.stval = s; st_attache(s,ns);
	ns->op2.stval = t; st_attache(t,ns);
	ns->flag = id;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_group( statement *s ){
	statement *ns = statement_create();
	ns->type = st_group;
	ns->op1.stval = s; st_attache(s,ns);
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_derive( statement *s, statement *t ){
	statement *ns = statement_create();
	ns->type = st_derive;
	ns->op1.stval = s; st_attache(s,ns);
	ns->op2.stval = t; st_attache(t,ns);
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_unique( statement *s, statement *g ){
	statement *ns = statement_create();
	ns->type = st_unique;
	ns->op1.stval = s; st_attache(s,ns);
	if (g){
		ns->op2.stval = g; st_attache(g,ns);
	}
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_ordered( statement *order, statement *res ){
	statement *ns = statement_create();
	ns->type = st_ordered;
	ns->op1.stval = order; st_attache(order,ns);
	ns->op2.stval = res; st_attache(res,ns);
	ns->nrcols = res->nrcols;
	ns->t = res->t;
	return ns;
}

statement *statement_order( statement *s, int direction ){
	statement *ns = statement_create();
	ns->type = st_order;
	ns->op1.stval = s; st_attache(s,ns);
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_reorder( statement *s, statement *t, int direction ){
	statement *ns = statement_create();
	ns->type = st_reorder;
	ns->op1.stval = s; st_attache(s,ns);
	ns->op2.stval = t; st_attache(t,ns);
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_atom( atom *op1 ){
	statement *s = statement_create();
	s->type = st_atom;
	s->op1.aval = op1;
	return s;
}

statement *statement_select( statement *op1, statement *op2, comp_type cmptype){
	statement *s = statement_create();
	s->type = st_select;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = op2; st_attache(op2,s);
	assert(cmptype >= cmp_equal && cmptype <= cmp_gte );
	s->flag = cmptype;
	s->nrcols = 1;
	s->h = s->op1.stval->h;
	return s;
}

statement *statement_select2( statement *op1, statement *op2, 
			      statement *op3, int cmp ){
	statement *s = statement_create();
	s->type = st_select2;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = op2; st_attache(op2,s);
	s->op3.stval = op3; st_attache(op3,s);
	s->flag = cmp;
	s->nrcols = 1;
	s->h = s->op1.stval->h;
	return s;
}

statement *statement_like( statement *op1, statement *a ){
	statement *s = statement_create();
	s->type = st_like;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = a; st_attache(a,s);
	s->nrcols = 1;
	s->h = s->op1.stval->h;
	s->t = s->op1.stval->t;
	return s;
}

statement *statement_join( statement *op1, statement *op2, comp_type cmptype){
	statement *s = statement_create();
	s->type = st_join;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = op2; st_attache(op2,s);
	s->flag = cmptype;
	s->nrcols = 2; 
	s->h = op1->h;
	s->t = op2->t;
	return s;
}

statement *statement_semijoin( statement *op1, statement *op2 ){
	statement *s = statement_create();
	s->type = st_semijoin;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = op2; st_attache(op2,s);
	s->nrcols = op1->nrcols; 
	s->h = op1->h;
	s->t = op1->t;
	return s;
}
statement *statement_diff( statement *op1, statement *op2 ){
	statement *s = statement_create();
	s->type = st_diff;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = op2; st_attache(op2,s);
	s->nrcols = op1->nrcols; 
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

statement *statement_intersect( statement *op1, statement *op2 ){
	statement *s = statement_create();
	s->type = st_intersect;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = op2; st_attache(op2,s);
	s->nrcols = op1->nrcols; 
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

statement *statement_union( statement *op1, statement *op2 ){
	statement *s = statement_create();
	s->type = st_union;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = op2; st_attache(op2,s);
	s->nrcols = op1->nrcols; 
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

statement *statement_insert( table *t, list *l){
	statement *s = statement_create();
	s->type = st_insert;
	s->op1.tval = t;
	s->op2.lval = l;
	return s;
}
statement *statement_list( list *l){
	statement *s = statement_create();
	s->type = st_list;
	s->op1.lval = l;
	return s;
}

statement *statement_output( statement *l){
	statement *s = statement_create();
	s->type = st_output;
	s->op1.stval = l; st_attache(l,s);
	return s;
}

statement *statement_set( statement *s1){
	statement *s = statement_create();
	s->type = st_set;
	s->op1.lval = list_append_statement(list_create(), s1);
	return s;
}

statement *statement_sets( list *l1){
	statement *s = statement_create();
	s->type = st_sets;
	s->op1.lval = list_append_list(list_create(), l1);
	return s;
}

statement *statement_insert_column( statement *c, statement *a){
	statement *s = statement_create();
	s->type = st_insert_column;
	s->op1.stval = c; st_attache(c,s);
	s->op2.stval = a; st_attache(a,s);
	s->h = c->h;
	s->t = c->t;
	return s;
}

statement *statement_update( column *c, statement *b ){
	statement *s = statement_create();
	s->type = st_update;
	s->op1.cval = c; 
	s->op2.stval = b; st_attache(b,s);
	s->h = NULL;
	s->nrcols = 1;
	return s;
}

statement *statement_replace( statement *c, statement *b ){
	statement *s = statement_create();
	s->type = st_replace;
	s->op1.stval = c; st_attache(c,s);
	s->op2.stval = b; st_attache(b,s);
	s->h = c->h;
	s->nrcols = c->nrcols;
	return s;
}


statement *statement_delete( table *t, statement *where ){
	statement *s = statement_create();
	s->type = st_delete;
	s->op1.tval = t;
	s->op2.stval = where; st_attache(where,s);
	return s;
}

statement *statement_unop( statement *op1, func *op ){
	statement *s = statement_create();
	s->type = st_unop;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.funcval = op;
	s->h = op1->h;
	s->nrcols = op1->nrcols;
	return s;
}

statement *statement_binop( statement *op1, statement *op2, func *op ){
	statement *s = statement_create();
	s->type = st_binop;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.stval = op2; st_attache(op2,s);
	s->op3.funcval = op;
	if (op1->nrcols > op2->nrcols)
		s->h = op1->h;
	else
		s->h = op2->h;
	s->nrcols = (op1->nrcols >= op2->nrcols)?op1->nrcols:op2->nrcols;
	return s;
}
statement *statement_triop( statement *op1, statement *op2, statement *op3, func *op ){
	statement *s = statement_create();
	s->type = st_triop;
	s->op1.lval = list_create();
	list_append_statement(s->op1.lval, op1);
	list_append_statement(s->op1.lval, op2);
	list_append_statement(s->op1.lval, op3);
	s->op2.funcval = op;
	if (op1->nrcols > op2->nrcols)
		s->h = op1->h;
	else
		s->h = op2->h;
	s->nrcols = (op1->nrcols >= op2->nrcols)?op1->nrcols:op2->nrcols;
	return s;
}
statement *statement_aggr( statement *op1, aggr *op, statement *group ){
	statement *s = statement_create();
	s->type = st_aggr;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.aggrval = op;
	if (group){
		s->op3.stval = group; st_attache(group,s);
		s->nrcols = 1;
		s->h = group->h;
	} else {
		s->nrcols = 1; /* aggr's again lead to tables, 
				  	with a single value */
		s->h = op1->h;
	}
	return s;
}
statement *statement_exists( statement *op1, list *l ){
	statement *s = statement_create();
	s->type = st_exists;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.lval = l;
	s->h = op1->h;
	s->nrcols = op1->nrcols;
	return s;
}

statement *statement_name( statement *op1, char *name ){
	statement *s = statement_create();
	s->type = st_name;
	s->op1.stval = op1; st_attache(op1,s);
	s->op2.sval = _strdup(name);
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

type *tail_type( statement *st ){
	switch(st->type){
	case st_const: 
	case st_join: return tail_type(st->op2.stval);

	case st_select: 
	case st_select2: 
	case st_unique: 
	case st_union:
	case st_update: 
	case st_replace: 
	case st_mark: 
	case st_name: return tail_type(st->op1.stval);

	case st_column: return st->op1.cval->tpe; 
	case st_reverse: return head_type(st->op1.stval);

	case st_aggr: return st->op2.aggrval->res;
	case st_unop: return st->op2.funcval->res;
	case st_binop: return st->op3.funcval->res;
	case st_triop: return st->op2.funcval->res;
	case st_atom: return atom_type(st->op1.aval);

	default:
		fprintf( stderr, "missing tail type %d %s\n", 
				st->type, statement2string(st));
		return NULL;
	}
}

type *head_type( statement *st ){
	switch(st->type){
	case st_aggr:
	case st_unop: 
	case st_binop:
	case st_triop: 
	case st_unique:
	case st_union:
	case st_name: 
	case st_join: 
	case st_semijoin: 
	case st_select: 
	case st_select2: 
			return head_type(st->op1.stval);

	case st_mark: 
	case st_column: return NULL; /* oid */

	case st_reverse: return tail_type(st->op1.stval);
	case st_atom: return atom_type(st->op1.aval);
	default:
		fprintf( stderr, "missing head type %d %s\n", 
				st->type, statement2string(st));
		return NULL;
	}
}

statement *tail_column( statement *st ){
	switch(st->type){
	case st_join: 
	case st_derive: 
	case st_intersect: return tail_column(st->op2.stval);

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
	case st_union: 
	case st_unique: return tail_column(st->op1.stval);

	case st_column: return st; 

	case st_reverse: return head_column(st->op1.stval);

	case st_triop: return head_column(st->op1.lval->h->data.stval);
	default:
		fprintf( stderr, "missing base column %d %s\n", 
				st->type, statement2string(st));
		assert(0);
		return NULL;
	}
}

statement *head_column( statement *st ){
	switch(st->type){
	case st_atom: 
	case st_mark: 
	case st_name: 
	case st_union: 
	case st_unique: 
	case st_aggr: 
	case st_unop: 
	case st_binop: 
	case st_join: 
	case st_intersect: 
	case st_semijoin: 
	case st_like: 
	case st_select: 
	case st_select2: return head_column(st->op1.stval);
	case st_column: return st;

	case st_group: 
	case st_reverse: return tail_column(st->op1.stval);

	case st_derive: return tail_column(st->op2.stval);

	case st_triop: return head_column(st->op1.lval->h->data.stval);
	default:
		fprintf( stderr, "missing base column %d %s\n", 
				st->type, statement2string(st));
		assert(0);
		return NULL;
	}
}

static
char *aggr_name( char *n1, char *n2){
	int l1 = strlen(n1);
	int l2 = strlen(n2);
	char *ns = NEW_ARRAY( char, l1+l2+2), *s = ns;
	strncpy(ns, n1, l1 );
	ns += l1;
	*ns++ = '_';
	strncpy(ns, n2, l2 );
	ns += l2;
	*ns = '\0';
	return s;
}

char *column_name( statement *st ){
	switch(st->type){
	case st_reverse: return column_name(head_column(st->op1.stval));
	case st_join: return column_name(st->op2.stval);
	case st_union: 
	case st_mark: 
	case st_select: 
	case st_select2: return column_name(st->op1.stval);

	case st_column: return st->op1.cval->name;
	case st_aggr: return aggr_name( st->op2.aggrval->name, 
				column_name( st->op1.stval ));
	case st_name: return st->op2.sval;
	case st_unique: return column_name(st->op1.stval);
	default:
		fprintf( stderr, "missing name %d %s\n", 
				st->type, statement2string(st));
		return NULL;
	}
}

