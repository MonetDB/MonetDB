
#include "mem.h"
#include "statement.h"

static
statement *statement_create(){
	statement *s = NEW(statement);
	s->type = -1;
	s->op1.sval = NULL;
	s->op2.sval = NULL;
	s->op3.sval = NULL;
	s->flag = 0;
	s->nrcols = 0;
	s->nr = 0;
	s->h = NULL;
	s->t = NULL;
	s->refcnt = 0;
	s->v.data = NULL;
	s->v.destroy = NULL;
	return s;
}

#define cr(c,v) case c: return v

const char *statement2string( statement *s ){
	if (!s) return "NULL";
	switch(s->type){
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
		cr(st_diamond,"diamond");
		cr(st_pearl,"pearl");
		cr(st_list,"list");
		cr(st_insert_list,"insert_list");
		cr(st_output,"output");
	}
	return "unknown";
}

void statement_destroy( statement *s ){
	if (--s->refcnt <= 0){
		switch(s->type){
			/* statement_destroy  op1 */
		case st_not_null: 
		case st_reverse: 
		case st_count: 
		case st_group: 
		case st_unique: 
		case st_order: 
		case st_unop: 
		case st_name: 
		case st_output: 
			statement_destroy( s->op1.stval );
			break;
		case st_exists:
			statement_destroy( s->op1.stval );
			list_destroy( s->op2.lval );
			break;
			/* statement_destroy  op1 and op2 */
		case st_update: 
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
			statement_destroy( s->op1.stval );
			statement_destroy( s->op2.stval );
			break;
		case st_insert: 
			statement_destroy( s->op2.stval );
			if (s->op3.stval)
				statement_destroy( s->op3.stval );
			break;
		case st_delete: 
			if (s->op2.stval)
				statement_destroy( s->op2.stval );
			break;
		case st_mark: 
			statement_destroy( s->op1.stval );
			if (s->op2.stval)
				statement_destroy( s->op2.stval );
			break;
		case st_select: 
		case st_select2: 
			statement_destroy( s->op1.stval );
			statement_destroy( s->op2.stval );
			if (s->op3.stval)
				statement_destroy( s->op3.stval );
			break;
		case st_aggr: 
			statement_destroy( s->op1.stval );
			if (s->op3.stval)
				statement_destroy( s->op3.stval );
			break;
		case st_diamond: 
		case st_list: 
		case st_insert_list: 
		case st_triop: 
			list_destroy( s->op1.lval );
			break;
		case st_pearl: {
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

		case st_create_column: 
		case st_column:

			break;
		}
		_DELETE(s);
	}
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
	s->op1.stval = col; col->refcnt++;
	return s;
}

statement *statement_default( statement *col, statement *def ){
	statement *s = statement_create();
	s->type = st_default;
	s->op1.stval = col; col->refcnt++;
	s->op2.stval = def; def->refcnt++;
	return s;
}

statement *statement_column( column *op1, var *basetable ){
	statement *s = statement_create();
	s->type = st_column;
	s->op1.cval = op1;
	s->nrcols = 1;
	s->h = basetable;
	return s;
}

statement *statement_reverse( statement *s ){
	statement *ns = statement_create();
	ns->type = st_reverse;
	ns->op1.stval = s; s->refcnt++;
	ns->nrcols = s->nrcols;
	ns->h = s->t;
	ns->t = s->h;
	return ns;
}

statement *statement_count( statement *s ){
	statement *ns = statement_create();
	ns->type = st_count;
	ns->op1.stval = s; s->refcnt++;
	ns->nrcols = 0;
	ns->t = s->t;
	return ns;
}

statement *statement_const( statement *s, statement *val ){
	statement *ns = statement_create();
	ns->type = st_const;
	ns->op1.stval = s; s->refcnt++;
	ns->op2.stval = val; val->refcnt++;
	ns->nrcols = s->nrcols;
	ns->h = s->h;
	return ns;
}

statement *statement_mark( statement *s, int id ){
	statement *ns = statement_create();
	ns->type = st_mark;
	ns->op1.stval = s; s->refcnt++;
	ns->flag = id;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_remark( statement *s, statement *t, int id ){
	statement *ns = statement_create();
	ns->type = st_mark;
	ns->op1.stval = s; s->refcnt++;
	ns->op2.stval = t; t->refcnt++;
	ns->flag = id;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_group( statement *s ){
	statement *ns = statement_create();
	ns->type = st_group;
	ns->op1.stval = s; s->refcnt++;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_derive( statement *s, statement *t ){
	statement *ns = statement_create();
	ns->type = st_derive;
	ns->op1.stval = s; s->refcnt++;
	ns->op2.stval = t; t->refcnt++;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_unique( statement *s ){
	statement *ns = statement_create();
	ns->type = st_unique;
	ns->op1.stval = s; s->refcnt++;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_ordered( statement *order, statement *res ){
	statement *ns = statement_create();
	ns->type = st_ordered;
	ns->op1.stval = order; order->refcnt++;
	ns->op2.stval = res; res->refcnt++;
	ns->nrcols = res->nrcols;
	ns->t = res->t;
	return ns;
}

statement *statement_order( statement *s, int direction ){
	statement *ns = statement_create();
	ns->type = st_order;
	ns->op1.stval = s; s->refcnt++;
	ns->flag = direction;
	ns->nrcols = s->nrcols;
	ns->t = s->t;
	return ns;
}

statement *statement_reorder( statement *s, statement *t, int direction ){
	statement *ns = statement_create();
	ns->type = st_reorder;
	ns->op1.stval = s; s->refcnt++;
	ns->op2.stval = t; t->refcnt++;
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
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = op2; op2->refcnt++;
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
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = op2; op2->refcnt++;
	s->op3.stval = op3; op3->refcnt++;
	s->flag = cmp;
	s->nrcols = 1;
	s->h = s->op1.stval->h;
	return s;
}

statement *statement_like( statement *op1, statement *a ){
	statement *s = statement_create();
	s->type = st_like;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = a; a->refcnt++;
	s->nrcols = 1;
	s->h = s->op1.stval->h;
	s->t = s->op1.stval->t;
	return s;
}

statement *statement_join( statement *op1, statement *op2, comp_type cmptype){
	statement *s = statement_create();
	s->type = st_join;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = op2; op2->refcnt++;
	s->flag = cmptype;
	s->nrcols = 2; 
	s->h = op1->h;
	s->t = op2->t;
	return s;
}

statement *statement_semijoin( statement *op1, statement *op2 ){
	statement *s = statement_create();
	s->type = st_semijoin;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = op2; op2->refcnt++;
	s->nrcols = op1->nrcols; 
	s->h = op1->h;
	s->t = op1->t;
	return s;
}
statement *statement_diff( statement *op1, statement *op2 ){
	statement *s = statement_create();
	s->type = st_diff;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = op2; op2->refcnt++;
	s->nrcols = op1->nrcols; 
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

statement *statement_intersect( statement *op1, statement *op2 ){
	statement *s = statement_create();
	s->type = st_intersect;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = op2; op2->refcnt++;
	s->nrcols = op1->nrcols; 
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

statement *statement_union( statement *op1, statement *op2 ){
	statement *s = statement_create();
	s->type = st_union;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = op2; op2->refcnt++;
	s->nrcols = op1->nrcols; 
	s->h = op1->h;
	s->t = op1->t;
	return s;
}

statement *statement_insert_list( list *l){
	statement *s = statement_create();
	s->type = st_insert_list;
	s->op1.lval = l;
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
	s->op1.stval = l; l->refcnt++;
	return s;
}

statement *statement_diamond( statement *s1){
	statement *s = statement_create();
	s->type = st_diamond;
	s->op1.lval = list_append_statement(list_create(), s1);
	return s;
}

statement *statement_pearl( list *l1){
	statement *s = statement_create();
	s->type = st_pearl;
	s->op1.lval = list_append_list(list_create(), l1);
	return s;
}

statement *statement_insert( column *c, statement *id, statement *v){
	statement *s = statement_create();
	s->type = st_insert;
	s->op1.cval = c;
	s->op2.stval = id; id->refcnt++;
	if (v){
		s->op3.stval = v; v->refcnt++;
	}
	return s;
}

statement *statement_insert_column( statement *c, statement *a){
	statement *s = statement_create();
	s->type = st_insert_column;
	s->op1.stval = c; c->refcnt++;
	s->op2.stval = a; a->refcnt++;
	s->h = c->h;
	s->t = c->t;
	return s;
}

statement *statement_update( statement *c, statement *b ){
	statement *s = statement_create();
	s->type = st_update;
	s->op1.stval = c; c->refcnt++;
	s->op2.stval = b; b->refcnt++;
	s->h = c->h;
	s->nrcols = c->nrcols;
	return s;
}

statement *statement_delete( column *c, statement *where ){
	statement *s = statement_create();
	s->type = st_delete;
	s->op1.cval = c;
	s->op2.stval = where; where->refcnt++;
	return s;
}

statement *statement_unop( statement *op1, func *op ){
	statement *s = statement_create();
	s->type = st_unop;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.funcval = op;
	s->h = op1->h;
	s->nrcols = op1->nrcols;
	return s;
}

statement *statement_binop( statement *op1, statement *op2, func *op ){
	statement *s = statement_create();
	s->type = st_binop;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.stval = op2; op2->refcnt++;
	s->op3.funcval = op;
	s->h = op1->h;
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
	s->h = op1->h;
	s->nrcols = (op1->nrcols >= op2->nrcols)?op1->nrcols:op2->nrcols;
	return s;
}
statement *statement_aggr( statement *op1, aggr *op, statement *group ){
	statement *s = statement_create();
	s->type = st_aggr;
	s->op1.stval = op1; op1->refcnt++;
	s->op2.aggrval = op;
	if (group){
		s->op3.stval = group; group->refcnt++;
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
	s->op1.stval = op1; op1->refcnt++;
	s->op2.lval = l;
	s->h = op1->h;
	s->nrcols = op1->nrcols;
	return s;
}

statement *statement_name( statement *op1, char *name ){
	statement *s = statement_create();
	s->type = st_name;
	s->op1.stval = op1; op1->refcnt++;
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
	case st_delete: 
	case st_insert: 
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

static 
statement *_basecolumn( statement *st ){
	switch(st->type){
	case st_join: 
	case st_intersect: return _basecolumn(st->op2.stval);
	case st_select: return _basecolumn(st->op1.stval);
	case st_select2: return _basecolumn(st->op1.stval);
	case st_column: return st; 
	case st_reverse: return basecolumn(st->op1.stval);
	case st_unop: return basecolumn(st->op1.stval);
	case st_binop: return basecolumn(st->op1.stval);
	case st_triop: return basecolumn(st->op1.lval->h->data.stval);
	case st_atom: 
	case st_name: return _basecolumn(st->op1.stval);
	case st_unique: return _basecolumn(st->op1.stval);
	case st_mark: return basecolumn(st->op1.stval);
	default:
		assert(0);
		fprintf( stderr, "missing base column %d %s\n", 
				st->type, statement2string(st));
		return NULL;
	}
}

statement *basecolumn( statement *st ){
	switch(st->type){
	case st_join: 
	case st_intersect: return basecolumn(st->op1.stval);
	case st_select: return basecolumn(st->op1.stval);
	case st_select2: return basecolumn(st->op1.stval);
	case st_column: return st;
	case st_reverse: return _basecolumn(st->op1.stval);
	case st_aggr: return basecolumn(st->op1.stval);
	case st_unop: return basecolumn(st->op1.stval);
	case st_binop: return basecolumn(st->op1.stval);
	case st_triop: return basecolumn(st->op1.lval->h->data.stval);
	case st_atom: 
	case st_name: return basecolumn(st->op1.stval);
	case st_unique: return basecolumn(st->op1.stval);
	case st_mark: return basecolumn(st->op1.stval);
	default:
		assert(0);
		fprintf( stderr, "missing base column %d %s\n", 
				st->type, statement2string(st));
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
	case st_join: return column_name(st->op2.stval);
	case st_union: 
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

