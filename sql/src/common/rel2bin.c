
#include "rel2bin.h"
#include "statement.h"
#include <stdio.h>

#define create_stmt_list() list_create((fdestroy)&stmt_destroy)

stmt *rel2bin( context *c, stmt *s ){
	stmt *res = s;

	switch(s->type){
	/* first just return those statements which we cannot optimize,
	 * such as schema manipulation, transaction managment, 
	 * and user authentication.
	 */
	case st_none:
	case st_release: case st_commit: case st_rollback: 
	case st_schema: case st_table: case st_column: case st_key: 
	case st_create_schema: case st_drop_schema: 
	case st_create_table: case st_drop_table: 
	case st_create_column: case st_null: case st_default: 
	case st_create_key: 
	case st_create_role: case st_drop_role: 
	case st_grant_role: case st_revoke_role: 
	case st_grant: case st_revoke:

	case st_dbat: case st_obat: case st_basetable: case st_kbat:

	case st_atom: 
	case st_find: 
	case st_bulkinsert: case st_senddata: 
	case st_var:

		s->optimized = 2;
		return stmt_dup(s);

	case st_reljoin: {
		if (s->optimized > 1) 
			return stmt_dup(s);

	{	node *n1, *n2;

		for(n1 = s->op1.lval->h, n2 = s->op2.lval->h; n1 && n2; n1 = n1->next, n2 = n2->next ){
			n1->data = rel2bin(c, n1->data);
			n2->data = rel2bin(c, n2->data);
		}
		s->optimized = 2;
		return stmt_dup(s);
	}}

	case st_relselect: {
		if (s->optimized > 1) 
			return stmt_dup(s);

	{	stmt *res;
		node *n;
		list * l = create_stmt_list();

		for(n = s->op1.lval->h; n; n = n->next ){
			list_append(l, rel2bin(c, n->data));
		}
		res = (stmt*)list_reduce(l, (freduce)&stmt_semijoin, (fdup)&stmt_dup );
		list_destroy(l);

		res->optimized = 2;
		return res;
	}}

	case st_temp:
	case st_filter: 
	case st_select: case st_select2: case st_like: case st_semijoin: 
	case st_diff: case st_intersect: case st_union: case st_outerjoin:
	case st_join: 
	case st_mirror: case st_reverse: case st_const: case st_mark: 
	case st_group: case st_group_ext: case st_derive: case st_unique: 
	case st_limit: case st_order: case st_reorder: case st_ordered: 

	case st_alias: case st_column_alias: 
	case st_ibat: 
	case st_output: 
	case st_append: case st_insert: case st_replace: 
	case st_exception:

	case st_count: case st_aggr: 
	case st_op: case st_unop: case st_binop: case st_Nop:

		if (s->optimized > 1) 
			return stmt_dup(s);

		if (s->op1.stval){
			stmt *os = s->op1.stval;
		       	stmt *ns = rel2bin(c, os);
			s->op1.stval = ns;
			stmt_destroy(os);
		}
		if (s->op2.stval){
			stmt *os = s->op2.stval;
		       	stmt *ns = rel2bin(c, os);
			s->op2.stval = ns;
			stmt_destroy(os);
		}
		if (s->op3.stval){
			stmt *os = s->op3.stval;
		       	stmt *ns = rel2bin(c, os);
			s->op3.stval = ns;
			stmt_destroy(os);
		}
		s->optimized = 2;
		return stmt_dup(s);

	case st_list: {
		stmt *res = NULL;
		node *n;
		list *l = s->op1.lval;
		list *nl = NULL;

		if (s->optimized > 1) 
			return stmt_dup(s);

		nl = create_stmt_list();
		for(n = l->h; n; n = n->next ){
			stmt *ns = rel2bin(c, n->data);
			list_append(nl, ns);
		}
		res = stmt_list(nl);
		res->optimized = 2;
		return res;
	} 

	case st_bat: case st_ubat: 
		if (s->optimized > 1) 
			return stmt_dup(s);
		if (s->op1.cval->table->type == tt_view){
			return rel2bin(c, s->op1.cval->s);
		} else {
			s->optimized = 2;
			return stmt_dup(s);
		}

	case st_pivot: 
	case st_partial_pivot:
	case st_ptable:
	case st_set: case st_sets: 
		assert(0); 	/* these should have been rewriten by now */
	}
	return res;
}
