#ifndef _STATEMENT_H_
#define _STATEMENT_H_

#include "sym.h"
#include "atom.h"
#include "context.h"
#include "catalog.h"

#define RDONLY 0
#define INS 1
#define DEL 2
#define UPD 3

#define create_stmt_list() list_create((fdestroy)&stmt_destroy)

typedef enum stmt_type {
	st_none,
	st_schema,
	st_table,
	st_column,
	st_key,
	st_basetable,
	st_temp,	/* temporal bat */
	st_bat,
	st_ubat,
	st_ibat,	/* intermediate table result */
	st_obat,
	st_dbat,
	st_kbat,
	st_drop_schema,
	st_create_schema,
	st_drop_table,
	st_create_table,
	st_create_column,
	st_null,
	st_default,
	st_create_key,
	st_create_role,
	st_drop_role,
	st_grant,
	st_revoke,
	st_grant_role,
	st_revoke_role,
	st_var,

	st_commit,
	st_rollback,
	st_release,

	st_reverse,
	st_mirror,
	st_atom,
	st_reljoin,
	st_join,
	st_semijoin,
	st_outerjoin,
	st_diff,
	st_intersect,
	st_union,
	st_filter,
	st_select,
	st_select2,
	st_find,
	st_bulkinsert,
	st_senddata,
	st_like,
	st_append,
	st_insert,
	st_replace,
	st_exception,
	st_count,
	st_const,
	st_mark,
	st_group_ext,
	st_group,
	st_derive,
	st_unique,
	st_ordered,
	st_order,
	st_reorder,
	st_op,
	st_unop,
	st_binop,
	st_Nop,
	st_aggr,
	st_limit,
	st_column_alias,
	st_alias,
	st_set,
	st_sets,
	st_ptable,
	st_pivot,
	st_partial_pivot,
	/* used internally only */
	st_list,
	st_output /* return table */
} st_type;

typedef enum comp_type {
	cmp_equal,
	cmp_notequal,
	cmp_lt,
	cmp_lte,
	cmp_gt,
	cmp_gte,
	cmp_all
} comp_type;

typedef struct stmt {
	st_type type;
	symdata op1;
	symdata op2;
	symdata op3;
	symdata op4;		/* only op4 will hold other types */
	char nrcols;
	char key;		/* key (aka all values are unique) */
	int flag;

	int nr; 		/* variable assignement */
	struct stmt *h;
	struct stmt *t;
	int refcnt;
	int optimized;
} stmt;

typedef struct group {
	stmt *grp;
	stmt *ext;
	int refcnt;
} group;

extern const char * st_type2string(st_type type);

extern stmt *stmt_none();
extern stmt *stmt_var(char *varname, stmt *val);

/* since Savepoints and transactions related the 
 * stmt commit function includes the savepoint creation.
 * And rollbacks can be eigther full or until a given savepoint. 
 * The special stmt_release can be used to release savepoints. 
 */
extern stmt *stmt_commit(int chain, char *name);
extern stmt *stmt_rollback(int chain, char *name);
extern stmt *stmt_release(char *name);

extern stmt *stmt_bind_schema(schema * sc);
extern stmt *stmt_bind_table(stmt *schema, table * t);
extern stmt *stmt_bind_column(stmt *table, column *c);
extern stmt *stmt_bind_key(stmt *table, key *k);

extern stmt *stmt_drop_schema(schema * s, int dropaction);
extern stmt *stmt_create_schema(schema * s);
extern stmt *stmt_drop_table(stmt *s, char * name, int drop_action);
extern stmt *stmt_create_table(stmt *s, table * t);
extern stmt *stmt_create_column(stmt *t, column * c);
extern stmt *stmt_null(stmt * col, int flag);
extern stmt *stmt_default(stmt * col, stmt * def);

extern stmt *stmt_create_key(key *k, stmt *rk );  

extern stmt *stmt_create_role(char *name, int admin);
extern stmt *stmt_drop_role(char *name );
extern stmt *stmt_grant_role(char *authid, char *role);
extern stmt *stmt_revoke_role(char *authid, char *role);

/*
extern stmt *stmt_schema_grant(stmt *s, char *authid, int privilege);
extern stmt *stmt_schema_revoke(stmt *s, char *authid, int privilege);
extern stmt *stmt_table_grant(stmt *t, char *authid, int privilege);
extern stmt *stmt_table_revoke(stmt *t, char *authid, int privilege);
extern stmt *stmt_column_grant(stmt *c, char *authid, int privilege);
extern stmt *stmt_column_revoke(stmt *c, char *authid, int privilege);
*/

extern stmt *stmt_basetable(table *t); 
#define isbasetable(s) (s->type == st_basetable)
#define basetable_table(s) s->op1.tval 

#define ptable_ppivots(s) 	((s)->op1.lval)
#define ptable_pivots(s) 	((s)->op2.lval)
#define ptable_statements(s) 	((s)->op3.stval)

extern stmt *stmt_cbat(column * c, stmt * basetable, int access, int type);
extern stmt *stmt_ibat(stmt * i, stmt * basetable );
extern stmt *stmt_tbat(table * t, int access, int type);
extern stmt *stmt_kbat(key *k, int access );

extern stmt *stmt_temp(sql_subtype * t );
extern stmt *stmt_atom(atom * op1);
extern stmt *stmt_filter(stmt * sel);
extern stmt *stmt_select(stmt * op1, stmt * op2, comp_type cmptype);
/* cmp 0 ==   l <= x <= h
       1 ==   l <  x <  h
       2 == !(l <= x <= h)  => l >  x >  h
       3 == !(l <  x <  h)  => l >= x >= h
       */
extern stmt *stmt_select2(stmt * op1, stmt * op2,
				    stmt * op3, int cmp);

extern stmt *stmt_like(stmt * op1, stmt * a);
extern stmt *stmt_reljoin1(list * joins);
extern stmt *stmt_reljoin2(list * l1, list * l2);
extern stmt *stmt_join(stmt * op1, stmt * op2, comp_type cmptype);
extern stmt *stmt_outerjoin(stmt * op1, stmt * op2, comp_type cmptype);
extern stmt *stmt_semijoin(stmt * op1, stmt * op2);

extern stmt *stmt_diff(stmt * op1, stmt * op2);
extern stmt *stmt_intersect(stmt * op1, stmt * op2);
extern stmt *stmt_union(stmt * op1, stmt * op2);
extern stmt *stmt_list(list * l);
extern stmt *stmt_set(stmt * s1);
extern stmt *stmt_sets(list * s1);
extern stmt *stmt_ptable();
extern stmt *stmt_pivot(stmt *s, stmt *ptable);

extern stmt *stmt_find(stmt *b, stmt *v );
extern stmt *stmt_bulkinsert(stmt *t, char *sep, char *rsep, stmt *file, int nr);
extern stmt *stmt_senddata();

extern stmt *stmt_append(stmt *c, stmt * values);
extern stmt *stmt_insert(stmt *c, stmt * values);
extern stmt *stmt_replace(stmt * c, stmt * values);

/* raise exception incase the condition (cond) holds */
extern stmt *stmt_exception(stmt * cond, char *errstr); 

extern stmt *stmt_count(stmt * s);
extern stmt *stmt_const(stmt * s, stmt * val);
extern stmt *stmt_mark(stmt * s, int id);
extern stmt *stmt_remark(stmt * s, stmt * t, int id);
extern stmt *stmt_reverse(stmt * s);
extern stmt *stmt_mirror(stmt * s);
extern stmt *stmt_unique(stmt * s, group * grp);

extern stmt *stmt_limit(stmt * s, int limit);
extern stmt *stmt_order(stmt * s, int direction);
extern stmt *stmt_reorder(stmt * s, stmt * t, int direction);
extern stmt *stmt_ordered(stmt * order, stmt * res);

extern stmt *stmt_op(sql_subfunc * op);
extern stmt *stmt_unop(stmt * op1, sql_subfunc * op);
extern stmt *stmt_binop(stmt * op1, stmt * op2, sql_subfunc * op);
extern stmt *stmt_Nop(stmt * ops, sql_subfunc * op);
extern stmt *stmt_aggr(stmt * op1, group * grp, sql_subaggr * op );

extern stmt *stmt_alias(stmt * op1, char *name);
extern stmt *stmt_column(stmt * op1, stmt *t, char *tname, char *cname);

extern stmt *stmt_output(stmt * l);

extern sql_subtype *head_type(stmt * st);
sql_export sql_subtype *tail_type(stmt * st);

sql_export char *column_name(stmt * st);
extern char *table_name(stmt * st);
extern stmt *head_column(stmt * st);
extern stmt *tail_column(stmt * st);
sql_export column *basecolumn(stmt *st);

extern int stmt_dump(stmt * s, int *nr, context * sql);

sql_export void stmt_destroy(stmt *s );
extern stmt *stmt_dup( stmt *s );

extern group *grp_create(stmt * s, group *og );
extern group *grp_semijoin(group *og, stmt *s );
extern void grp_destroy(group * g);
extern group *grp_dup(group * g);

#endif				/* _STATEMENT_H_ */
