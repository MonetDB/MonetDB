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

typedef enum stmt_type {
	st_none,
	st_schema,
	st_table,
	st_column,
	st_key,
	st_basetable,
	st_bat,
	st_ubat,
	st_obat,
	st_dbat,
	st_drop_schema,
	st_create_schema,
	st_drop_table,
	st_create_table,
	st_create_column,
	st_not_null,
	st_default,
	st_create_key,
	st_create_role,
	st_drop_role,
	st_grant,
	st_revoke,
	st_grant_role,
	st_revoke_role,

	st_commit,
	st_rollback,
	st_release,

	st_reverse,
	st_atom,
	st_join,
	st_semijoin,
	st_outerjoin,
	st_diff,
	st_intersect,
	st_union,
	st_select,
	st_select2,
	st_copyfrom,
	st_insert,
	st_like,
	st_replace,
	st_delete,
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
	st_unop,
	st_binop,
	st_triop,
	st_aggr,
	st_exists,
	st_alias,
	st_column_alias,
	st_set,
	st_sets,
	/* used internally only */
	st_list,
	st_output, /* return table */
	st_result  /* return status */
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
	char nrcols;
	char key;		/* key (aka all values are unique) */
	int flag;

	int nr; 		/* variable assignement */
	struct stmt *h;
	struct stmt *t;
	int refcnt;
} stmt;

typedef struct group {
	stmt *grp;
	stmt *ext;
	int refcnt;
} group;

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
extern stmt *stmt_not_null(stmt * col);
extern stmt *stmt_default(stmt * col, stmt * def);

extern stmt *stmt_key(key *k, stmt *rk );  

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

extern stmt *stmt_cbat(column * c, stmt * basetable, int access, int type);
extern stmt *stmt_tbat(table * t, int access, int type);

extern stmt *stmt_atom(atom * op1);
extern stmt *stmt_select(stmt * op1, stmt * op2, comp_type cmptype);
/* cmp 0 ==   l <= x <= h
       1 ==   l <  x <  h
       2 == !(l <= x <= h)  => l >  x >  h
       3 == !(l <  x <  h)  => l >= x >= h
       */
extern stmt *stmt_select2(stmt * op1, stmt * op2,
				    stmt * op3, int cmp);

extern stmt *stmt_like(stmt * op1, stmt * a);
extern stmt *stmt_join(stmt * op1, stmt * op2, comp_type cmptype);
extern stmt *stmt_outerjoin(stmt * op1, stmt * op2, comp_type cmptype);
extern stmt *stmt_semijoin(stmt * op1, stmt * op2);

extern stmt *stmt_diff(stmt * op1, stmt * op2);
extern stmt *stmt_intersect(stmt * op1, stmt * op2);
extern stmt *stmt_union(stmt * op1, stmt * op2);
extern stmt *stmt_list(list * l);
extern stmt *stmt_set(stmt * s1);
extern stmt *stmt_sets(list * s1);

extern stmt *stmt_copyfrom(table * t, list *files, char *tsep, char *rsep, int nr );
extern list *stmt_copyfrom_files( stmt *s );

extern stmt *stmt_insert(stmt *c, stmt * values, int unique_oids);
extern stmt *stmt_replace(stmt * c, stmt * values);
extern stmt *stmt_delete(table * t, stmt * where);

extern stmt *stmt_count(stmt * s);
extern stmt *stmt_const(stmt * s, stmt * val);
extern stmt *stmt_mark(stmt * s, int id);
extern stmt *stmt_remark(stmt * s, stmt * t, int id);
extern stmt *stmt_reverse(stmt * s);
extern stmt *stmt_unique(stmt * s, group * grp);

extern stmt *stmt_order(stmt * s, int direction);
extern stmt *stmt_reorder(stmt * s, stmt * t, int direction);
extern stmt *stmt_ordered(stmt * order, stmt * res);

extern stmt *stmt_unop(stmt * op1, sql_func * op);
extern stmt *stmt_binop(stmt * op1, stmt * op2, sql_func * op);
extern stmt *stmt_triop(stmt * op1, stmt * op2, stmt * op3, sql_func * op);
extern stmt *stmt_aggr(stmt * op1, sql_aggr * op, group * grp);

extern stmt *stmt_exists(stmt * op1, list * l);

extern stmt *stmt_alias(stmt * op1, char *name);
extern stmt *stmt_column(stmt * op1, stmt *t, char *tname, char *cname);

extern stmt *stmt_output(stmt * l);
extern stmt *stmt_result(stmt * l);

extern sql_subtype *head_type(stmt * st);
extern sql_subtype *tail_type(stmt * st);

extern char *column_name(stmt * st);
extern char *table_name(stmt * st);
extern stmt *head_column(stmt * st);
extern stmt *tail_column(stmt * st);
extern column *basecolumn(stmt *st);

extern int stmt_dump(stmt * s, int *nr, context * sql);

extern void stmt_destroy(stmt *s );
/* reset the stmt nr's */
extern void stmt_reset( stmt *s );
extern stmt *stmt_dup( stmt *s );

extern group *grp_create(stmt * s, group *og );
extern group *grp_semijoin(group *og, stmt *s );
extern void grp_destroy(group * g);
extern group *grp_dup(group * g);

extern int stmt_cmp_nrcols( stmt *s, int *nr );

extern stmt *stmt_push_down_head(stmt * s, stmt * select);
extern stmt *stmt_push_down_tail(stmt * s, stmt * select);
extern stmt *stmt_push_join_head(stmt * s, stmt * join);
extern stmt *stmt_push_join_tail(stmt * s, stmt * join);
extern stmt *stmt_join2select(stmt * join);

#endif				/* _STATEMENT_H_ */
