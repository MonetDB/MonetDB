#ifndef _STATEMENT_H_
#define _STATEMENT_H_

#include "sym.h"
#include "atom.h"
#include "context.h"
#include "catalog.h"

typedef enum statement_type {
	st_create_schema,
	st_drop_schema,
	st_create_table,
	st_drop_table,
	st_create_column,
	st_not_null,
	st_default,
	st_column,
	st_reverse,
	st_atom,
	st_cast,
	st_join,
	st_semijoin,
	st_intersect,
	st_select,
	st_insert,
	st_insert_column,
	st_like,
	st_update,
	st_delete,
	st_count,
	st_const,
	st_mark,
	st_group,
	st_derive,
	st_unique,
	st_ordered,
	st_order,
	st_reorder,
	st_unop,
	st_binop,
	st_aggr,
	st_exists,
	st_name,
	st_diamond, 
	st_pearl, 
	/* used internally only */
	st_list, 
	st_insert_list, 
	st_output,
} st_type;

typedef enum comp_type {
	cmp_equal,
	cmp_notequal,
	cmp_lt,
	cmp_lte,
	cmp_gt,
	cmp_gte,
} comp_type;

typedef void (*fdestroy)(void*);
typedef struct value {
	void *data;
	fdestroy destroy;
} value;

typedef struct statement {
	st_type type;
	symdata op1;
	symdata op2;
	symdata op3;
	int flag;
	int nrcols;
	int nr;
	table *h;
	table *t;
	int refcnt;
	value v;
} statement;

extern statement *statement_create_schema( schema *s );
extern statement *statement_drop_schema( schema *s );

extern statement *statement_create_table( table *t );
extern statement *statement_drop_table( table *t, int drop_action );
extern statement *statement_create_column( column *c ); 
extern statement *statement_not_null( statement *col );
extern statement *statement_default( statement *col, statement *def );

extern statement *statement_column( column *c );
extern statement *statement_reverse( statement *s );
extern statement *statement_atom( atom *op1 );
extern statement *statement_cast( char *convert, statement *s );
extern statement *statement_select( statement *op1, statement *op2, comp_type cmptype );
extern statement *statement_select2( statement *op1, statement *op2, statement *op3 );
extern statement *statement_like( statement *op1, statement *a );
extern statement *statement_join( statement *op1, statement *op2, comp_type cmptype);
extern statement *statement_semijoin( statement *op1, statement *op2 );
extern statement *statement_intersect( statement *op1, statement *op2 );
extern statement *statement_list( list *l );
extern statement *statement_output( statement *l );
extern statement *statement_diamond( statement *s1 );
extern statement *statement_pearl( list *s1 );

extern statement *statement_insert_list( list *l );
extern statement *statement_insert( column *c, statement *id, statement *v );

extern statement *statement_insert_column( statement *c, statement *a );
extern statement *statement_update( column *col, statement *values );
extern statement *statement_delete( column *col, statement *where );

extern statement *statement_count( statement *s );
extern statement *statement_const( statement *s, statement *val );
extern statement *statement_mark( statement *s, int id );
extern statement *statement_remark( statement *s, statement *t, int id );
extern statement *statement_group( statement *s );
extern statement *statement_derive( statement *s, statement *t );
extern statement *statement_unique( statement *s );

extern statement *statement_ordered( statement *order, statement *res );
extern statement *statement_order( statement *s, int direction );
extern statement *statement_reorder( statement *s, statement *t, int direction );

extern statement *statement_unop( statement *op1, func *op );
extern statement *statement_binop( statement *op1, statement *op2, func *op );
extern statement *statement_aggr( statement *op1, aggr *op, statement *group );

extern statement *statement_exists( statement *op1, list *l );

extern statement *statement_name( statement *op1, char *name );

extern const char *column_type( statement *st );
extern char *column_name( statement *st );
extern column *basecolumn( statement *st );

extern int statement_dump( statement *s, int *nr, context *sql );

extern void statement_destroy( statement *s );
#endif /* _STATEMENT_H_ */
