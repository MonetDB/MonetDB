#ifndef SQL_RELATION_H
#define SQL_RELATION_H

#include <sql_catalog.h>

#define BASETABLE 0
#define RELATION 1

typedef enum expression_type {
	e_exp,
	e_atom,
	e_relation,
	e_column,
	e_cmp,
	e_func,
	e_aggr,
	e_unique,
} expression_type;

typedef struct expression {
	char *name;
	void *l;
	void *r;
	expression_type  type;	/* atom, cmp, func/aggr */
	void *f; 	/* =,!=, but also func's and aggr's and column type */
	int  flag;
	char card;	/* card 
				(0 truth value!)
				(1 atoms (some aggr too))) 
				(2 ie unknown)
			*/
} sql_exp;
 
typedef enum operator_type {
	op_basetable,
	op_crossproduct,
	op_project,
	op_select,
	op_join,
	op_union,
	op_inter,
	op_diff,
	op_groupby,
	op_orderby,
	op_topn
} operator_type;

typedef struct relation {
	char *name;   
	void *l;
	void *r;
	operator_type op;	
	list *exps; 
	int nrcols;	/* nr of cols */	
	int card;	/* 0, 1 (row), 2 unkown */
} sql_rel;

#endif /* SQL_RELATION_H */
