#ifndef SYM_H
#define SYM_H

typedef enum symtype {
	type_int, 
	type_string,
	type_list,
	type_atom,
	type_symbol,
	type_statement,
	type_column,
	type_table,
	type_aggr,
	type_func,
	type_type,
} symtype;

typedef union symdata {
	int    ival;
	char   *sval;
	struct atom   *aval;
	struct list   *lval;
	struct statement *stval;
	struct column *cval;
	struct table *tval;
	struct aggr *aggrval;
	struct func *funcval;
	struct type *typeval;
} symdata;


#endif
