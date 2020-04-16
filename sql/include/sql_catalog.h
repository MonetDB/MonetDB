/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef SQL_CATALOG_H
#define SQL_CATALOG_H

#include "sql_mem.h"
#include "sql_list.h"
#include "mapi_querytype.h"
#include "stream.h"

#define tr_none		0
#define tr_readonly	1
#define tr_writable	2
#define tr_serializable 4
#define tr_append 	8

#define ACT_NO_ACTION 0
#define ACT_CASCADE 1
#define ACT_RESTRICT 2
#define ACT_SET_NULL 3
#define ACT_SET_DEFAULT 4

#define DROP_RESTRICT 0
#define DROP_CASCADE 1
#define DROP_CASCADE_START 2

#define PRIV_SELECT 1
#define PRIV_UPDATE 2
#define PRIV_INSERT 4
#define PRIV_DELETE 8
#define PRIV_EXECUTE 16
#define PRIV_GRANT 32
#define PRIV_TRUNCATE 64
/* global privs */
#define PRIV_COPYFROMFILE 1
#define PRIV_COPYINTOFILE 2

typedef enum sql_dependency { 
	SCHEMA_DEPENDENCY = 1,
	TABLE_DEPENDENCY = 2,
	COLUMN_DEPENDENCY = 3,
	KEY_DEPENDENCY = 4,
	VIEW_DEPENDENCY = 5,
	USER_DEPENDENCY = 6,
	FUNC_DEPENDENCY = 7,
	TRIGGER_DEPENDENCY = 8,
	OWNER_DEPENDENCY = 9,
	INDEX_DEPENDENCY = 10,
	FKEY_DEPENDENCY = 11,
	SEQ_DEPENDENCY = 12,
	PROC_DEPENDENCY = 13,
	BEDROPPED_DEPENDENCY = 14, /*The object must be dropped when the dependent object is dropped independently of the DROP type.*/
	TYPE_DEPENDENCY = 15
} sql_dependency;

#define NO_DEPENDENCY 0
#define HAS_DEPENDENCY 1
#define CICLE_DEPENDENCY 2
#define DEPENDENCY_CHECK_ERROR 3
#define DEPENDENCY_CHECK_OK 0

#define ROLE_PUBLIC   1
#define ROLE_SYSADMIN 2
#define USER_MONETDB  3

#define ISO_READ_UNCOMMITED 1
#define ISO_READ_COMMITED   2
#define ISO_READ_REPEAT	    3
#define ISO_SERIALIZABLE    4

#define SCALE_NONE	0
#define SCALE_FIX	1	/* many numerical functions require equal
                           scales/precision for all their inputs */
#define SCALE_NOFIX	2
#define SCALE_MUL	3	/* multiplication gives the sum of scales */
#define SCALE_DIV	4	/* div on the other hand reduces the scales */ 
#define DIGITS_ADD	5	/* some types grow under functions (concat) */
#define INOUT		6	/* output type equals input type */
#define SCALE_EQ	7	/* user defined functions need equal scales */
#define SCALE_DIGITS_FIX 8	/* the geom module requires the types and functions to have the same scale and digits */

/* Warning TR flags is a bitmask */
#define TR_NEW 1

#define RDONLY 0
#define RD_INS 1
#define RD_UPD_ID 2
#define RD_UPD_VAL 3
#define QUICK  4

/* the following list of macros are used by rel_rankop function */
#define UNBOUNDED_PRECEDING_BOUND 0
#define UNBOUNDED_FOLLOWING_BOUND 1
#define CURRENT_ROW_BOUND         2

#define FRAME_ROWS  0 		/* number of rows (preceding/following) */
#define FRAME_RANGE 1		/* logical range (based on the ordering column).
				   Example:
				   RANGE BETWEEN INTERVAL '1' MONTH PRECEDING  
				             AND INTERVAL '1' MONTH FOLLOWING */
#define FRAME_GROUPS 2
#define FRAME_ALL 3 /* special case of FRAME_RANGE, cover the entire partition */

/* the following list of macros are used by SQLwindow_bound function */
#define BOUND_FIRST_HALF_PRECEDING  0
#define BOUND_FIRST_HALF_FOLLOWING  1
#define BOUND_SECOND_HALF_PRECEDING 2
#define BOUND_SECOND_HALF_FOLLOWING 3
#define CURRENT_ROW_PRECEDING       4
#define CURRENT_ROW_FOLLOWING       5

#define EXCLUDE_NONE 0		/* nothing excluded (also the default) */
#define EXCLUDE_CURRENT_ROW 1	/* exclude the current row */
#define EXCLUDE_GROUP 2		/* exclude group */
#define EXCLUDE_TIES 3		/* exclude group but not the current row */

/* The following macros are used in properties field of sql_table */
#define PARTITION_RANGE       1
#define PARTITION_LIST        2
#define PARTITION_COLUMN      4
#define PARTITION_EXPRESSION  8

#define STORAGE_MAX_VALUE_LENGTH 2048

#define cur_user 1
#define cur_role 2

#define sql_max(i1,i2) ((i1)<(i2))?(i2):(i1)

#define dt_schema 	"%dt%"
#define isDeclaredSchema(s) 	(strcmp(s->base.name, dt_schema) == 0)

extern const char *TID;

typedef enum temp_t { 
	SQL_PERSIST = 0,
	SQL_LOCAL_TEMP = 1,
	SQL_GLOBAL_TEMP = 2,
	SQL_DECLARED_TABLE = 3,	/* variable inside a stored procedure */
	SQL_MERGE_TABLE = 4,
	SQL_STREAM = 5,
	SQL_REMOTE = 6,
	SQL_REPLICA_TABLE = 7
} temp_t;

typedef enum comp_type {
	cmp_gt = 0,
	cmp_gte = 1,
	cmp_lte = 2,
	cmp_lt = 3,
	cmp_equal = 4,
	cmp_notequal = 5,

	cmp_filter = 6,
	cmp_or = 7,
	cmp_in = 8,			/* in value list */
	cmp_notin = 9,			/* not in value list */

	mark_in = 10,			/* mark joins */
	mark_notin = 11,
	mark_exists = 12,
	mark_notexists = 13,

	/* The followin cmp_* are only used within stmt (not sql_exp) */
	cmp_all = 14,			/* special case for crossproducts */
	cmp_project = 15,		/* special case for projection joins */
	cmp_joined = 16, 		/* special case already joined */
	cmp_left = 17,			/* special case equi join, keep left order */
	cmp_left_project = 18		/* last step of outer join */
} comp_type;

/* for ranges we keep the requirment for symmetric */
#define CMP_SYMMETRIC 8
#define CMP_BETWEEN 16

#define is_theta_exp(e) ((e) == cmp_gt || (e) == cmp_gte || (e) == cmp_lte ||\
		         (e) == cmp_lt || (e) == cmp_equal || (e) == cmp_notequal)

#define is_complex_exp(et) ((et) == cmp_or || (et) == cmp_in || (et) == cmp_notin || (et) == cmp_filter)

typedef enum commit_action_t { 
	CA_COMMIT, 	/* commit rows, only for persistent tables */
	CA_DELETE, 	/* delete rows */
	CA_PRESERVE,	/* preserve rows */
	CA_DROP		/* drop table */
} ca_t;

typedef int sqlid;

typedef struct sql_base {
	int wtime;
	int rtime;
	int stime;
	int allocated;
	int flags;
	int refcnt;
	sqlid id;
	char *name;
} sql_base;

#define newFlagSet(x)     ((x & TR_NEW) == TR_NEW)
#define removeNewFlag(x)  ((x)->base.flags &= ~TR_NEW)
#define isNew(x)          (newFlagSet((x)->base.flags))

extern void base_init(sql_allocator *sa, sql_base * b, sqlid id, int flags, const char *name);

typedef struct changeset {
	sql_allocator *sa;
	fdestroy destroy;
	struct list *set;
	struct list *dset;
	node *nelm;
} changeset;

extern void cs_new(changeset * cs, sql_allocator *sa, fdestroy destroy);
extern void cs_destroy(changeset * cs);
extern void cs_add(changeset * cs, void *elm, int flag);
extern void *cs_add_with_validate(changeset * cs, void *elm, int flag, fvalidate cmp);
extern void cs_add_before(changeset * cs, node *n, void *elm);
extern void cs_del(changeset * cs, node *elm, int flag);
extern void cs_move(changeset *from, changeset *to, void *data);
extern void *cs_transverse_with_validate(changeset * cs, void *elm, fvalidate cmp);
extern int cs_size(changeset * cs);
extern node *cs_find_name(changeset * cs, const char *name);
extern node *cs_find_id(changeset * cs, sqlid id);
extern node *cs_first_node(changeset * cs);
extern node *cs_last_node(changeset * cs);
extern void cs_remove_node(changeset * cs, node *n);

typedef void *backend_code;
typedef size_t backend_stack;

typedef struct sql_trans {
	char *name;
	int stime;		/* start of transaction */
	int wstime;		/* first write transaction time stamp */
	int rtime;		/* timestamp of latest read performed in transaction*/
	int wtime;		/* timestamp of latest write performed in transaction*/
	int schema_number;	/* schema timestamp */
	int schema_updates;	/* set on schema changes */
	int active;		/* active transaction */
	int status;		/* status of the last query */
	list *dropped;  	/* protection against recursive cascade action*/
	list *moved_tables;

	changeset schemas;

	sql_allocator *sa;	/* transaction allocator */

	struct sql_trans *parent;	/* multilevel transaction support */
	backend_stack stk;		
} sql_trans;

typedef struct sql_schema {
	sql_base base;
	sqlid auth_id;
	sqlid owner;
	bit system;		/* system or user schema */
	// TODO? int type;	/* persistent, session local, transaction local */

	changeset tables;
	changeset types;
	changeset funcs;
	changeset seqs;
	list *keys;		/* Names for keys, idxs and triggers are */
	list *idxs;		/* global, but these objects are only */
	list *triggers;		/* useful within a table */

	char *internal; 	/* optional internal module name */
	sql_trans *tr;
} sql_schema;

typedef enum sql_class {
	EC_ANY,
	EC_TABLE,
	EC_BIT,
	EC_CHAR,
	EC_STRING,
	EC_BLOB,
	EC_POS,
	EC_NUM,
	EC_MONTH,
	EC_SEC,
	EC_DEC,
	EC_FLT,
	EC_TIME,
	EC_TIME_TZ,
	EC_DATE,
	EC_TIMESTAMP,
	EC_TIMESTAMP_TZ,
	EC_GEOM,
	EC_EXTERNAL,
	EC_MAX /* evaluated to the max value, should be always kept at the bottom */
} sql_class;

#define has_tz(e,n)		(EC_TEMP_TZ(e)) 
#define type_has_tz(t)		has_tz((t)->type->eclass, (t)->type->sqlname)
#define EC_VARCHAR(e)		(e==EC_CHAR||e==EC_STRING)
#define EC_INTERVAL(e)		(e==EC_MONTH||e==EC_SEC)
#define EC_NUMBER(e)		(e==EC_POS||e==EC_NUM||EC_INTERVAL(e)||e==EC_DEC||e==EC_FLT)
#define EC_COMPUTE(e)		(e==EC_NUM||e==EC_FLT)
#define EC_BOOLEAN(e)		(e==EC_BIT||e==EC_NUM||e==EC_FLT)
#define EC_TEMP_TZ(e)		(e==EC_TIME_TZ||e==EC_TIMESTAMP_TZ)
#define EC_TEMP(e)		(e==EC_TIME||e==EC_DATE||e==EC_TIMESTAMP||EC_TEMP_TZ(e))
#define EC_TEMP_FRAC(e)		(e==EC_TIME||e==EC_TIMESTAMP||EC_TEMP_TZ(e))
#define EC_FIXED(e)		(e==EC_BIT||e==EC_CHAR||e==EC_POS||e==EC_NUM||EC_INTERVAL(e)||e==EC_DEC||EC_TEMP(e))

typedef struct sql_type {
	sql_base base;

	char *sqlname;
	unsigned int digits;
	unsigned int scale;	/* indicates how scale is used in functions */
	int localtype;		/* localtype, need for coersions */
	unsigned char radix;
	unsigned int bits;
	sql_class eclass; 	/* types are grouped into equivalence classes */
	sql_schema *s;
} sql_type;

typedef struct sql_alias {
	char *name;
	char *alias;
} sql_alias;

#define ARG_IN 1
#define ARG_OUT 0

typedef struct sql_subtype {
	sql_type *type;
	unsigned int digits;
	unsigned int scale;
} sql_subtype;

/* sql_func need type transform rules types are equal if underlying
 * types are equal + scale is equal if types do not mach we try type
 * conversions which means for simple 1 arg functions
 */

typedef struct sql_arg {
	char *name;
	bte inout;
	sql_subtype type;
} sql_arg;

typedef enum sql_ftype {
	F_FUNC = 1,
	F_PROC = 2,
	F_AGGR = 3,
	F_FILT = 4,
	F_UNION = 5,
	F_ANALYTIC = 6,
	F_LOADER = 7
} sql_ftype;

#define IS_FUNC(f)     ((f)->type == F_FUNC)
#define IS_PROC(f)     ((f)->type == F_PROC)
#define IS_AGGR(f)     ((f)->type == F_AGGR)
#define IS_FILT(f)     ((f)->type == F_FILT)
#define IS_UNION(f)    ((f)->type == F_UNION)
#define IS_ANALYTIC(f) ((f)->type == F_ANALYTIC)
#define IS_LOADER(f)   ((f)->type == F_LOADER)

#define FUNC_TYPE_STR(type) \
	switch (type) { \
		case F_FUNC: \
			F = "FUNCTION"; \
			fn = "function"; \
			break; \
		case F_PROC: \
			F = "PROCEDURE"; \
			fn = "procedure"; \
			break; \
		case F_AGGR: \
			F = "AGGREGATE"; \
			fn = "aggregate"; \
			break; \
		case F_FILT: \
			F = "FILTER FUNCTION"; \
			fn = "filter function"; \
			break; \
		case F_UNION: \
			F = "UNION FUNCTION"; \
			fn = "union function"; \
			break; \
		case F_ANALYTIC: \
			F = "WINDOW FUNCTION"; \
			fn = "window function"; \
			break; \
		case F_LOADER: \
			F = "LOADER FUNCTION"; \
			fn = "loader function"; \
			break; \
		default: \
			assert(0); \
	}

typedef enum sql_flang {
	FUNC_LANG_INT = 0, /* internal */
	FUNC_LANG_MAL = 1, /* create sql external mod.func */
	FUNC_LANG_SQL = 2, /* create ... sql function/procedure */
	FUNC_LANG_R = 3,   /* create .. language R */
	FUNC_LANG_C = 4,   /* create .. language C */
	FUNC_LANG_J = 5,   /* create .. language JAVASCRIPT (not implemented) */
	/* this should probably be done in a better way */
	FUNC_LANG_PY = 6,       /* create .. language PYTHON */
	FUNC_LANG_MAP_PY = 7,   /* create .. language PYTHON_MAP */
	/* values 8 and 9 were for Python 2 */
	FUNC_LANG_PY3 = 10,     /* create .. language PYTHON3 */
	FUNC_LANG_MAP_PY3 = 11, /* create .. language PYTHON3_MAP */
	FUNC_LANG_CPP = 12      /* create .. language CPP */
} sql_flang;

#define LANG_EXT(l)  (l>FUNC_LANG_SQL)

typedef struct sql_func {
	sql_base base;

	char *imp;
	char *mod;
	sql_ftype type;
	list *ops;	/* param list */
	list *res;	/* list of results */
	int nr;
	int sql;	/* 0 native implementation
			   1 sql 
			   2 sql instantiated proc 
			*/
	sql_flang lang;
	char *query;	/* sql code */
	bit side_effect;
	bit varres;	/* variable output result */
	bit vararg;	/* variable input arguments */
	bit system;	/* system function */
	int fix_scale;
			/*
	   		   SCALE_NOFIX/SCALE_NONE => nothing
	   		   SCALE_FIX => input scale fixing,
	   		   SCALE_ADD => leave inputs as is and do add scales
	   		   example numerical multiplication
	   		   SCALE_SUB => first input scale, fix with second scale
	   		   result scale is equal to first input
	   		   example numerical division
	   		   DIGITS_ADD => result digits, sum of args
	   		   example string concat
	 		*/
	sql_schema *s;
	sql_allocator *sa;
	void *rel;	/* implementation */
} sql_func;

typedef struct sql_subfunc {
	sql_func *func;
	list *res;
	list *coltypes; /* we need this for copy into from loader */
	list *colnames; /* we need this for copy into from loader */
	char *sname, *tname; /* we need this for create table from loader */
} sql_subfunc;

typedef enum key_type {
	pkey,
	ukey,
	fkey
} key_type;

typedef struct sql_kc {
	struct sql_column *c;
	int trunc;		/* 0 not truncated, >0 colum is truncated */
} sql_kc;

typedef enum idx_type {
	hash_idx,
	join_idx,
	oph_idx,		/* order preserving hash */
	no_idx,			/* no idx, ie no storage */
	imprints_idx,
	ordered_idx,
	new_idx_types
} idx_type;

#define hash_index(t) 		(t == hash_idx || t == oph_idx )
#define idx_has_column(t) 	(hash_index(t) || t == join_idx)
#define oid_index(t)		(t == join_idx)
#define non_updatable_index(t) (t == ordered_idx || t == no_idx)

typedef struct sql_idx {
	sql_base base;
	idx_type type;		/* unique */
	struct list *columns;	/* list of sql_kc */
	struct sql_table *t;
	struct sql_key *key;	/* key */
	struct sql_idx *po;	/* the outer transactions idx */
	void *data;
} sql_idx;

/* fkey consists of two of these */
typedef struct sql_key {	/* pkey, ukey, fkey */
	sql_base base;
	key_type type;		/* pkey, ukey, fkey */
	sql_idx *idx;		/* idx to accelerate key check */

	struct list *columns;	/* list of sql_kc */
	struct sql_table *t;
	int drop_action;	/* only needed for alter drop key */
} sql_key;

typedef struct sql_ukey {	/* pkey, ukey */
	sql_key k;
	list *keys;
} sql_ukey;

typedef struct sql_fkey {	/* fkey */
	sql_key k;
	/* no action, restrict (default), cascade, set null, set default */
	int on_delete;	
	int on_update;	
	struct sql_ukey *rkey;	/* only set for fkey and rkey */
} sql_fkey;

typedef struct sql_trigger {
	sql_base base;
	sht time;		/* before or after */
	sht orientation; 	/* row or statement */
	sht event;		/* insert, delete, update, truncate */
	/* int action_order;	 TODO, order within the set of triggers */
	struct list *columns;	/* update trigger on list of (sql_kc) columns */

	struct sql_table *t;
	char *old_name;		/* name referencing the old values */
	char *new_name;		/* name referencing the new values */
	
	char *condition; 	/* when search condition, ie query */
	char *statement;	/* action, ie list of sql statements */
} sql_trigger;

typedef struct sql_sequence {
	sql_base base;
	lng start;
	lng minvalue;
	lng maxvalue;
	lng increment;
	lng cacheinc;
	bit cycle;
	bit bedropped;		/*Drop the SEQUENCE if you are dropping the column, e.g., SERIAL COLUMN".*/
	sql_schema *s;
} sql_sequence;

typedef struct sql_column {
	sql_base base;
	sql_subtype type;
	int colnr;
	bit null;
	char *def;
	char unique; 		/* NOT UNIQUE, UNIQUE, SUB_UNIQUE */
	int drop_action;	/* only used for alter statements */
	char *storage_type;
	int sorted;		/* for DECLARED (dupped tables) we keep order info */
	size_t dcount;
	char *min;
	char *max;

	struct sql_table *t;
	struct sql_column *po;	/* the outer transactions column */
	void *data;
} sql_column;

typedef enum table_types {
	tt_table = 0, 		/* table */
	tt_view = 1, 		/* view */
	tt_merge_table = 3,	/* multiple tables form one table */
	tt_stream = 4,		/* stream */
	tt_remote = 5,		/* stored on a remote server */
	tt_replica_table = 6	/* multiple replica of the same table */
} table_types;

#define TABLE_TYPE_DESCRIPTION(tt,properties)                                                                       \
(tt == tt_table)?"TABLE":(tt == tt_view)?"VIEW":(tt == tt_merge_table && !properties)?"MERGE TABLE":                \
(tt == tt_stream)?"STREAM TABLE":(tt == tt_remote)?"REMOTE TABLE":                                                  \
(tt == tt_merge_table && (properties & PARTITION_LIST) == PARTITION_LIST)?"LIST PARTITION TABLE":                   \
(tt == tt_merge_table && (properties & PARTITION_RANGE) == PARTITION_RANGE)?"RANGE PARTITION TABLE":"REPLICA TABLE"

#define isTable(x)                        ((x)->type==tt_table)
#define isView(x)                         ((x)->type==tt_view)
#define isNonPartitionedTable(x)          ((x)->type==tt_merge_table && !(x)->properties)
#define isRangePartitionTable(x)          ((x)->type==tt_merge_table && ((x)->properties & PARTITION_RANGE) == PARTITION_RANGE)
#define isListPartitionTable(x)           ((x)->type==tt_merge_table && ((x)->properties & PARTITION_LIST) == PARTITION_LIST)
#define isPartitionedByColumnTable(x)     ((x)->type==tt_merge_table && ((x)->properties & PARTITION_COLUMN) == PARTITION_COLUMN)
#define isPartitionedByExpressionTable(x) ((x)->type==tt_merge_table && ((x)->properties & PARTITION_EXPRESSION) == PARTITION_EXPRESSION)
#define isMergeTable(x)                   ((x)->type==tt_merge_table)
#define isStream(x)                       ((x)->type==tt_stream)
#define isRemote(x)                       ((x)->type==tt_remote)
#define isReplicaTable(x)                 ((x)->type==tt_replica_table)
#define isKindOfTable(x)                  (isTable(x) || isMergeTable(x) || isRemote(x) || isReplicaTable(x))
#define isPartition(x)                    (isTable(x) && (x)->p)

#define TABLE_WRITABLE	0
#define TABLE_READONLY	1
#define TABLE_APPENDONLY	2

typedef struct sql_part_value {
	ptr value;
	size_t length;
} sql_part_value;

typedef struct sql_part {
	sql_base base;
	struct sql_table *t; /* cached value of the merge table */
	sql_subtype tpe;     /* the column/expression type */
	bit with_nills;      /* 0 no nills, 1 holds nills, NULL holds all values -> range FROM MINVALUE TO MAXVALUE WITH NULL */
	union {
		list *values;         /* partition by values/list */
		struct sql_range {    /* partition by range */
			ptr minvalue;
			ptr maxvalue;
			size_t minlength;
			size_t maxlength;
		} range;
	} part;
} sql_part;

typedef struct sql_expression {
	sql_subtype type; /* the returning sql_subtype of the expression */
	char *exp;        /* the expression itself */
	list *cols;       /* list of colnr of the columns of the table used in the expression */
} sql_expression;

typedef struct sql_table {
	sql_base base;
	sht type;		/* table, view, etc */
	sht access;		/* writable, readonly, appendonly */
	bit system;		/* system or user table */
	bit bootstrap;		/* system table created during bootstrap */
	bte properties;		/* used for merge_tables */
	temp_t persistence;	/* persistent, global or local temporary */
	ca_t commit_action;  	/* on commit action */
	char *query;		/* views may require some query */
	int  sz;

	sql_ukey *pkey;
	changeset columns;
	changeset idxs;
	changeset keys;
	changeset triggers;
	changeset members;
	int drop_action;	/* only needed for alter drop table */

	int cleared;		/* cleared in the current transaction */
	void *data;
	struct sql_schema *s;
	struct sql_table *po;	/* the outer transactions table */

	struct sql_table *p;	 /* The table is part of this merge table */
	union {
		struct sql_column *pcol; /* If it is partitioned on a column */
		struct sql_expression *pexp; /* If it is partitioned by an expression */
	} part;
} sql_table;

typedef struct sql_moved_table {
	sql_schema *from;
	sql_schema *to;
	sql_table *t;
} sql_moved_table;

typedef struct res_col {
	char *tn;
	char *name;
	sql_subtype type;
	bat b;
	int mtype;
	ptr *p;
} res_col;

typedef struct res_table {
	int id;
	oid query_id;
	mapi_query_t query_type;
	int nr_cols;
	int cur_col;
	const char *tsep;
	const char *rsep;
	const char *ssep;
	const char *ns;
	res_col *cols;
	bat order;
	struct res_table *next;
} res_table;

typedef struct sql_session {
	sql_trans *tr; 		/* active transaction */	

	char *schema_name;
	sql_schema *schema;

	char ac_on_commit;	/* if 1, auto_commit should be enabled on
	                           commit, rollback, etc. */
	char auto_commit;
	int level;		/* TRANSACTION isolation level */
	int status;		/* status, ok/error */
	backend_stack stk;
} sql_session;

extern void schema_destroy(sql_schema *s);
extern void table_destroy(sql_table *t);
extern void column_destroy(sql_column *c);
extern void key_destroy(sql_key *k);
extern void idx_destroy(sql_idx * i);

extern int base_key(sql_base *b);
extern node *list_find_name(list *l, const char *name);
extern node *list_find_id(list *l, sqlid id);
extern node *list_find_base_id(list *l, sqlid id);

extern sql_key *find_sql_key(sql_table *t, const char *kname);
extern node *find_sql_key_node(sql_schema *s, sqlid id);
extern sql_key *sql_trans_find_key(sql_trans *tr, sqlid id);

extern sql_idx *find_sql_idx(sql_table *t, const char *kname);
extern node *find_sql_idx_node(sql_schema *s, sqlid id);
extern sql_idx *sql_trans_find_idx(sql_trans *tr, sqlid id);

extern sql_column *find_sql_column(sql_table *t, const char *cname);

extern sql_part *find_sql_part(sql_table *t, const char *tname);

extern sql_table *find_sql_table(sql_schema *s, const char *tname);
extern sql_table *find_sql_table_id(sql_schema *s, sqlid id);
extern node *find_sql_table_node(sql_schema *s, sqlid id);
extern sql_table *sql_trans_find_table(sql_trans *tr, sqlid id);

extern sql_sequence *find_sql_sequence(sql_schema *s, const char *sname);

extern sql_schema *find_sql_schema(sql_trans *t, const char *sname);
extern sql_schema *find_sql_schema_id(sql_trans *t, sqlid id);
extern node *find_sql_schema_node(sql_trans *t, sqlid id);

extern sql_type *find_sql_type(sql_schema * s, const char *tname);
extern sql_type *sql_trans_bind_type(sql_trans *tr, sql_schema *s, const char *name);
extern node *find_sql_type_node(sql_schema *s, sqlid id);
extern sql_type *sql_trans_find_type(sql_trans *tr, sqlid id);

extern sql_func *find_sql_func(sql_schema * s, const char *tname);
extern list *find_all_sql_func(sql_schema * s, const char *tname, sql_ftype type);
extern sql_func *sql_trans_bind_func(sql_trans *tr, const char *name);
extern sql_func *sql_trans_find_func(sql_trans *tr, sqlid id);
extern node *find_sql_func_node(sql_schema *s, sqlid id);

extern node *find_sql_trigger_node(sql_schema *s, sqlid id);
extern sql_trigger *sql_trans_find_trigger(sql_trans *tr, sqlid id);

extern void *sql_values_list_element_validate_and_insert(void *v1, void *v2, void *tpe, int* res);
extern void *sql_range_part_validate_and_insert(void *v1, void *v2);
extern void *sql_values_part_validate_and_insert(void *v1, void *v2);

typedef struct {
	BAT *b;
	char* name;
	void* def;
} sql_emit_col;

#endif /* SQL_CATALOG_H */
