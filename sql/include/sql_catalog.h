/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef SQL_CATALOG_H
#define SQL_CATALOG_H

#include "sql_mem.h"
#include "sql_list.h"
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

#define SCHEMA_DEPENDENCY 1
#define TABLE_DEPENDENCY 2
#define COLUMN_DEPENDENCY 3
#define KEY_DEPENDENCY 4
#define VIEW_DEPENDENCY 5
#define USER_DEPENDENCY 6
#define FUNC_DEPENDENCY 7
#define TRIGGER_DEPENDENCY 8
#define OWNER_DEPENDENCY 9
#define INDEX_DEPENDENCY 10
#define FKEY_DEPENDENCY 11
#define SEQ_DEPENDENCY 12
#define PROC_DEPENDENCY 13
#define BEDROPPED_DEPENDENCY 14		/*The object must be dropped when the dependent object is dropped independently of the DROP type.*/
#define TYPE_DEPENDENCY 15

#define NO_DEPENDENCY 0
#define HAS_DEPENDENCY 1
#define CICLE_DEPENDENCY 2
#define DEPENDENCY_CHECK_ERROR 3
#define DEPENDENCY_CHECK_OK 0

#define NO_TRIGGER 0
#define IS_TRIGGER 1

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

#define TR_OLD 0
#define TR_NEW 1

#define RDONLY 0
#define RD_INS 1
#define RD_UPD_ID 2
#define RD_UPD_VAL 3
#define QUICK  4

#define FRAME_ROWS  0 
#define FRAME_RANGE 1

#define EXCLUDE_NONE 0
#define EXCLUDE_CURRENT_ROW 1
#define EXCLUDE_GROUP 2
#define EXCLUDE_TIES 3
#define EXCLUDE_NO_OTHERS 4

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
	cmp_in = 8,
	cmp_notin = 9,

	/* The followin cmp_* are only used within stmt (not sql_exp) */
	cmp_all = 10,			/* special case for crossproducts */
	cmp_project = 11,		/* special case for projection joins */
	cmp_joined = 12, 		/* special case already joined */
	cmp_equal_nil = 13, 		/* special case equi join, with nil = nil */
	cmp_left = 14,			/* special case equi join, keep left order */
	cmp_left_project = 15		/* last step of outer join */
} comp_type;

/* for ranges we keep the requirment for symmetric */
#define CMP_SYMMETRIC 8

#define is_theta_exp(e) ((e) == cmp_gt || (e) == cmp_gte || (e) == cmp_lte ||\
		         (e) == cmp_lt || (e) == cmp_equal || (e) == cmp_notequal)

#define is_complex_exp(e) ((e&CMPMASK) == cmp_or || (e) == cmp_in || (e) == cmp_notin || (e&CMPMASK) == cmp_filter)

typedef enum commit_action_t { 
	CA_COMMIT, 	/* commit rows, only for persistent tables */
	CA_DELETE, 	/* delete rows */
	CA_PRESERVE,	/* preserve rows */
	CA_DROP,	/* drop table */
	CA_ABORT	/* abort changes, internal only */
} ca_t;

typedef int sqlid;

typedef struct sql_base {
	int wtime;
	int rtime;
	int allocated;
	int flag;
	int refcnt;
	sqlid id;
	char *name;
} sql_base;

extern void base_init(sql_allocator *sa, sql_base * b, sqlid id, int flag, const char *name);
extern void base_set_name(sql_base * b, const char *name);
extern void base_destroy(sql_base * b);

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
extern void cs_add_before(changeset * cs, node *n, void *elm);
extern void cs_del(changeset * cs, node *elm, int flag);
extern int cs_size(changeset * cs);
extern node *cs_find_name(changeset * cs, const char *name);
extern node *cs_find_id(changeset * cs, int id);
extern node *cs_first_node(changeset * cs);
extern node *cs_last_node(changeset * cs);
extern void cs_remove_node(changeset * cs, node *n);

typedef void *backend_code;
typedef size_t backend_stack;

typedef struct sql_trans {
	char *name;
	int stime;		/* read transaction time stamp */
	int wstime;		/* write transaction time stamp */
	int rtime;
	int wtime;
	int schema_number;	/* schema timestamp */
	int schema_updates;	/* set on schema changes */
	int status;		/* status of the last query */
	list *dropped;  	/* protection against recursive cascade action*/

	changeset schemas;

	sql_allocator *sa;	/* transaction allocator */

	struct sql_trans *parent;	/* multilevel transaction support */
	backend_stack stk;		
} sql_trans;

typedef struct sql_schema {
	sql_base base;
	int auth_id;
	int owner;
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

typedef struct sql_type {
	sql_base base;

	char *sqlname;
	unsigned int digits;
	unsigned int scale;	/* indicates how scale is used in functions */
	int localtype;		/* localtype, need for coersions */
	unsigned char radix;
	unsigned int bits;
	unsigned char eclass; 	/* types are grouped into equivalence classes */
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

#define F_FUNC 1
#define F_PROC 2
#define F_AGGR 3
#define F_FILT 4
#define F_UNION 5
#define F_ANALYTIC 6
#define F_LOADER 7

#define IS_FUNC(f) (f->type == F_FUNC)
#define IS_PROC(f) (f->type == F_PROC)
#define IS_AGGR(f) (f->type == F_AGGR)
#define IS_FILT(f) (f->type == F_FILT)
#define IS_UNION(f) (f->type == F_UNION)
#define IS_ANALYTIC(f) (f->type == F_ANALYTIC)
#define IS_LOADER(f) (f->type == F_LOADER)

#define FUNC_LANG_INT 0	/* internal */
#define FUNC_LANG_MAL 1 /* create sql external mod.func */
#define FUNC_LANG_SQL 2 /* create ... sql function/procedure */
#define FUNC_LANG_R   3 /* create .. language R */
#define FUNC_LANG_C   4 /* create .. language C */
#define FUNC_LANG_J   5
// this should probably be done in a better way
#define FUNC_LANG_PY  6 /* create .. language PYTHON */
#define FUNC_LANG_MAP_PY  7 /* create .. language PYTHON_MAP */
#define FUNC_LANG_PY2  8 /* create .. language PYTHON2 */
#define FUNC_LANG_MAP_PY2  9 /* create .. language PYTHON2_MAP */
#define FUNC_LANG_PY3  10 /* create .. language PYTHON3 */
#define FUNC_LANG_MAP_PY3  11 /* create .. language PYTHON3_MAP */
#define FUNC_LANG_CPP   12 /* create .. language CPP */

#define LANG_EXT(l)  (l>FUNC_LANG_SQL)

typedef struct sql_func {
	sql_base base;

	char *imp;
	char *mod;
	int type;
	list *ops;	/* param list */
	list *res;	/* list of results */
	int nr;
	int sql;	/* 0 native implementation
			   1 sql 
			   2 sql instantiated proc 
			*/
	int lang;
	char *query;	/* sql code */
	bit side_effect;
	bit varres;	/* variable output result */
	bit vararg;	/* variable input arguments */
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

typedef struct sql_subaggr {
	sql_func *aggr;
	list *res;
} sql_subaggr;

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

/* histogram types */
typedef enum sql_histype {
       X_EXACT,
       X_EQUI_WIDTH,
       X_EQUI_HEIGHT
} sql_histype;

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

#define isTable(x) 	  (x->type==tt_table)
#define isView(x)  	  (x->type==tt_view)
#define isMergeTable(x)   (x->type==tt_merge_table)
#define isStream(x)  	  (x->type==tt_stream)
#define isRemote(x)  	  (x->type==tt_remote)
#define isReplicaTable(x) (x->type==tt_replica_table)
#define isKindOfTable(x)  (isTable(x) || isMergeTable(x) || isRemote(x) || isReplicaTable(x))
#define isPartition(x)    (isTable(x) && x->p)

#define TABLE_WRITABLE	0
#define TABLE_READONLY	1
#define TABLE_APPENDONLY	2

typedef struct sql_part {
	sql_base base;
	struct sql_table *t; /* cached value */
} sql_part;

typedef struct sql_table {
	sql_base base;
	sht type;		/* table, view, etc */
	sht access;		/* writable, readonly, appendonly */
	bit system;		/* system or user table */
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
	struct sql_table *p;	/* The table is part of this merge table */
	struct sql_table *po;	/* the outer transactions table */
} sql_table;

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
	int query_type;
	int nr_cols;
	int cur_col;
	char *tsep;
	char *rsep;
	char *ssep;
	char *ns;
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
	int active;		/* active transaction */
	int status;		/* status, ok/error */
	backend_stack stk;
} sql_session;

extern void schema_destroy(sql_schema *s);
extern void table_destroy(sql_table *t);
extern void column_destroy(sql_column *c);
extern void kc_destroy(sql_kc *kc);
extern void key_destroy(sql_key *k);
extern void idx_destroy(sql_idx * i);

extern int base_key(sql_base *b);
extern node *list_find_name(list *l, const char *name);
extern node *list_find_id(list *l, int id);
extern node *list_find_base_id(list *l, int id);

extern sql_key *find_sql_key(sql_table *t, const char *kname);

extern sql_idx *find_sql_idx(sql_table *t, const char *kname);

extern sql_column *find_sql_column(sql_table *t, const char *cname);

extern sql_table *find_sql_table(sql_schema *s, const char *tname);
extern sql_table *find_sql_table_id(sql_schema *s, int id);
extern node *find_sql_table_node(sql_schema *s, int id);

extern sql_sequence *find_sql_sequence(sql_schema *s, const char *sname);

extern sql_schema *find_sql_schema(sql_trans *t, const char *sname);
extern sql_schema *find_sql_schema_id(sql_trans *t, int id);
extern node *find_sql_schema_node(sql_trans *t, int id);

extern sql_type *find_sql_type(sql_schema * s, const char *tname);
extern sql_type *sql_trans_bind_type(sql_trans *tr, sql_schema *s, const char *name);
extern node *find_sql_type_node(sql_schema *s, int id);

extern sql_func *find_sql_func(sql_schema * s, const char *tname);
extern list *find_all_sql_func(sql_schema * s, const char *tname, int type);
extern sql_func *sql_trans_bind_func(sql_trans *tr, const char *name);
extern sql_func *sql_trans_find_func(sql_trans *tr, int id);
extern node *find_sql_func_node(sql_schema *s, int id);

typedef struct {
	BAT *b;
	char* name;
	void* def;
} sql_emit_col;

#endif /* SQL_CATALOG_H */
