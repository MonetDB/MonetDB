/**
 * @file
 *
 * Declarations specific to M5 SQL algebra.
 *
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

#ifndef MSA_H
#define MSA_H

#include "array.h"

#include "properties.h"

#include "algebra.h"

#include "msa_mnemonic.h"

/* --------------- M5 SQL algebra expressions --------------- */

/* M5 SQL algebra expression kinds */
enum PFmsa_expr_kind_t{
      msa_expr_func             = 1
    , msa_expr_num_gen          = 3
    , msa_expr_comp             = 2
    , msa_expr_aggr             = 8
    , msa_expr_convert          = 9
    , msa_expr_column           = 10
    , msa_expr_atom             = 11
};
typedef enum PFmsa_expr_kind_t PFmsa_expr_kind_t;

/* M5 SQL algebra number generating function kinds
 (for la operators rank, rowrank and rownum) */
enum PFmsa_num_gen_kind_t {
      msa_num_gen_rank     = 1
    , msa_num_gen_rowrank  = 2
    , msa_num_gen_rownum   = 3
    , msa_num_gen_rowid    = 4
};
typedef enum PFmsa_num_gen_kind_t PFmsa_num_gen_kind_t;

/* M5 SQL algebra comparison function kinds */
enum PFmsa_comp_kind_t {
      msa_comp_equal            = 1
    , msa_comp_notequal         = 2
    , msa_comp_lt               = 4
    , msa_comp_lte              = 5
    , msa_comp_gt               = 6
    , msa_comp_gte              = 7
};
typedef enum PFmsa_comp_kind_t PFmsa_comp_kind_t;


/* M5 SQL algebra aggregation function kinds */
enum PFmsa_aggr_kind_t {
      msa_aggr_count                = 1
    , msa_aggr_sum                  = 2
    , msa_aggr_avg                  = 3
    , msa_aggr_max                  = 4
    , msa_aggr_min                  = 5
    , msa_aggr_dist                 = 6         /* aggregate of a column that functionally
                                                depends on the partitioning column */
    , msa_aggr_seqty1               = 7         /* sequence type matching for `1' occurrence */
    , msa_aggr_all                  = 8         /* all existential quantifier */
    , msa_aggr_prod                 = 9         /* product aggregate */
};
typedef enum PFmsa_aggr_kind_t PFmsa_aggr_kind_t;

/* M5 SQL algebra function name space */
enum PFmsa_expr_func_name {
      msa_func_add         = 1
    , msa_func_sub         = 2
    , msa_func_mult        = 4
    , msa_func_div         = 5
    , msa_func_mod         = 6
    , msa_func_and         = 7
    , msa_func_or          = 8
    , msa_func_not         = 9
};
typedef enum PFmsa_expr_func_name PFmsa_expr_func_name;



/* semantic content in expressions */
union PFmsa_expr_sem_t {

    struct {
        PFmsa_expr_func_name    name;
    } func;
    
    struct {
        /* number generating functions: rownum, rowrank and rank */
        PFmsa_num_gen_kind_t    kind;
        PFmsa_exprlist_t        *sort_cols;      /* sort columns for rownum, rowrank and rank op */
        PFmsa_exprlist_t        *part_cols;      /* partitioning columns for rownum op */
    } num_gen;
    
    struct {
        PFmsa_comp_kind_t       kind;
    } comp;

    struct {
        PFmsa_aggr_kind_t       kind;
    } aggr;
    
    struct {
        PFalg_col_t             old;
    } col;
    
    struct {
        PFalg_atom_t            atom;
    } atom;

};
typedef union PFmsa_expr_sem_t PFmsa_expr_sem_t;

/* maximum number of children of a #PFla_op_t node */
#define PFMSA_EXPR_MAXCHILD 2

/* M5 SQL expression node */
struct PFmsa_expr_t {

    PFmsa_expr_kind_t    kind;               /* operator kind */
    PFmsa_expr_sem_t     sem;                /* expression semantics */
    struct PFmsa_expr_t *child[PFMSA_EXPR_MAXCHILD];     /* child list */

    PFalg_col_t          col;
    PFalg_simple_type_t  type;

    unsigned int         refctr;

    unsigned             bit_reset:1;        /* used to reset the dag bit
                                                    in a DAG traversal */
    unsigned             bit_dag:1;          /* enables DAG traversal */
    int                  node_id;            /* specifies the id of this operator
                                                  node; required exclusively to
                                                  create dot output. */
    bool                sortorder;          /* sortorder: false = ascending (DIR_ASC in algebra.h), 
                                             true = descending (DIR_DESC in algebra.h) */
};
typedef struct PFmsa_expr_t PFmsa_expr_t;


/* --------------- M5 SQL algebra operators --------------- */

/* M5 SQL algebra operator kinds */
enum PFmsa_op_kind_t{
      msa_op_serialize_rel      = 1
    , msa_op_project            = 2
    , msa_op_select             = 3
    , msa_op_literal_table      = 4
    , msa_op_table              = 5
    , msa_op_union              = 6
    , msa_op_except             = 7
    , msa_op_groupby            = 8
    , msa_op_join               = 9
    , msa_op_semijoin           = 10
    , msa_op_cross              = 11
    , msa_op_nil_node           = 12
};
typedef enum PFmsa_op_kind_t PFmsa_op_kind_t;

/* semantic content in algebra operators */
union PFmsa_op_sem_t {

    /* semantic content for serialization operator */
    struct {
        PFalg_col_t         iter;                           /* name of column iter */
        PFalg_col_t         pos;                            /* name of column pos */
        PFalg_collist_t     *items;                         /* list of item columns */
    } ser_rel;

    /* semantic content for projection operator */
    struct {
        PFmsa_exprlist_t    *expr_list;
    } proj;

    /* semantic content for select operator */
    struct {
        PFmsa_exprlist_t    *expr_list;                     /* list of conjunctions */
    } select;

    /* semantic content for literal table operator */
    struct {
        unsigned int          tuples_count;                 /* count of tuples in tuple list */
        PFalg_tuple_t         *tuples;                      /* tuple list */
    } literal_table;

    /* semantic content for table operator */
    struct {
        char*               name;
        PFmsa_exprlist_t    *expr_list;
        PFmsa_exprlist_t    *col_names;                     /* list of expressions containing
                                                             original column names */
    } table;

    /* semantic content for groupby operator */
    struct {
        PFmsa_exprlist_t    *grp_list;                 /* grouping criteria list */
        
        PFmsa_exprlist_t    *prj_list;                      /* projection list */
    } groupby;

    /* semantic content for join operator */
    /* semantic content for semijoin operator */
    struct {
        PFmsa_exprlist_t    *expr_list;                     /* list of conjunctions */
    } join;
};
typedef union PFmsa_op_sem_t PFmsa_op_sem_t;


/* maximum number of children of a #PFla_op_t node */
#define PFMSA_OP_MAXCHILD 2

/* M5 SQL algebra operator node */
struct PFmsa_op_t {

    PFmsa_op_kind_t    kind;                            /* operator kind */
    PFmsa_op_sem_t     sem;                             /* operator semantics */
    PFalg_schema_t     schema;                          /* result schema */
    struct PFmsa_op_t *child[PFMSA_OP_MAXCHILD];        /* child list */

    unsigned int       refctr;

    bool               distinct;
    
    unsigned           bit_reset:1;         /* used to reset the dag bit
                                             in a DAG traversal */
    unsigned           bit_dag:1;           /* enables DAG traversal */
    int                node_id;             /* specifies the id of this operator
                                            node; required exclusively to
                                             create dot output. */
};
typedef struct PFmsa_op_t PFmsa_op_t;

/* Annotation for translation (typedef see logical.h) */
struct PFmsa_ann_t {
    PFmsa_op_t          *op;
    PFmsa_exprlist_t    *prj_list;
    PFmsa_exprlist_t    *sel_list;
};

/* -------------------- Auxiliary functions --------------------*/

/* Auxiliary function to get schema from a list of expressions 
   (needed in lalg2msa.c) */
PFalg_schema_t schema_from_exprlist(PFmsa_exprlist_t *expr_list);

/* --------------- Constructor stubs for expressions --------------- */

/* Contructor for function expression */
PFmsa_expr_t * PFmsa_expr_func (PFmsa_expr_func_name name, PFmsa_exprlist_t *child_list,
                                PFalg_col_t res_col);

/* Contructor for number generating function expression */
PFmsa_expr_t * PFmsa_expr_num_gen (PFmsa_num_gen_kind_t kind, PFmsa_exprlist_t *child_list,
                                   PFalg_col_t res_col);

/* Contructor for comparison expression */
PFmsa_expr_t * PFmsa_expr_comp (PFmsa_expr_t *n1, PFmsa_expr_t *n2, PFmsa_comp_kind_t kind, 
                                      PFalg_col_t res_col);

/* Contructor for aggr expression */
PFmsa_expr_t * PFmsa_expr_aggr (PFmsa_expr_t *n, PFmsa_aggr_kind_t aggr_kind, 
                                PFalg_col_t res_col);

/* Contructor for convert expression */
PFmsa_expr_t * PFmsa_expr_convert (PFmsa_expr_t *n, PFalg_simple_type_t type,
                                   PFalg_col_t res_col);

/* Contructor for column expression */
PFmsa_expr_t * PFmsa_expr_column (PFalg_col_t col, PFalg_simple_type_t type);

/* Contructor for atom expression */
PFmsa_expr_t * PFmsa_expr_atom (PFalg_col_t res_col, PFalg_atom_t atom);


/* --------------- Constructor stubs for operators --------------- */

/* Constructor for nil node operator */
PFmsa_op_t * PFmsa_op_nil_node (void);

/* Contructor for serialize operator */
PFmsa_op_t * PFmsa_op_serialize_rel (PFmsa_op_t *DAG, PFmsa_op_t *side_effects,
                                  PFalg_col_t iter, PFalg_col_t pos,
                                  PFalg_collist_t *items);

/* Contructor for project operator */
PFmsa_op_t * PFmsa_op_project (PFmsa_op_t *n, bool distinct_flag, PFmsa_exprlist_t *expr_list);

/* Contructor for select operator */
PFmsa_op_t * PFmsa_op_select (PFmsa_op_t *n, PFmsa_exprlist_t *expr_list);

/* Contructor for literal table operator */
PFmsa_op_t * PFmsa_op_literal_table (PFalg_schema_t names, unsigned int tuples_count, 
                                  PFalg_tuple_t *tuples);

/* Contructor for table operator */
PFmsa_op_t * PFmsa_op_table (char* name, PFalg_schema_t schema, PFmsa_exprlist_t *expr_list,
                             PFmsa_exprlist_t *col_names);

/* Contructor for union operator */
PFmsa_op_t * PFmsa_op_union (PFmsa_op_t *n1, PFmsa_op_t *n2);

/* Contructor for except operator */
PFmsa_op_t * PFmsa_op_except (PFmsa_op_t *n1, PFmsa_op_t *n2);

/* Contructor for groupby operator */
PFmsa_op_t * PFmsa_op_groupby (PFmsa_op_t *n, PFmsa_exprlist_t *grp_list, 
                            PFmsa_exprlist_t *prj_list);

/* Contructor for join operator */
PFmsa_op_t * PFmsa_op_join (PFmsa_op_t *n1, PFmsa_op_t *n2, PFmsa_exprlist_t *expr_list);

/* Contructor for semijoin operator */
PFmsa_op_t * PFmsa_op_semijoin (PFmsa_op_t *n1, PFmsa_op_t *n2, PFmsa_exprlist_t *expr_list);

/* Contructor for cross operator */
PFmsa_op_t * PFmsa_op_cross (PFmsa_op_t *n1, PFmsa_op_t *n2);

/* --------------- Set distinct flag in operator --------------- */
PFmsa_op_t * PFmsa_distinct(PFmsa_op_t *op);

#endif  /* MSA_H */

/* vim:set shiftwidth=4 expandtab: */
