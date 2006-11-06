/**
 * Fri Oct 27 13:26:38 CEST 2006, Manuel Mayr
 */

/**
 * @file
 *
 * Declarations regarding the transformation from logical algebra to sql. 
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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef __SQL_H__
#define __SQL_H__


#include "algebra.h"

/* ........................ type definitions ...............................*/

/* type to count the tuples in a table */
typedef unsigned int PFsql_tuple_count_t;
/* type to count the atoms in a tuple */
typedef unsigned int PFsql_atom_count_t;
/* type to count the schema items */
typedef unsigned int PFsql_schema_item_count_t;
/* type to count attributes */
typedef unsigned int PFsql_att_count_t;

/* sql schema attribute names */
enum PFsql_att_t { 
    sql_att_iter,
    sql_att_item,
    sql_att_item1
};
typedef enum PFsql_att_t PFsql_att_t;

/** list of attributes */
struct PFsql_attlist_t {
   PFsql_att_count_t    count;   /**< number of attributes */
   PFsql_att_t          *atts;   /**< array that holds the list items */
};
typedef struct PFsql_attlist_t PFsql_attlist_t; 

/* types in SQL does not know polymorphic types,
 * therefore we use an enum to represent simple
 * types in sql
 */
// TODO define types
enum PFsql_simple_type_t {
    sql_type_integer = 1
};
typedef enum PFsql_simple_type_t PFsql_simple_type_t;

struct PFsql_schema_item_t {
    PFsql_att_t name;
    PFsql_simple_type_t type;   
};
typedef struct PFsql_schema_item_t PFsql_schema_item_t;

struct PFsql_schema_t {
    PFsql_schema_item_count_t  count;
    PFsql_schema_item_t        *items;
};
typedef struct PFsql_schema_t PFsql_schema_t;

union PFsql_atom_val_t {
   int      int_;    /**< value for integer atoms */
   double   dbl_;    /**< value for double atoms */
   bool     bln_;    /**< value for boolean atoms */
};
typedef union PFsql_atom_val_t PFsql_atom_val_t;


/* an atom is a value with simple type in SQL */
struct PFsql_atom_t {
    PFsql_simple_type_t    type;   
    PFsql_atom_val_t       val;
};
typedef struct PFsql_atom_t PFsql_atom_t;

/* a tuple is an array of atoms, with length specified in count */
struct PFsql_tuple_t {
    PFsql_atom_count_t  count;
    PFsql_atom_t        *atoms; 
};
typedef struct PFsql_tuple_t PFsql_tuple_t;

/* .............. sql operators (relational operators) .................... */

/** sql operator kinds */
enum PFsql_op_kind_t {
   sql_tmp_tbl          = 1
};
typedef enum PFsql_op_kind_t PFsql_op_kind_t;

/** semantic content in sql operators */
union PFsql_op_sem_t {
    struct {
        PFsql_tuple_count_t   count;   /**< number of tuples */
        PFsql_tuple_t         *tuples; /**< array holding the tuples */               
    } tmp_tbl;
    
};
typedef union PFsql_op_sem_t PFsql_op_sem_t; 

/* each node has at most two childs */
#define PFSQL_OP_MAXCHILD 2

/** sql operator node */
struct PFsql_op_t {
    PFsql_op_kind_t     kind;       /**< operator kind */
    PFsql_op_sem_t      sem;        /**< semantic content for this operator */
    //PFalg_schema_t      schema;     /**< result schema */

    struct PFsql_op_t   *child[PFSQL_OP_MAXCHILD];
};
typedef struct PFsql_op_t PFsql_op_t;

/************************** Constructors **********************/

/**
 * Construct literal integer atom.
 */
PFsql_atom_t PFsql_lit_int(long long int value);

/**
 * Construct an attribute list.
 */
#define PFsql_attlist(...) \
PFsql_attlist_ ( (sizeof( (PFsql_att_t[]) { __VA_ARGS__ } ) \
            / sizeof( PFsql_att_t ) ), \
            (PFsql_att_t[]) { __VA_ARGS__ } ) 
PFsql_attlist_t PFsql_attlist_(PFsql_att_count_t count, PFsql_att_t *atts);

/**
 * Construct temporary table, that is not gonna be stored in the database.
 */
PFsql_op_t* PFsql_tmp_tbl_(PFalg_attlist_t a,
                                unsigned int count, PFalg_tuple_t *tuples);

/************************* Translation ************************/

/**
 * Convert an algebra schema item to a SQL specific schema.
 */
PFsql_schema_item_t PFsql_alg_schmitm_conv(PFalg_schm_item_t item);

/**
 * Convert an algeba schema to a SQL specific  schema.
 */
PFsql_schema_t PFsql_alg_schema_conv(PFalg_schema_t schema);

#endif /* __SQL_H__ */

/* vim:set shiftwidth=4 expandtab: */
