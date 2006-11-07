/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Functions related to sql tree construction.
 */

/*
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

#include "pathfinder.h"
#include "sql.h"
#include "algebra.h"
#include "mem.h"
#include "oops.h"

#include <stdio.h>

#define DEBUG

static PFsql_op_t* PFsql_op_leaf (PFsql_op_kind_t kind);

/***************** Constructors ******************/

/*
 * Construct literal integer atom.
 *
 * @param value   Integer value of the atom.
 */
PFsql_atom_t
PFsql_lit_int(long long int value)
{
    return (PFsql_atom_t) {
        .type = sql_type_integer,
        .val = (PFsql_atom_val_t) {
            .int_ = value
        }
    };
}

/**
 * Constructor for attribute lists.
 *
 * @param count   Number of array elements that follow.
 * @param atts    Array of attribute names.
 *                Has to be exactly @a count elements
 *                long.
 *
 * @note
 *    Typically you want to use this function directly.
 *    Use the wrapper macro #PFsql_attlist() (or its abbreviation
 *    #attlist(), defined in sql_mnemonic) instead. It will
 *    determine @a count on its own, so you only have to pass an
 *    arbitrary number of attribute names.
 */
PFsql_attlist_t
PFsql_attlist_(PFsql_att_count_t count, PFsql_att_t *atts)
{
    PFsql_attlist_t  ret;
    unsigned int     i;

    ret  = (PFsql_attlist_t) {
        .count = count,
        .atts  = PFmalloc( count * sizeof( PFsql_att_t ) )
    };

    for ( i = 0; i < count; i++ )
        ret.atts[i] = atts[i];

    return ret;
}

/**
 * Construct an algebra node representing a temporary table
 * processed in the main memory an that is not gonna be stored
 * in the database. The constructor takes a list of attributes
 * and a list of tuples.
 *
 * @param attlist Attribute list of the literal table.
 * @param count   Number of tuples that follow.
 * @param tuples  Tuples of this literal table.
 *                This array has to be exactly @a count
 *                items long.
 */
PFsql_op_t*
PFsql_tmp_tbl_(PFalg_attlist_t a,
        PFsql_tuple_count_t count, PFalg_tuple_t *tuples)
{
    PFsql_op_t    *ret;       /* node we want to return */
    a = a;
    count = count;
    tuples = tuples;
    /* create a new sql operator leaf */
    ret = PFsql_op_leaf( sql_tmp_tbl );

    return ret;
}

/**
 * Convert an algebra attribute name to an sql attribute name.
 *
 * @param att Algebra attribute to convert.
 */
static PFsql_att_t
PFsql_alg_att_conv(PFalg_att_t att)
{
    PFsql_att_t ret;

#ifdef DEBUG
    printf("alg_att_conv processing ... ");
#endif
    
    switch(att) {
        case att_iter:
        {
            ret = sql_att_iter;
        } break;
        case att_item:
        {
            ret = sql_att_item;
        } break;
        case att_item1:
        {
            ret = sql_att_item1;
        } break;
        default:
        {
            PFoops(OOPS_FATAL,
                    "This attribute is not supported by pathfinder." );
        }
    }

#ifdef DEBUG
    printf("Conversion successful.\n");
#endif

    return ret;
}

/**
 * Converts an algebra schema item into an SQL 
 * specific schema item.
 *
 * @param item Algebra schema item to convert.
 */
PFsql_schema_item_t
PFsql_alg_schmitm_conv(PFalg_schm_item_t item)
{
    PFsql_schema_item_t ret;

    ret.name = PFsql_alg_att_conv(item.name);

    return ret;
}

/**
 * Convert an algebra schema to an SQL specific
 * schema.
 *
 * @param schema Algebra schema item to convert.
 */
PFsql_schema_t
PFsql_alg_schema_conv(PFalg_schema_t schema)
{
   PFsql_schema_t ret;
   /* unsigned int i; */
   ret  = (PFsql_schema_t) {
        .count = schema.count,
        .items = PFmalloc( schema.count * 
           sizeof( PFsql_schema_item_t ) )
   };
   
   
   /*for( i = 0; i < schema.count; i++ ) {
      test = PFsql_alg_schmitm_conv(schema.items[i]);
   }*/
   return ret;
}

/**
 * Create an sql operator (leaf) node.
 *
 * @param kind Kind of the operator node.
 *
 * @note
 *    Allocates memory and initializes fields of an 
 *    sql operator. The node has the kind @a kind.
 */
static PFsql_op_t*
PFsql_op_leaf (PFsql_op_kind_t kind)
{
    unsigned int i;

    /* node we want to return */
    PFsql_op_t *ret = PFmalloc( sizeof( PFsql_op_t ) );

    ret->kind = kind;
    
    /* initialize childs */
    for( i = 0; i < PFSQL_OP_MAXCHILD; i++ )
        ret->child[i] = NULL;

    return ret;
}

