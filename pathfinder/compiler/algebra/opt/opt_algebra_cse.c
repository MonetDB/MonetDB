/**
 * @file
 *
 * Common subexpression elimination on the logical algebra.
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

/* always include pf_config.h first! */
#include "pf_config.h"
#include "pathfinder.h"

#include "oops.h"
#include "mem.h"
#include "opt_algebra_cse.h"
#include "array.h"
#include "alg_dag.h"
#include "algebra.h"
#include "logical.h"
#include "logical_mnemonic.h"
#include "ordering.h"

#include <assert.h>
#include <string.h> /* strcmp */
#include <stdio.h>

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/* prune already checked nodes           */
#define SEEN(p)      ((p)->bit_dag)

/* checks if an column is NULL */
#define IS_NULL(a) (a == col_NULL)

#define DEF_WIDTH 10

/* Operator names */
static char *ID[] = {
      [la_serialize_seq]   = "la_serialize_seq"
    , [la_serialize_rel]   = "la_serialize_rel"
    , [la_side_effects]    = "la_side_effects"
    , [la_lit_tbl]         = "la_lit_tbl"
    , [la_empty_tbl]       = "la_empty_tbl"
    , [la_ref_tbl]         = "la_ref_tbl"
    , [la_attach]          = "la_attach"
    , [la_cross]           = "la_cross"
    , [la_eqjoin]          = "la_eqjoin"
    , [la_semijoin]        = "la_semijoin"
    , [la_thetajoin]       = "la_thetajoin"
    , [la_project]         = "la_project"
    , [la_select]          = "la_select"
    , [la_pos_select]      = "la_pos_select"
    , [la_disjunion]       = "la_disjunion"
    , [la_intersect]       = "la_intersect"
    , [la_difference]      = "la_difference"
    , [la_distinct]        = "la_distinct"
    , [la_fun_1to1]        = "la_fun_1to1"
    , [la_num_eq]          = "la_num_eq"
    , [la_num_gt]          = "la_num_gt"
    , [la_bool_and]        = "la_bool_and"
    , [la_bool_or]         = "la_bool_or"
    , [la_bool_not]        = "la_bool_not"
    , [la_to]              = "la_to"
    , [la_aggr]            = "la_aggr"
    , [la_rownum]          = "la_rownum"
    , [la_rowrank]         = "la_rowrank"
    , [la_rank]            = "la_rank"
    , [la_rowid]           = "la_rowid"
    , [la_type]            = "la_type"
    , [la_type_assert]     = "la_type_assert"
    , [la_cast]            = "la_cast"
    , [la_step]            = "la_step"
    , [la_step_join]       = "la_step_join"
    , [la_guide_step]      = "la_guide_step"
    , [la_guide_step_join] = "la_guide_step_join"
    , [la_doc_index_join]  = "la_doc_index_join"
    , [la_doc_tbl]         = "la_doc_tbl"
    , [la_doc_access]      = "la_doc_access"
    , [la_twig]            = "la_twig"
    , [la_fcns]            = "la_fcns"
    , [la_element]         = "la_element"
    , [la_textnode]        = "la_textnode"
    , [la_comment]         = "la_comment"
    , [la_processi]        = "la_processi"
    , [la_content]         = "la_content"
    , [la_merge_adjacent]  = "la_merge_adjacent"
    , [la_roots]           = "la_roots"
    , [la_frag_union]      = "la_frag_union"
    , [la_error]           = "la_error"
    , [la_nil]             = "la_nil"
    , [la_cache]           = "la_cache"
    , [la_trace]           = "la_trace"
    , [la_trace_items]     = "la_trace_items"
    , [la_trace_msg]       = "la_trace_msg"
    , [la_trace_map]       = "la_trace_map"
    , [la_rec_fix]         = "la_rec_fix"
    , [la_rec_param]       = "la_rec_param"
    , [la_rec_arg]         = "la_rec_arg"
    , [la_rec_base]        = "la_rec_base"
    , [la_fun_call]        = "la_fun_call"
    , [la_fun_param]       = "la_fun_param"
    , [la_fun_frag_param]  = "la_fun_frag_param"
    , [la_proxy]           = "la_proxy"
    , [la_proxy_base]      = "la_proxy_base"
    , [la_internal_op]     = "!!!INTERNAL OP!!!"
    , [la_string_join]     = "la_string_join"
    , [la_dummy]           = "la_dummy"
};

/**
 * maps original subtrees to their corresponding cse subtrees
 */
static PFarray_t *cse_map;
/**
 *maps ori subtrees to their corresponding actual columns
 */
static PFarray_t *actcol_map;
/**
 * maps cse subtrees to their original subtrees, triggered the
 * creation of the cse node
 */
static PFarray_t *ori_map;

/**
 * Subexpressions that we already saw.
 *
 * This is an array of arrays. We create a separate for
 * each algebra node kind we encounter. This speeds up
 * lookups when we search for an existing algebra node.
 */
static PFarray_t *subexps;

/* struct to map the original operators
 * to those generated during the CSE.
 */
struct ori_cse_map_t {
    PFla_op_t *ori;
    PFla_op_t *cse;
};
typedef struct ori_cse_map_t ori_cse_map_t;

/* structure to map the original column/type
 * pair to the effective column/type pair
 */
struct actcol_map_t {
    PFalg_col_t ori_col;
    PFalg_col_t act_col;
};
typedef struct actcol_map_t actcol_map_t;

/* structure to map the original operator to
 * their effective columns.
 */
struct ori_actcol_map_t {
    PFla_op_t *ori;
    PFarray_t *actcols;
};
typedef struct ori_actcol_map_t ori_actcol_map_t;

/* structure tp map the operators in the CSE plan
 * to their original operators who caused the insertion.
 */
struct cse_ori_map_t {
    PFla_op_t *cse;
    PFla_op_t *ori;
};
typedef struct cse_ori_map_t cse_ori_map_t;

/*--------------------------------------------------------------*/
/*-------------------- Helper Functions ------------------------*/
/*--------------------------------------------------------------*/

/*------------- CSE Map -------------*/

/* lookup operator in already generated  *
 * CSE parts and return the              *
 * corresponding operator in the CSE     *
 * plan                                  */
#ifndef NDEBUG
/* says us exactly where the lookup fails in the code */
#define CSE(p)       (assert(lookup_cse((cse_map),(p)) != col_NULL), \
                      lookup_cse((cse_map),(p)))
#else
#define CSE(p)       (lookup_cse((cse_map),(p)))
#endif

/* creates mapping of original operator   *
 * and CSE operator                      */
#define INCSE(o,c)   (insert_cse((cse_map), (o), (c)))

/**
 * Worker for 'CSE(p)': based on the original
 * subtree looks up the corresponding subtree
 * with already generated CSE subtrees.
 * When ori is NULL return NULL.
 */
static PFla_op_t *
lookup_cse (PFarray_t *map, PFla_op_t *ori)
{
    assert (map);
    if (ori == NULL) return NULL;

    /* run through the list and search the original node */
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((ori_cse_map_t *) (PFarray_at (map, i)))->ori == ori)
            return ((ori_cse_map_t *) PFarray_at (map, i))->cse;
    }

#ifndef NDEBUG
    return NULL;
#else
    PFoops (OOPS_FATAL, "Could not look up node (%s) (cse subtrees)",
            ID[ori->kind]);

    return NULL; /* satisfy picky compilers */
#endif
}

/**
 * Worker for 'INCSE(o,c)'.
 * Inserts a mapping from the original nodes
 * to their equivalents in the CSE plan.
 * Remapping an operator is not allowed.
 */
static void
insert_cse (PFarray_t *map, PFla_op_t *ori,
            PFla_op_t *cse)
{
    assert (map);
    assert (ori);
    assert (cse);

    /* check if the operator has already a mapping */
    ori_cse_map_t *temp = NULL;
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((ori_cse_map_t *) (PFarray_at (map, i)))->ori == ori) {
            temp = (ori_cse_map_t *) PFarray_at (map, i);
            break;
        }
    }

    /* the operator is already mapped to a
     * CSE operator */
    if (temp) {
        PFoops (OOPS_FATAL, "No operator should be remapped.");
    }

    /* insert the operator */
    *(ori_cse_map_t *) PFarray_add (map) =
         (ori_cse_map_t)
         {
             .ori = ori,
             .cse = cse
         };
}

/*------------- ORI Map -------------*/

/* reverse operations to CSE and INCSE   */
#ifndef NDEBUG
/* says us exactly where the lookup fails in the code */
#define ORI(p)       (assert (lookup_ori((ori_map), (p)) != NULL), \
                      lookup_ori ((ori_map), (p)))
#else
#define ORI(p)       (lookup_ori((ori_map),(p)))
#endif

#define INORI(c,o)   (insert_ori((ori_map), (c), (o)))

/**
 * Worker for 'ORI(p)': based on the cse subtree
 * looks up the corresponding subtree in the
 * original plan, caused the insertion.
 */
static PFla_op_t*
lookup_ori (PFarray_t *map, PFla_op_t *cse)
{
    assert (map);
    assert (cse);

    /* run through the list and search the cse node */
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((cse_ori_map_t *) (PFarray_at (map, i)))->cse == cse)
            return ((cse_ori_map_t *) PFarray_at (map, i))->ori;
    }

#ifndef NDEBUG
    return NULL;
#else
    PFoops (OOPS_FATAL, "Could not look up node (%s) (ori subtrees)",
            ID[cse->kind]);

    return NULL; /* satisfy picky compilers */
#endif
}

/* Worker for 'INORI(c,o)'.
 * Insert a structure that maps the operator in the
 * cse tree to its original operator, triggered the
 * insertion.
 * Remapping is not allowed.
 */
static void
insert_ori (PFarray_t *map, PFla_op_t *cse,
            PFla_op_t *ori)
{
    assert (map);
    assert (cse);
    assert (ori);

    /* integrity check if the operator has already a mapping */
    cse_ori_map_t *temp = NULL;
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((cse_ori_map_t *) (PFarray_at (map, i)))->cse == cse) {
            temp = (cse_ori_map_t *) PFarray_at (map, i);
            break;
        }
    }

    /* remapping is not possible */
    if (temp) {
        PFoops (OOPS_FATAL, "No operator should be remapped.");
    }

    *(cse_ori_map_t *) PFarray_add (map) =
        (cse_ori_map_t)
        {
            .cse = cse,
            .ori = ori
        };
}

/*--------- Actual Attributes -------*/

/* to ensure the structural equivalence  *
 * of operators we annotate each         *
 * operator of the original plan with a  *
 * mapping list of their original and    *
 * actual columns of CSE plan.           */
#ifndef NDEBUG
/* says us exactly where the lookup fails in the code */
#define ACT(p)       (assert (lookup_actcols((actcol_map),(p)) != NULL), \
                      lookup_actcols((actcol_map),(p)))
#else
#define ACT(p)       (lookup_actcols((actcol_map),(p)))
#endif

#define INACT(o,e)   (insert_actcols((actcol_map), (o), (e)))

/**
 * Worker for 'INACT(o,p)'.
 * Annotates an operator @a ori with
 * a projection-list.
 */
static void
insert_actcols (PFarray_t *map, PFla_op_t *ori, PFarray_t *proj)
{
    assert (map);
    assert (ori);
    assert (proj);

    /* check if the operator has already a mapping */
    ori_actcol_map_t *temp = NULL;
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((ori_actcol_map_t *) (PFarray_at (map, i)))->ori == ori) {
            temp = (ori_actcol_map_t *) PFarray_at (map, i);
            break;
        }
    }

    if (temp) {
        PFoops (OOPS_FATAL,
                "No operator should be remapped");
    }

    *(ori_actcol_map_t *) PFarray_add (map) =
        (ori_actcol_map_t)
        {
            .ori = ori,
            .actcols = proj
        };
}

#if 0

static void
print_actmap (PFarray_t *actmap)
{
    fprintf (stderr, "actmap --->\n");
    for (unsigned int i = 0; i < PFarray_last (actmap); i++) {
        fprintf (stderr, "act_item: %s ---> %s\n",
                 PFcol_str ((*(actcol_map_t *)
                            PFarray_at (actmap, i)).ori_col),
                 PFcol_str ((*(actcol_map_t *)
                            PFarray_at (actmap, i)).act_col));
    }
    fprintf (stderr, "end --->\n");
}

static void
print_literal (PFalg_atom_t atom)
{
    switch (atom.type) {
        /* if type is nat, compare nat member of union */
        case aat_nat:
            fprintf (stdout, "%i", atom.val.nat_);
            break;
        /* if type is int, compare int member of union */
        case aat_int:
            fprintf (stdout, "%i", atom.val.int_);
            break;
        /* if type is str, compare str member of union */
        case aat_uA:
        case aat_str:
            fprintf (stdout, "%s", atom.val.str);
            break;
        /* if type is float, compare float member of union */
        case aat_dec:
            fprintf (stdout, "%f", atom.val.dec_);
            break;
        /* if type is double, compare double member of union */
        case aat_dbl:
            fprintf (stdout, "%d", atom.val.dbl);
            break;
        /* if type is double, compare double member of union */
        case aat_bln:
            fprintf (stdout, "%i", atom.val.bln);
            break;
        case aat_qname:
            fprintf (stdout, "%s", atom.val.qname);
            break;
        /* anything else is actually bogus (e.g. there are no
         * literal nodes */
        default:
        {
            PFinfo (OOPS_WARNING, "literal value that do not make sense");
        } break;
    }
}
#endif

/**
 * Create a new unique name, based on the used
 * cols in @a n schema.
 */
static PFalg_col_t
create_unq_name (PFalg_schema_t schema, PFalg_col_t col)
{
    for (unsigned int i = 0; i < schema.count; i++)
        if (schema.items[i].name == col)
            return PFcol_new (col);
    return col;
}

/**
 * Worker for 'ACT(p)': based on the original
 * subtree looks up the corresponding subtree
 * with already annotated effective columns.
 */
static PFarray_t *
lookup_actcols (PFarray_t *map, PFla_op_t *ori)
{
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((ori_actcol_map_t *) (PFarray_at (map, i)))->ori == ori)
            return ((ori_actcol_map_t *) PFarray_at (map, i))->actcols;
    }

#ifndef NDEBUG
    return NULL;
#else
    PFoops (OOPS_FATAL, "actual columns of the "
                        "%s operator not found", ID[ori->kind]);

    return NULL; /* satisfy picky compilers */
#endif
}

/**
 * Creates a new projection list.
 */
static PFarray_t *
create_actcol_map (void)
{
    return PFarray (sizeof(actcol_map_t), DEF_WIDTH);
}

/*........... Handling of actual columns .........*/

#ifndef NDEBUG
#define ACTCOL(n, col)  (assert (act_column_ (ACT(n), (col)) != col_NULL), \
                                 act_column ((n), (col)))
#else
#define ACTCOL(n, col)  act_column ((n), (col))
#endif

#define INACTCOL(map,...)                                          \
    insert_act_column ((map),                                   \
                   (sizeof ((actcol_map_t[]) { __VA_ARGS__ })      \
                       / sizeof (actcol_map_t)),                   \
                   (actcol_map_t[]) { __VA_ARGS__ })

/**
 * Worker for INACTCOL
 */
static void
insert_act_column (PFarray_t *map, unsigned int count, actcol_map_t *act)
{
    unsigned int i = 0;
    unsigned int j = 0;
    PFalg_col_t act_ori;
    for (i = 0; i < count; i++) {
        act_ori = act[i].ori_col;
        for (j = 0; j < PFarray_last (map); j++) {
            if ((*(actcol_map_t *) PFarray_at (map, j)).ori_col ==  act_ori)
                assert (!"remapping not allowed");
        }
    }

    for (i = 0; i < count; i++) {
        *(actcol_map_t *) PFarray_add (map) = act[i];
    }
}

/**
 * Worker for 'eff_column (map, col)'.
 * Checks the map and returns the effective column
 */
static PFalg_col_t
act_column_ (PFarray_t *map, PFalg_col_t col)
{
    assert (map);
    for (unsigned int i = 0;
         i < PFarray_last (map); i++) {
        if ( (*(actcol_map_t *)PFarray_at (map, i)).ori_col == col)
            return (*(actcol_map_t *)PFarray_at (map, i)).act_col;
    }

#ifndef NDEBUG
    return col_NULL;
#else
    PFoops (OOPS_FATAL, "Attribute %s not found", PFcol_str (col));

    return col_NULL; /* pacify picky compilers */
#endif
}

/**
 * See 'act_column_ (map, col)' above.
 * Checks the map and returns the effective column.
 */
static PFalg_col_t
act_column (PFla_op_t *n, PFalg_col_t col)
{
    PFarray_t *map = ACT (n);

    return act_column_ (map, col);
}

/**
 * Creates a new mapping item for
 * original and actual column.
 */
static actcol_map_t
actcol (PFalg_col_t new, PFalg_col_t old)
{
    return (actcol_map_t) {
                .ori_col = old,
                .act_col = new
           };
}

/**
 * Simply copy the map denoted by
 * @a actmap.
 */
static PFarray_t *
actcol_map_copy (PFarray_t *actmap)
{
    return PFarray_copy (actmap);
}

/**
 * Worker for 'rev_act_column (map, col)'.
 * Checks the map and returns the original column.
 */
static PFalg_col_t
rev_act_column_ (PFarray_t *map, PFalg_col_t col)
{
    for (unsigned int i = 0; i < PFarray_last (map);
            i++) {
        if ( (*(actcol_map_t *)PFarray_at (map, i)).act_col == col)
            return (*(actcol_map_t *)PFarray_at (map, i)).ori_col;
    }

#ifndef NDEBUG
    return col_NULL;
#else
    PFoops (OOPS_FATAL, "Attribute %s not found", PFcol_str (col));

    /* assert ("name not found"); */
    return col_NULL; /* pacify picky compilers */
#endif
}

#ifndef NDEBUG
#define REVACTCOL(n, col)  (assert (rev_act_column_ (ACT(n), (col)) != col_NULL), \
                                    rev_act_column ((n), (col)))
#else
#define REVACTCOL(n, col)  rev_act_column ((n), (col))
#endif

/**
 * See 'eff_column_ (map, col)' above.
 * Checks the map and returns the effective column
 */
static PFalg_col_t
rev_act_column (PFla_op_t *n, PFalg_col_t col)
{
    PFarray_t *map = ACT (n);

    return rev_act_column_ (map, col);
}

/**
 * Create column list from schema.
 */
static PFalg_collist_t *
create_collist (PFalg_schema_t schema)
{
    PFalg_collist_t *ret = PFalg_collist (schema.count);

    for (unsigned int i = 0; i < schema.count; i++) {
        cladd (ret) = schema.items[i].name;
    }

    return ret;
}

/**
 * Test the equality of two literal table tuples.
 *
 * @param a Tuple to test against tuple @a b.
 * @param b Tuple to test against tuple @a a.
 * @return Boolean value @c true, if the two tuples are equal.
 */
static bool
tuple_eq (PFalg_tuple_t a, PFalg_tuple_t b)
{
    unsigned int i;
    bool mismatch = false;

    /* schemata are definitly not equal if they
     * have a different number of columns */
    if (a.count != b.count)
        return false;

    for (i = 0; i < a.count; i++) {
        /* check the equivalence */
        mismatch = !(PFalg_atom_comparable (a.atoms[i], b.atoms[i]) &&
                     (PFalg_atom_cmp (a.atoms[i], b.atoms[i]) == 0));

        if (mismatch)
            break;
    }

    return (i == a.count);
}

/*------------------------------------------------*/
/*----------------- Core Functions ---------------*/
/*------------------------------------------------*/

/**
 * Test if two subexpressions are equal:
 * The subexpression in @a a is a node
 * of the original plan, @a b is a node from
 * the CSE plan.
 *
 * - both nodes must have the same kind,
 * - the node @a must have the same childs
 *   in his CSE plan as node @a b
 *   (In terms of C pointers).
 * - if the nodes carry additional semantic content, the
 *   content of both nodes must match. So there is
 *   a lot of operator-specific stuff.
 * - there must exist a projection in a child node, to
 *   project the items to the node b.
 */
static bool
match (PFla_op_t *a, PFla_op_t *b)
{
    assert (a);
    assert (b);


    /* shortcut for the trivial case */
    if (a == b)
        return true;

    /* see if a node kind is identical */
    if (a->kind != b->kind)
        return false;

    /* both nodes must have identical children in terms of
     * their underlying cse plans (C pointers) */
    if (L(a))
        if (!(CSE (L (a)) == CSE (L (b))))
            return false;

    if (R(a))
        if (!(CSE (R (a)) == CSE (R (b))))
            return false;

    /* check if at least the number of schema items
     * is the same */
    if (a->schema.count != b->schema.count)
        return false;

    /* special equivalence test for each operator */
    switch (a->kind) {
        case la_serialize_seq:
            if ((ACTCOL (R(a), a->sem.ser_seq.pos) ==
                 ACTCOL (R(b), b->sem.ser_seq.pos)) &&
                (ACTCOL (R(a), a->sem.ser_seq.item) ==
                 ACTCOL (R(b), b->sem.ser_seq.item)))
                return true;

        case la_serialize_rel:
            if ((ACTCOL (R(a), a->sem.ser_rel.iter) != 
                 ACTCOL (R(b), b->sem.ser_rel.iter)) ||
                (ACTCOL (R(a), a->sem.ser_rel.pos) !=
                 ACTCOL (R(b), a->sem.ser_rel.pos)))
                return false;

            for (unsigned int i = 0; i < clsize (a->sem.ser_rel.items);
                     i++) {
                if (ACTCOL (R(a), clat (a->sem.ser_rel.items, i)) !=
                    ACTCOL (R(b), clat (b->sem.ser_rel.items, i)))
                    return false;
            }
            return true;
                        
        case la_side_effects:
            return true;

        case la_lit_tbl:
            if (a->sem.lit_tbl.count !=
                b->sem.lit_tbl.count)
                return false;

            if (a->schema.count !=
                b->schema.count)
                return false;


            /* the items must have the same ordering,
             * in relations the order of tuples is not relevant,
             * but it keeps things simpler for us */
            for (unsigned int i = 0; i < a->sem.lit_tbl.count; i++) {
                if (!(tuple_eq (a->sem.lit_tbl.tuples[i],
                            b->sem.lit_tbl.tuples[i])))
                    return false;
            }

            return true;

        case la_empty_tbl:
            /* since names there are not relevant we
             * only have to check the cardinality of
             * schema */
            if (a->schema.count != b->schema.count)
                return false;

            return true;

        case la_ref_tbl:
            if (strcmp (a->sem.ref_tbl.name,
                         b->sem.ref_tbl.name))
                return false;

            if (PFarray_last (a->sem.ref_tbl.tcols) !=
                PFarray_last (b->sem.ref_tbl.tcols))
                return false;

            /* names have to be aligned; this makes life
             * much easier for us*/
            for (unsigned int i = 0;
                 i < PFarray_last (a->sem.ref_tbl.tcols); i++) {
                 if (strcmp ( *(char**) PFarray_at (a->sem.ref_tbl.tcols, i),
                              *(char**) PFarray_at (b->sem.ref_tbl.tcols, i)))
                     return false;
            }


            /* checking keys */
            if (PFarray_last (a->sem.ref_tbl.keys) !=
                PFarray_last (b->sem.ref_tbl.keys))
                return false;

            for (unsigned int key = 0;
                 key < PFarray_last (a->sem.ref_tbl.keys); key++) {

                PFarray_t * keyPositionsA = *((PFarray_t**) PFarray_at (a->sem.ref_tbl.keys, key));
                PFarray_t * keyPositionsB = *((PFarray_t**) PFarray_at (b->sem.ref_tbl.keys, key));

                if (PFarray_last (keyPositionsA) !=
                    PFarray_last (keyPositionsB))
                    return false;

                for (unsigned int i = 0;
                    i < PFarray_last (keyPositionsA);
                    i++)
                {

                    if (*(int *) PFarray_at (keyPositionsA, i) !=
                        *(int *) PFarray_at (keyPositionsB, i))
                        return false;
                }
            }

            return true;

        case la_attach:
            if (!PFalg_atom_comparable (a->sem.attach.value,
                                        b->sem.attach.value))
                return false;

            if (PFalg_atom_cmp (a->sem.attach.value,
                                b->sem.attach.value) != 0)
                return false;

            return true;

        case la_cross:
            /* only the subexpressions are relevant here */
            return true;

        case la_eqjoin:
        case la_semijoin:
            if ((ACTCOL (L(a), a->sem.eqjoin.col1) ==
                 ACTCOL (L(b), b->sem.eqjoin.col1)) &&
                (ACTCOL (R(a), a->sem.eqjoin.col2) ==
                 ACTCOL (R(b), b->sem.eqjoin.col2)))
                return true;

            return false;

        case la_thetajoin:
        {
            unsigned int i = 0;
            unsigned int j = 0;
            for (i = 0; i < a->sem.thetajoin.count; i++) {
                 for (j = 0; j < b->sem.thetajoin.count; j++) {
                     if ((ACTCOL (L(a), a->sem.thetajoin.pred[i].left) ==
                          ACTCOL (L(b), b->sem.thetajoin.pred[j].left)) &&
                         (ACTCOL (R(a), a->sem.thetajoin.pred[i].right) ==
                          ACTCOL (R(b), b->sem.thetajoin.pred[j].right)) &&
                         (a->sem.thetajoin.pred[i].comp ==
                          b->sem.thetajoin.pred[j].comp))
                         break;
                 }

                 if (j >= b->sem.thetajoin.count)
                     return false;
            }

            return true;
        }

        case la_project:
            if (a->sem.proj.count != b->sem.proj.count)
                return false;

            for (unsigned int i = 0;  i < a->sem.proj.count; i++) {
                if (ACTCOL (L(a), a->sem.proj.items[i].old) !=
                    ACTCOL (L(b), b->sem.proj.items[i].old))
                    return false;
            }

            return true;

        case la_select:
            if (ACTCOL (L(a), a->sem.select.col) ==
                ACTCOL (L(b), b->sem.select.col))
                return true;

            return false;

        case la_pos_select:

            /* partition column is not necesserily set */
            if (IS_NULL(a->sem.pos_sel.part)?col_NULL:
                    ACTCOL (L(a), a->sem.pos_sel.part) !=
                IS_NULL(b->sem.pos_sel.part)?col_NULL:
                    ACTCOL (L(b), b->sem.pos_sel.part))
                return false;

            if (a->sem.pos_sel.pos != b->sem.pos_sel.pos)
                return false;

            /* sort criterions have to be aligned */
            for (unsigned int i = 0;
                 i < PFord_count (a->sem.sort.sortby); i++) {
                PFalg_col_t aitem = PFord_order_col_at (a->sem.sort.sortby, i);
                PFalg_col_t bitem = PFord_order_col_at (b->sem.sort.sortby, i);
                bool adir         = PFord_order_dir_at (a->sem.sort.sortby, i);
                bool bdir         = PFord_order_dir_at (b->sem.sort.sortby, i);
                if (!((ACTCOL (L(a), aitem) == ACTCOL (L(b), bitem)) &&
                      (adir == bdir)))
                    return false;
            }

            return true;

        case la_disjunion:
        case la_intersect:
        case la_difference:
            /* only subexpressions are relevant here */
            return true;

        case la_distinct:
            /* only subexpression is relevant here */
            return true;

        case la_fun_1to1:
            if (a->sem.fun_1to1.kind !=
                b->sem.fun_1to1.kind)
                return false;

            if (clsize (a->sem.fun_1to1.refs) !=
                clsize (b->sem.fun_1to1.refs))
                return false;

            for (unsigned int i = 0; i < clsize (a->sem.fun_1to1.refs); i++)
                 if (clat (a->sem.fun_1to1.refs, i) !=
                     clat (b->sem.fun_1to1.refs, i))
                     return false;

            return true;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            if ((ACTCOL (L(a), a->sem.binary.col1) &&
                 ACTCOL (L(b), b->sem.binary.col1)) ==
                (ACTCOL (L(a), a->sem.binary.col2) &&
                 ACTCOL (L(b), b->sem.binary.col2)))
                return true ;

            return false;

        case la_bool_not:
            if (ACTCOL (L(a), a->sem.unary.col) ==
                ACTCOL (L(b), b->sem.unary.col))
                return true;

            return false;

        case la_aggr:
        {
            unsigned int i = 0;
            unsigned int j = 0;
            for (i = 0; i < a->sem.aggr.count; i++) {
                 for (j = 0; j < b->sem.aggr.count; j++) {
                     if ((IS_NULL (a->sem.aggr.aggr[i].col)
                          ? col_NULL
                          : ACTCOL (L(a), a->sem.aggr.aggr[i].col)) ==
                         (IS_NULL (b->sem.aggr.aggr[j].col)
                          ? col_NULL
                          : ACTCOL (L(b), b->sem.aggr.aggr[j].col)) &&
                         (a->sem.aggr.aggr[i].kind ==
                          b->sem.aggr.aggr[j].kind))
                         break;
                 }

                 if (j >= b->sem.aggr.count)
                     return false;
            }

            /* partition column is not necesserily set */
            if (IS_NULL(a->sem.aggr.part)?col_NULL:
                    ACTCOL (L(a), a->sem.aggr.part) !=
                IS_NULL(b->sem.aggr.part)?col_NULL:
                    ACTCOL (L(b), b->sem.aggr.part))
                return false;

        }   return true;

        case la_rownum:
        case la_rowrank:
        case la_rank:

            if (PFord_count (a->sem.sort.sortby) !=
                PFord_count (b->sem.sort.sortby))
                return false;

            /* sort criterions have to be aligned */
            for (unsigned int i = 0;
                 i < PFord_count (a->sem.sort.sortby); i++) {
                PFalg_col_t aitem = PFord_order_col_at (a->sem.sort.sortby, i);
                PFalg_col_t bitem = PFord_order_col_at (b->sem.sort.sortby, i);
                bool adir         = PFord_order_dir_at (a->sem.sort.sortby, i);
                bool bdir         = PFord_order_dir_at (b->sem.sort.sortby, i);
                if (!((ACTCOL (L(a), aitem) == ACTCOL (L(b), bitem)) &&
                      (adir == bdir)))
                    return false;
            }

            /* partition column is not necesserily set */
            if (IS_NULL(a->sem.sort.part)?col_NULL:
                    ACTCOL (L(a), a->sem.sort.part) !=
                IS_NULL(b->sem.sort.part)?col_NULL:
                    ACTCOL (L(b), b->sem.sort.part))
                return false;

            return true;

        case la_rowid:
            return true;

        case la_type:
        case la_type_assert:
        case la_cast:
            if ((ACTCOL (L(a), a->sem.type.col) ==
                 ACTCOL (L(b), b->sem.type.col)) &&
                a->sem.type.ty == b->sem.type.ty)
                return true;

            return false;

        case la_step:
            if (!((ACTCOL (R(a), a->sem.step.iter) ==
                 ACTCOL (R(b), b->sem.step.iter))))
                return false;
            /* falling through */
    
        case la_step_join:
            if ((ACTCOL (R(a), a->sem.step.item) ==
                 ACTCOL (R(b), b->sem.step.item))  &&
                (a->sem.step.spec.axis == b->sem.step.spec.axis) &&
                (a->sem.step.spec.kind == b->sem.step.spec.kind) &&
                (!PFqname_eq (a->sem.step.spec.qname,
                              b->sem.step.spec.qname)) &&
                (a->sem.step.level == b->sem.step.level))
                return true;

             return false;

        case la_guide_step:
            if (!((ACTCOL (R(a), a->sem.step.iter) ==
                 ACTCOL (R(b), b->sem.step.iter))))
                return false;
            /* falling through */
            
        case la_guide_step_join:
            if (!((ACTCOL (R(a), a->sem.step.item) ==
                 ACTCOL (R(b), b->sem.step.item))  &&
                (a->sem.step.spec.axis == b->sem.step.spec.axis) &&
                (a->sem.step.spec.kind == b->sem.step.spec.kind) &&
                (!PFqname_eq (a->sem.step.spec.qname,
                              b->sem.step.spec.qname)) &&
                (a->sem.step.level == b->sem.step.level) &&
                (a->sem.step.guide_count == b->sem.step.guide_count)))
                return false;

            /* we assume the guides are aligned */
            for (unsigned int i = 0;
                 i < a->sem.step.guide_count; i++) {
                 if (a->sem.step.guides[i]->guide !=
                     b->sem.step.guides[i]->guide)
                     return false;
            }

            return true;

        case la_doc_index_join:
            if ((ACTCOL (R(a), a->sem.doc_join.item) ==
                 ACTCOL (R(b), b->sem.doc_join.item)) &&
                (ACTCOL (R(a), a->sem.doc_join.item_doc) ==
                 ACTCOL (R(b), b->sem.doc_join.item_doc)) &&
                (a->sem.doc_join.kind == b->sem.doc_join.kind) &&
                strcmp(a->sem.doc_join.ns1, b->sem.doc_join.ns1) == 0 &&
                strcmp(a->sem.doc_join.loc1, b->sem.doc_join.loc1) == 0 &&
                strcmp(a->sem.doc_join.ns2, b->sem.doc_join.ns2) == 0 &&
                strcmp(a->sem.doc_join.loc2, b->sem.doc_join.loc2) == 0)
                return true;

            return false;

        case la_doc_tbl:
            if ((ACTCOL (L(a), a->sem.doc_tbl.col) ==
                 ACTCOL (L(b), b->sem.doc_tbl.col)) &&
                (a->sem.doc_tbl.kind ==
                 b->sem.doc_tbl.kind))
                return true;

            return false;

        case la_doc_access:
            if ((ACTCOL (R(a), a->sem.doc_access.col) ==
                 ACTCOL (R(b), b->sem.doc_access.col)) &&
                a->sem.doc_access.doc_col == b->sem.doc_access.doc_col)
                return true;

            return false;

        case la_twig:
        case la_fcns:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
            return false;

        case la_merge_adjacent:
            if ((ACTCOL (R(a), a->sem.merge_adjacent.iter_in) ==
                 ACTCOL (R(b), b->sem.merge_adjacent.iter_in)) &&
                (ACTCOL (R(a), a->sem.merge_adjacent.pos_in) ==
                 ACTCOL (R(b), b->sem.merge_adjacent.pos_in)) &&
                (ACTCOL (R(a), a->sem.merge_adjacent.item_in) ==
                 ACTCOL (R(b), b->sem.merge_adjacent.item_in)))
                return true;

            return false;

        case la_roots:
            return true;

        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
            return true;

        case la_error:
            if (ACTCOL (L(a), a->sem.err.col) ==
                ACTCOL (L(b), b->sem.err.col))
                return true;

            return false;

        case la_nil:
            return true;

        case la_cache:
            if (a->sem.cache.id == b->sem.cache.id &&
                ACTCOL (L(a), a->sem.cache.pos) ==
                ACTCOL (L(b), b->sem.cache.pos) &&
                ACTCOL (L(a), a->sem.cache.item) ==
                ACTCOL (L(b), b->sem.cache.item))
                return true;

            return false;


        case la_trace:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
        case la_frag_extract:
            return false;

        case la_proxy:
        case la_proxy_base:
            return false;

        case la_string_join:
            if ((ACTCOL (L(a), a->sem.string_join.iter) ==
                 ACTCOL (L(b), b->sem.string_join.iter)) &&
                (ACTCOL (L(a), a->sem.string_join.pos) ==
                 ACTCOL (L(b), b->sem.string_join.pos)) &&
                (ACTCOL (L(a), a->sem.string_join.item) ==
                 ACTCOL (L(b), b->sem.string_join.item)) &&
                (ACTCOL (R(a), a->sem.string_join.iter_sep) ==
                 ACTCOL (R(b), b->sem.string_join.iter_sep)) &&
                (ACTCOL (R(a), a->sem.string_join.item_sep) ==
                 ACTCOL (R(b), b->sem.string_join.item_sep)))
                return true;

            return false;

        case la_dummy:
        case la_internal_op:
            break;
    }

    PFoops (OOPS_FATAL, "this should not occur (match)");
    return false;
}

/**
 * Checks the equivalence of two columns
 * of different literal table operators.
 */
static bool
column_eq (unsigned int col1, unsigned int col2,
           PFla_op_t *littbl1, PFla_op_t *littbl2)
{
    assert (littbl1->kind == la_lit_tbl);
    assert (littbl2->kind == la_lit_tbl);

    if (littbl1->sem.lit_tbl.count !=
        littbl2->sem.lit_tbl.count)
        return false;

    unsigned int i;

    PFalg_tuple_t *a = littbl1->sem.lit_tbl.tuples;
    PFalg_tuple_t *b = littbl2->sem.lit_tbl.tuples;

    /* check every item of the column */
    for (i = 0; i < littbl1->sem.lit_tbl.count; i++) {
        if (!(PFalg_atom_comparable (a[i].atoms[col1], b[i].atoms[col2]) &&
              (PFalg_atom_cmp (a[i].atoms[col1], b[i].atoms[col2]) == 0)))
            return false;
    }
    return true;
}

/**
 * checks the columns of @a littbl1 and @a littbl2
 * and returns the corresponding name to the @a name
 * in littbl2
 */
static PFalg_col_t
littbl_column (PFalg_col_t name, PFla_op_t *littbl1, PFla_op_t *littbl2,
               PFalg_collist_t *seen)
{
    assert (littbl1->kind == la_lit_tbl);
    assert (littbl2->kind == la_lit_tbl);

    /* trivial check for number of tuples */
    assert (littbl1->sem.lit_tbl.count ==
            littbl2->sem.lit_tbl.count);

    unsigned int column;

    /* check if schema name is at least in table1 */
    for (column = 0; column < littbl1->schema.count; column++) {
        if (littbl1->schema.items[column].name == name)
            break;
    }

    /* if column name is not in table1 */
    if (column >= littbl1->schema.count)
       PFoops (OOPS_FATAL,
               "Column %s not found in literal table",
               PFcol_str (name));

    /* check for column equality and return the right name */
    bool match = false;
    for (unsigned int i = 0; i < littbl2->schema.count; i++) {
        match = false;
        if (column_eq (column, i, littbl1, littbl2)) {
            /* loop over the protocolled columns to check if
             * if we want to use a column twice */
            for (unsigned int j = 0; j < clsize (seen); j++)
                if (littbl2->schema.items[i].name == clat (seen, j))
                    match = true;
            if (match) continue;

            return littbl2->schema.items[i].name;
        }
    }

    PFoops (OOPS_FATAL,
            "This should not happen: no equal columns found");
    return col_NULL; /* satisfy picky compilers */
}

/**
 * Check if a column @a b appears in list @a a.
 */
static bool
in (PFalg_collist_t *a, PFalg_col_t b)
{
    if (!a) return false;

    for (unsigned int i = 0; i < clsize (a); i++)
        if (b == clat (a, i))
            return true;

    return false;
}

/**
 * Creates a new operator for every node not yet in
 * the CSE-plan.
 *
 * Each node will be duplicated.
 */
static PFla_op_t*
new_operator (PFla_op_t *n)
{
    switch (n->kind) {
        case la_serialize_seq:
            return PFla_serialize_seq (CSE(L(n)), CSE(R(n)),
                                ACTCOL(R(n), n->sem.ser_seq.pos),
                                ACTCOL(R(n), n->sem.ser_seq.item));

        case la_serialize_rel:
        {
            PFalg_collist_t *items = PFalg_collist_copy (n->sem.ser_rel.items);
                            
            for (unsigned int i = 0; i < clsize (items); i++) {
                clat (items, i) = ACTCOL (R(n), clat (items, i));
            }
            
            return PFla_serialize_rel (CSE(L(n)), CSE(R(n)),
                                ACTCOL (R(n), n->sem.ser_rel.iter),
                                ACTCOL (R(n), n->sem.ser_rel.pos),
                                items);
        }

        case la_side_effects:
            return PFla_side_effects (CSE(L(n)), CSE(R(n)));

        case la_lit_tbl:
            return PFla_lit_tbl_ (create_collist (n->schema),
                                  n->sem.lit_tbl.count,
                                  n->sem.lit_tbl.tuples);

        case la_empty_tbl:
            return PFla_empty_tbl_ (n->schema);

        case la_ref_tbl:
            return PFla_ref_tbl_ (n->sem.ref_tbl.name,
                                n->schema,
                                n->sem.ref_tbl.tcols,
                                n->sem.ref_tbl.keys);

        case la_attach:
            return PFla_attach (CSE(L(n)),
                                create_unq_name (CSE(L(n))->schema,
                                                 n->sem.attach.res),
                                n->sem.attach.value);

        case la_cross:
        case la_eqjoin:
        case la_semijoin:
        case la_thetajoin:
        {
            PFalg_collist_t *used_collist = PFalg_collist (L(n)->schema.count +
                                                           R(n)->schema.count);
            PFalg_proj_t *p = NULL;
            PFla_op_t    *proj = CSE(R(n));
            bool projection = false;

            /* actual column mapping in case of a projection */
            PFarray_t    *actmap = create_actcol_map ();

            /* fill used columns with columns of the left schema */
            for (unsigned int i = 0; i < CSE(L(n))->schema.count; i++)
               cladd (used_collist) = CSE(L(n))->schema.items[i].name;

            /* determine number of conflicting columns */
            for (unsigned int i = 0; i < CSE(R(n))->schema.count; i++) {
                if (in (used_collist, CSE(R(n))->schema.items[i].name)) {
                    projection = true;
                    break;
                }
            }

            if (projection) {
                p = (PFalg_proj_t *) PFmalloc (CSE(R(n))->schema.count *
                                               sizeof(PFalg_proj_t));

                /* create projection */
                for (unsigned int i = 0; i < CSE(R(n))->schema.count; i++) {
                    /* check for conflicting columns */
                    if (in (used_collist, CSE(R(n))->schema.items[i].name)) {
                       /* create new column name */
                       PFalg_col_t t
                           = PFcol_new (CSE(R(n))->schema.items[i].name);

                       /* create new entry for projection */
                       p[i] = PFalg_proj (t, CSE(R(n))->schema.items[i].name);
                       INACTCOL (actmap,
                                 actcol (t,
                                         R(n)->schema.items[i].name));

                        /* add new created name to used_collist */
                        cladd (used_collist) = t;
                    }
                    /* this is not a conflicting column */
                    else {
                        p[i] = PFalg_proj (CSE(R(n))->schema.items[i].name,
                                           CSE(R(n))->schema.items[i].name);
                        INACTCOL (actmap,
                                  actcol (CSE(R(n))->schema.items[i].name,
                                          R(n)->schema.items[i].name));

                        /* add new name to conflicting column */
                        cladd (used_collist) = CSE(R(n))->schema.items[i].name;
                    }
                }

                PFla_op_t *prev = proj;

                proj = PFla_project_ (proj, CSE(R(n))->schema.count, p);

                /* insert dummy operator in the original plan */
                PFalg_proj_t *dummy_p = (PFalg_proj_t*)
                                       PFmalloc (R(n)->schema.count *
                                                 sizeof (PFalg_proj_t));
                /* create projection list */
                for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                     dummy_p[i] = PFalg_proj (R(n)->schema.items[i].name,
                                     R(n)->schema.items[i].name);
                }

                /* insert projection and actual columns
                 * in the original plan */
                R(n) = PFla_project_ (R(n), R(n)->schema.count, dummy_p);
                INACT (R(n), actmap);

                /* map the new operator to the cse operator
                 * of the previous operator
                 * to ensure the equality (match ()) */
                INCSE (R(n), prev);
            }
            switch (n->kind)
            {
                case la_cross:  return PFla_cross  (CSE(L(n)), proj);

                case la_eqjoin:
                    return PFla_eqjoin (CSE(L(n)), proj,
                                        ACTCOL (L(n), n->sem.eqjoin.col1),
                                        ACTCOL (R(n), n->sem.eqjoin.col2));

                case la_semijoin:
                    return PFla_semijoin (CSE(L(n)), proj,
                                          ACTCOL (L(n), n->sem.eqjoin.col1),
                                          ACTCOL (R(n), n->sem.eqjoin.col2));

                case la_thetajoin:
                {
                    PFalg_sel_t *sel =
                          (PFalg_sel_t *) PFmalloc (
                                               n->sem.thetajoin.count *
                                               sizeof (PFalg_sel_t));
                    /* copy sel_list */
                    for (unsigned int i = 0; i < n->sem.thetajoin.count; i++)
                        sel[i] = (PFalg_sel_t) {
                                       .comp =  n->sem.thetajoin.pred[i].comp,
                                       .left =  ACTCOL (L(n),
                                                 n->sem.thetajoin.pred[i].left),
                                       .right = ACTCOL (R(n),
                                                 n->sem.thetajoin.pred[i].right)
                                 };

                     return PFla_thetajoin (CSE(L(n)), proj,
                                            n->sem.thetajoin.count, sel);
                }

                default:
                    PFoops (OOPS_FATAL,
                            "join operator expected but %s found",
                            ID[n->kind]);
            }
        }

        case la_project:
        {
            PFalg_proj_t *newproj;
            unsigned int count = n->sem.proj.count;
            newproj = (PFalg_proj_t *) PFmalloc (count * sizeof(PFalg_proj_t));

            for (unsigned int i = 0; i < count; i++)
                newproj[i] = (PFalg_proj_t) {
                                 .new = n->sem.proj.items[i].new,
                                 .old = ACTCOL (L(n), n->sem.proj.items[i].old)
                             };

            return PFla_project_ (CSE(L(n)), count, newproj);
        }

        case la_select:
            return PFla_select (CSE(L(n)), ACTCOL (L(n), n->sem.select.col));

        case la_pos_select:
        {
            /* restructure of ordering */
            PFord_ordering_t sortby = PFordering ();
            for (unsigned int i = 0;
                 i < PFarray_last (n->sem.sort.sortby); i++) {
                 sortby = PFord_refine (
                               sortby,
                               ACTCOL(L(n),
                                      PFord_order_col_at (
                                           n->sem.sort.sortby, i)),
                               PFord_order_dir_at (n->sem.sort.sortby, i));
            }

            return PFla_pos_select (CSE(L(n)), n->sem.pos_sel.pos,
                                    sortby,
                                    (n->sem.pos_sel.part == col_NULL)?
                                    col_NULL:
                                    ACTCOL (L(n), n->sem.pos_sel.part));
        }

        case la_disjunion:
        case la_intersect:
        case la_difference:
        {
            /* function pointer to create the set operator */
            PFla_op_t * (*setop) (const PFla_op_t *n1, const PFla_op_t *n2);

            PFarray_t    *actmap = create_actcol_map ();
            PFalg_proj_t *p      = NULL;
            PFla_op_t    *proj   = CSE(R(n));

            /* trivial check if both operands have the same number of
             * columns */
            assert (CSE(L(n))->schema.count == CSE(R(n))->schema.count);

            /* check schema equivalence */
            unsigned int i = 0;
            unsigned int j = 0;
            bool projection = false;

            /* check if there are name conflicts and prepare
             * a projection if this is the case */
            for (i = 0; i < CSE(L(n))->schema.count; i++) {
                for (j = 0; j < CSE(R(n))->schema.count; j++) {
                    PFalg_col_t rev_l = col_NULL,
                                  rev_r = col_NULL;
                    if ((rev_l = REVACTCOL(
                                    L(n), CSE(L(n))->schema.items[i].name)) ==
                        (rev_r = REVACTCOL(
                                    R(n), CSE(R(n))->schema.items[j].name))) {
                        
                        if (ACTCOL(L(n), rev_l) !=
                            ACTCOL(R(n), rev_r))
                                projection = true;
        
                        break;
                    }
                }

                if (j >= CSE(R(n))->schema.count)
                    PFoops (OOPS_FATAL,
                            "Schema of two arguments of set operation (union, "
                            "difference, intersect) do not match");
            }

            /* prepare a projection and insert it */
            if (projection) {
                p = (PFalg_proj_t *)
                    PFmalloc (R(n)->schema.count * sizeof (PFalg_proj_t));
                for (i = 0; i < CSE(L(n))->schema.count; i++) {
                    for (j = 0; j < CSE(R(n))->schema.count; j++) {
                        PFalg_col_t rev_l = col_NULL,
                                    rev_r = col_NULL;
                                    
                        if ((rev_l = REVACTCOL(L(n),
                                        CSE(L(n))->schema.items[i].name)) ==
                            (rev_r = REVACTCOL(R(n),
                                        CSE(R(n))->schema.items[j].name))) {
                            /* the names are conflicting */
                            if (ACTCOL(L(n), L(n)->schema.items[i].name) !=
                                ACTCOL(R(n), R(n)->schema.items[j].name)) {
                                p[i] = PFalg_proj (
                                      ACTCOL(L(n),
                                             rev_l),
                                      ACTCOL(R(n),
                                             rev_r));

                                INACTCOL (actmap,
                                    actcol (ACTCOL(L(n),
                                               rev_l),
                                            REVACTCOL (R(n),
                                               CSE(R(n))->schema.items[j]
                                                   .name)));
                            }
                            else {
                                PFalg_col_t t = ACTCOL(R(n), rev_r);
                                
                                p[i] = PFalg_proj (t,t);

                                INACTCOL (
                                    actmap,
                                    actcol (t,t));
                            }
                        }
                    }
                }

                PFla_op_t *prev = proj;

                proj = PFla_project_ (CSE(R(n)), R(n)->schema.count, p);

                /* insert dummy operator in the original plan */
                PFalg_proj_t *dummy_p = (PFalg_proj_t*)
                                       PFmalloc (R(n)->schema.count *
                                                 sizeof (PFalg_proj_t));
                for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                     dummy_p[i] = PFalg_proj (R(n)->schema.items[i].name,
                                     R(n)->schema.items[i].name);
                }

                R(n) = PFla_project_ (R(n), R(n)->schema.count, dummy_p);
                INACT (R(n), actmap);

                /* map the new operator to the cse operator
                 * of the previous operator
                 * to ensure the equality (match ()) */
                INCSE (R(n), prev);
            }

            switch (n->kind) {
                case la_disjunion:  setop = PFla_disjunion;  break;
                case la_intersect:  setop = PFla_intersect;  break;
                case la_difference: setop = PFla_difference; break;
                default:
                     PFoops (OOPS_FATAL,
                             "set operator expected but found %s",
                             ID[n->kind]);
            }

            return setop (CSE(L(n)), proj);
        }

        case la_distinct:
            return PFla_distinct (CSE(L(n)));

        case la_fun_1to1:
        {
            /* prepare column list */
            PFalg_collist_t *items = PFalg_collist_copy (n->sem.fun_1to1.refs);
                            
            for (unsigned int i = 0; i < clsize (items); i++) {
                clat (items, i) = ACTCOL (L(n), clat (items, i));
            }

            return PFla_fun_1to1 (CSE(L(n)), n->sem.fun_1to1.kind,
                                  create_unq_name (CSE(L(n))->schema,
                                                   n->sem.fun_1to1.res),
                                  items);
        }

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
        {
            PFla_op_t * (*comp) (const PFla_op_t *n, PFalg_col_t res,
                                 PFalg_col_t att1, PFalg_col_t att2);

            switch (n->kind) {
               case la_num_eq:   comp = PFla_eq;  break;
               case la_num_gt:   comp = PFla_gt;  break;
               case la_bool_and: comp = PFla_and; break;
               case la_bool_or:  comp = PFla_or;  break;
               case la_to:       comp = PFla_to;  break;
               default:
                  PFoops (OOPS_FATAL, "bool or num expected but %s found",
                          ID[n->kind]);
            }

            return comp (CSE(L(n)),
                         create_unq_name (CSE(L(n))->schema,
                                          n->sem.binary.res),
                         ACTCOL (L(n), n->sem.binary.col1),
                         ACTCOL (L(n), n->sem.binary.col2));
        }

        case la_bool_not:
            return PFla_not (CSE(L(n)),
                             create_unq_name (CSE(L(n))->schema,
                                              n->sem.unary.res),
                             ACTCOL (L(n), n->sem.unary.col));

        case la_aggr:
        {
            PFalg_aggr_t *aggr = PFmalloc (n->sem.aggr.count *
                                           sizeof (PFalg_aggr_t));
            /* copy aggr_list */
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                aggr[i] = PFalg_aggr (
                              n->sem.aggr.aggr[i].kind,
                              create_unq_name (
                                  CSE(L(n))->schema,
                                  n->sem.aggr.aggr[i].res),
                              IS_NULL (n->sem.aggr.aggr[i].col)
                              ? col_NULL
                              : ACTCOL (L(n), n->sem.aggr.aggr[i].col));

             return PFla_aggr (CSE(L(n)),
                               IS_NULL (n->sem.aggr.part)
                               ? col_NULL
                               : ACTCOL (L(n), n->sem.aggr.part),
                               n->sem.aggr.count,
                               aggr);
        }

        case la_rownum:
        case la_rowrank:
        case la_rank:
        {
            /* restructure of ordering */
            PFord_ordering_t sortby = PFordering ();
            for (unsigned int i = 0;
                 i < PFarray_last (n->sem.sort.sortby); i++) {
                   sortby = PFord_refine (
                                    sortby,
                                    ACTCOL(L(n),
                                           PFord_order_col_at (
                                                  n->sem.sort.sortby, i)),
                                    PFord_order_dir_at (
                                           n->sem.sort.sortby, i));
            }

            switch (n->kind) {
                case la_rownum:
                    return PFla_rownum (CSE(L(n)),
                                 create_unq_name (CSE(L(n))->schema,
                                                  n->sem.sort.res),
                                 sortby,
                                 (n->sem.sort.part == col_NULL)?
                                 col_NULL:
                                 ACTCOL (L(n), n->sem.sort.part));
                case la_rowrank:
                    return PFla_rowrank (CSE(L(n)),
                                 create_unq_name (CSE(L(n))->schema,
                                                  n->sem.sort.res),
                                 sortby);
                case la_rank:
                    return PFla_rank (CSE(L(n)),
                                 create_unq_name (CSE(L(n))->schema,
                                                  n->sem.sort.res),
                                 sortby);
                default:
                    PFoops (OOPS_FATAL,
                            "(rownum|rowrank|rank) expected but %s found",
                            ID[n->kind]);
            }
        }

        case la_rowid:
             return PFla_rowid (CSE(L(n)),
                                create_unq_name (CSE(L(n))->schema,
                                                 n->sem.rowid.res));

        case la_type:
            return PFla_type (CSE(L(n)),
                              create_unq_name (CSE(L(n))->schema,
                                               n->sem.type.res),
                              ACTCOL (L(n), n->sem.type.col),
                              n->sem.type.ty);

        case la_type_assert:
            return PFla_type_assert (CSE(L(n)),
                                     ACTCOL (L(n), n->sem.type.col),
                                     n->sem.type.ty,
                                     true);

        case la_cast:
            return PFla_cast (CSE(L(n)),
                              create_unq_name (CSE(L(n))->schema,
                                               n->sem.type.res),
                              ACTCOL (L(n), n->sem.type.col),
                              n->sem.type.ty);

        case la_step:
            return PFla_step (CSE(L(n)), CSE(R(n)),
                              n->sem.step.spec,
                              n->sem.step.level,
                              ACTCOL (R(n), n->sem.step.iter),
                              ACTCOL (R(n), n->sem.step.item),
                              ACTCOL (R(n), n->sem.step.item));

        case la_step_join:
            return PFla_step_join (CSE(L(n)), CSE(R(n)),
                                   n->sem.step.spec,
                                   n->sem.step.level,
                                   ACTCOL (R(n), n->sem.step.item),
                                   create_unq_name (CSE(R(n))->schema,
                                                    n->sem.step.item_res));

        case la_guide_step:
            return PFla_guide_step (CSE(L(n)), CSE(R(n)),
                                    n->sem.step.spec,
                                    n->sem.step.guide_count,
                                    n->sem.step.guides,
                                    n->sem.step.level,
                                    ACTCOL (R(n), n->sem.step.iter),
                                    ACTCOL (R(n), n->sem.step.item),
                                    ACTCOL (R(n), n->sem.step.item));

        case la_guide_step_join:
            return PFla_guide_step_join (CSE(L(n)), CSE(R(n)),
                                         n->sem.step.spec,
                                         n->sem.step.guide_count,
                                         n->sem.step.guides,
                                         n->sem.step.level,
                                         ACTCOL (R(n), n->sem.step.item),
                                         create_unq_name (
                                             CSE(R(n))->schema,
                                             n->sem.step.item_res));

        case la_doc_index_join:
            return PFla_doc_index_join (CSE(L(n)), CSE(R(n)),
                                        n->sem.doc_join.kind,
                                        ACTCOL (R(n), n->sem.doc_join.item),
                                        create_unq_name (
                                            CSE(R(n))->schema,
                                            n->sem.doc_join.item_res),
                                        ACTCOL (R(n),
                                                n->sem.doc_join.item_doc),
                                        n->sem.doc_join.ns1,
                                        n->sem.doc_join.loc1,
                                        n->sem.doc_join.ns2,
                                        n->sem.doc_join.loc2);

        case la_doc_tbl:
            return PFla_doc_tbl (CSE(L(n)),
                                 create_unq_name (CSE(L(n))->schema,
                                                  n->sem.doc_tbl.res),
                                 ACTCOL (L(n), n->sem.doc_tbl.col),
                                 n->sem.doc_tbl.kind);

        case la_doc_access:
            return PFla_doc_access (CSE(L(n)), CSE(R(n)),
                                    create_unq_name (CSE(R(n))->schema,
                                                     n->sem.doc_access.res),
                                    ACTCOL (R(n), n->sem.doc_access.col),
                                    n->sem.doc_access.doc_col);
        case la_twig:
            return PFla_twig (CSE(L(n)),
                              n->sem.iter_item.iter,
                              n->sem.iter_item.item);

        case la_fcns:
            return PFla_fcns (CSE(L(n)), CSE(R(n)));

        case la_docnode:
            return PFla_docnode (CSE(L(n)), CSE(R(n)),
                                 ACTCOL (R(n), n->sem.docnode.iter));

        case la_element:
            return PFla_element (CSE(L(n)), CSE(R(n)),
                                 ACTCOL (L(n), n->sem.iter_item.iter),
                                 ACTCOL (L(n), n->sem.iter_item.item));

        case la_attribute:
            return PFla_attribute (CSE(L(n)),
                                   ACTCOL (L(n), n->sem.iter_item1_item2.iter),
                                   ACTCOL (L(n), n->sem.iter_item1_item2.item1),
                                   ACTCOL (L(n),
                                           n->sem.iter_item1_item2.item2));

        case la_textnode:
            return PFla_textnode (CSE(L(n)),
                                  ACTCOL (L(n), n->sem.iter_item.iter),
                                  ACTCOL (L(n), n->sem.iter_item.item));

        case la_comment:
            return PFla_comment (CSE(L(n)),
                                 ACTCOL (L(n), n->sem.iter_item.iter),
                                 ACTCOL (L(n), n->sem.iter_item.item));

        case la_processi:
            return PFla_processi (CSE(L(n)),
                                  ACTCOL (L(n), n->sem.iter_item1_item2.iter),
                                  ACTCOL (L(n), n->sem.iter_item1_item2.item1),
                                  ACTCOL (L(n), n->sem.iter_item1_item2.item2));

        case la_content:
            return PFla_content (CSE(L(n)), CSE(R(n)),
                                 ACTCOL (R(n), n->sem.iter_pos_item.iter),
                                 ACTCOL (R(n), n->sem.iter_pos_item.pos),
                                 ACTCOL (R(n), n->sem.iter_pos_item.item));

        case la_merge_adjacent:
            return PFla_pf_merge_adjacent_text_nodes (CSE(L(n)), CSE(R(n)),
                                 ACTCOL (R(n), n->sem.merge_adjacent.iter_in),
                                 ACTCOL (R(n), n->sem.merge_adjacent.pos_in),
                                 ACTCOL (R(n), n->sem.merge_adjacent.item_in),
                                 ACTCOL (R(n), n->sem.merge_adjacent.iter_in),
                                 ACTCOL (R(n), n->sem.merge_adjacent.pos_in),
                                 ACTCOL (R(n), n->sem.merge_adjacent.item_in));
            break;

        case la_roots:
            return PFla_roots (CSE(L(n)));

        case la_fragment:
            return PFla_fragment (CSE(L(n)));

        case la_frag_union:
            return PFla_frag_union (CSE(L(n)), CSE(R(n)));

        case la_empty_frag:
            return PFla_empty_frag ();

        case la_error:
            return PFla_error (CSE(L(n)),
                               CSE(R(n)),
                               ACTCOL (R(n), n->sem.err.col));

        case la_nil:
            return PFla_nil ();

        case la_cache:
            return PFla_cache (CSE(L(n)),
                               CSE(R(n)),
                               n->sem.cache.id,
                               ACTCOL (R(n), n->sem.cache.pos),
                               ACTCOL (R(n), n->sem.cache.item));

        case la_trace:
            return PFla_trace (CSE(L(n)), CSE(R(n)));

        case la_trace_items:
            return PFla_trace_items (CSE(L(n)), CSE(R(n)),
                                     ACTCOL (L(n), n->sem.iter_pos_item.iter),
                                     ACTCOL (L(n), n->sem.iter_pos_item.pos),
                                     ACTCOL (L(n), n->sem.iter_pos_item.item));

        case la_trace_msg:
            return PFla_trace_msg (CSE(L(n)), CSE(R(n)),
                                   ACTCOL (L(n), n->sem.iter_item.iter),
                                   ACTCOL (L(n), n->sem.iter_item.item));

        case la_trace_map:
            return PFla_trace_map (CSE(L(n)), CSE(R(n)),
                                   ACTCOL (L(n), n->sem.trace_map.inner),
                                   ACTCOL (L(n), n->sem.trace_map.outer));

        case la_string_join: {
            return PFla_fn_string_join (
                       CSE(L(n)), CSE(R(n)),
                       ACTCOL (L(n), n->sem.string_join.iter),
                       ACTCOL (L(n), n->sem.string_join.pos),
                       ACTCOL (L(n), n->sem.string_join.item),
                       ACTCOL (R(n), n->sem.string_join.iter_sep),
                       ACTCOL (R(n), n->sem.string_join.item_sep),
                       ACTCOL (L(n), n->sem.string_join.iter),
                       ACTCOL (L(n), n->sem.string_join.item));

/*                       create_unq_name (CSE(R(n))->schema,
                                        n->sem.string_join.iter_res),
                       create_unq_name (CSE(R(n))->schema,
                                        n->sem.string_join.item_res)); */
        }
        case la_dummy:
            PFoops (OOPS_FATAL,
                    "dummy operators are not allowed");

        case la_rec_fix:
            return PFla_rec_fix (CSE(L(n)), CSE(R(n)));
        case la_rec_param:
            return PFla_rec_param (CSE(L(n)), CSE(R(n)));
        case la_rec_arg: {
            PFla_op_t *seed   = CSE(L(n));
            PFla_op_t *result = CSE(R(n));

            PFarray_t *seed_actmap = create_actcol_map ();
            PFarray_t *res_actmap  = create_actcol_map ();

            assert (CSE(L(n))->schema.count ==
                    L(n)->schema.count);
            assert (CSE(R(n))->schema.count ==
                    R(n)->schema.count);

            unsigned int i = 0;
            for (i = 0; i < L(n)->schema.count; i++) {
                if (L(n)->schema.items[i].name !=
                    CSE(L(n))->schema.items[i].name)
                    break;
            }


            /* a projection is needed */
            if (i < L(n)->schema.count) {
                PFalg_proj_t *p = (PFalg_proj_t *)
                    PFmalloc (L(n)->schema.count * sizeof (PFalg_proj_t));

                for (i = 0; i < L(n)->schema.count; i++) {
                    p[i] = PFalg_proj (
                              L(n)->schema.items[i].name,
                              CSE(L(n))->schema.items[i].name);
                    INACTCOL (seed_actmap,
                              actcol (L(n)->schema.items[i].name,
                                      CSE(L(n))->schema.items[i].name));
                }

                PFla_op_t *prev = seed;

                seed = PFla_project_ (CSE(L(n)), L(n)->schema.count, p);

                /* insert dummy operator in the original plan */
                PFalg_proj_t *dummy_p = (PFalg_proj_t*)
                                         PFmalloc (L(n)->schema.count *
                                                   sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                     dummy_p[i] = PFalg_proj (L(n)->schema.items[i].name,
                                              L(n)->schema.items[i].name);
                }

                L(n) = PFla_project_ (L(n), L(n)->schema.count, dummy_p);
                INACT (L(n), seed_actmap);

                /* map the new operator to the cse operator
                 * of the previous operator
                 * to ensure the equality (match ()) */
                INCSE (L(n), prev);
            }

            for (i = 0; i < R(n)->schema.count; i++) {
                if (R(n)->schema.items[i].name !=
                    CSE(R(n))->schema.items[i].name)
                    break;
            }

            /* a projection is needed */
            if (i <= R(n)->schema.count) {
                PFalg_proj_t *p = (PFalg_proj_t *)
                    PFmalloc (R(n)->schema.count * sizeof (PFalg_proj_t));

                for (i = 0; i < R(n)->schema.count; i++) {
                    p[i] = PFalg_proj (
                              R(n)->schema.items[i].name,
                              CSE(R(n))->schema.items[i].name);
                    INACTCOL (res_actmap,
                              actcol (R(n)->schema.items[i].name,
                                      CSE(R(n))->schema.items[i].name));
                }

                PFla_op_t *prev = result;

                result = PFla_project_ (CSE(R(n)), R(n)->schema.count, p);

                /* insert dummy operator in the original plan */
                PFalg_proj_t *dummy_p = (PFalg_proj_t*)
                                         PFmalloc (R(n)->schema.count *
                                                   sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                     dummy_p[i] = PFalg_proj (R(n)->schema.items[i].name,
                                              R(n)->schema.items[i].name);
                }

                R(n) = PFla_project_ (R(n), R(n)->schema.count, dummy_p);
                INACT (R(n), res_actmap);

                /* map the new operator to the cse operator
                 * of the previous operator
                 * to ensure the equality (match ()) */
                INCSE (R(n), prev);
            }

            return PFla_rec_arg (seed, result,
                                 CSE(n->sem.rec_arg.base));
        }
        case la_rec_base:
            return PFla_rec_base (n->schema);

        case la_fun_call: {
            PFalg_schema_t schema;
            schema.count = n->schema.count;
            schema.items = (struct PFalg_schm_item_t*)
                           PFmalloc (schema.count *
                                     sizeof (struct PFalg_schm_item_t));

            /* not needed to change the schema */
            for (unsigned int i = 0; i < schema.count; i++)
                schema.items[i] = n->schema.items[i];

            return PFla_fun_call (CSE(L(n)), CSE(R(n)),
                                  schema, n->sem.fun_call.kind,
                                  n->sem.fun_call.qname, n->sem.fun_call.ctx,
                                  ACTCOL (L(n), n->sem.fun_call.iter),
                                  n->sem.fun_call.occ_ind);
        }
        case la_fun_param: {
            /* copy the schema */
            PFalg_schema_t schema;
            schema.count = n->schema.count;
            schema.items = (struct PFalg_schm_item_t *)
                           PFmalloc (schema.count *
                                     sizeof (struct PFalg_schm_item_t));

            for (unsigned int i = 0; i < schema.count; i++)
                schema.items[i] = (struct PFalg_schm_item_t) {
                                      .name = ACTCOL (L(n), n->schema.items[i].name),
                                      .type = n->schema.items[i].type
                                  };

            return PFla_fun_param (
                        CSE (L(n)), CSE(R(n)),
                        schema);
        }
        case la_fun_frag_param:
            return PFla_fun_frag_param (
                        CSE (L(n)), CSE(R(n)),
                        n->sem.col_ref.pos);
        case la_frag_extract:
            return PFla_frag_extract (
                        CSE (L(n)),
                        n->sem.col_ref.pos);
        case la_proxy:
        case la_proxy_base:
        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "Logical operator cloning does not"
                    "support node kind (%s).",
                    ID[n->kind]);
            break;
    }
    return NULL; /* satisfy picky compilers */
}

/**
 * Since we create new operators for every operator
 * in the original plan, we have to create a projection
 * list if the names of the columns will change.
 */
static void
adjust_operator (PFla_op_t *ori, PFla_op_t *cse)
{
    /* trivial check for kind equivalence */
    assert (ori->kind == cse->kind);

    PFarray_t *actmap = NULL;

    switch (ori->kind) {
        case la_serialize_seq:
            actmap = actcol_map_copy (ACT(R(ori)));
            break;

        case la_serialize_rel:
            actmap = actcol_map_copy (ACT(R(ori)));
            break;

        case la_side_effects:
            actmap = actcol_map_copy (ACT(R(ori)));
            break;

        case la_lit_tbl:
        {
            actmap =  create_actcol_map ();

            /* during the creation of the map
             * we have to protocol the columns
             * we have just seen, to avoid a column to be
             * used twice */
            PFalg_collist_t *seen = PFalg_collist(DEF_WIDTH);
            for (unsigned int i = 0; i < ori->schema.count; i++) {
                PFalg_col_t col = littbl_column (ori->schema.items[i].name,
                                                 ori, cse, seen);

                cladd (seen) = col;

                INACTCOL (actmap,
                          actcol (col, ori->schema.items[i].name));
            }
        }   break;

        case la_empty_tbl:
        {
            actmap = create_actcol_map ();

            assert (cse->schema.count == ori->schema.count);

            for (unsigned int i = 0; i < ori->schema.count; i++) {
                INACTCOL (actmap,
                          actcol (cse->schema.items[i].name,
                                  ori->schema.items[i].name));
            }
        }   break;

        case la_ref_tbl:
            actmap = create_actcol_map ();

            assert (ori->schema.count == cse->schema.count);

            /* we assume that columns are aligned, see match () */
            for (unsigned int i = 0; i < ori->schema.count; i++) {
                 INACTCOL (actmap,
                           actcol (cse->schema.items[i].name,
                                   ori->schema.items[i].name));
            }
            break;

        case la_attach:
            actmap = actcol_map_copy (ACT(L(ori)));
            INACTCOL(actmap,
                     actcol (cse->sem.attach.res, ori->sem.attach.res));
            break;

        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
        {
            actmap = actcol_map_copy (ACT(L(ori)));
            for (unsigned int i = 0; i < PFarray_last (ACT(R(ori))); i++) {
                *(actcol_map_t *) PFarray_add (actmap) =
                           *(actcol_map_t *) PFarray_at (ACT(R(ori)), i);
            }
        }   break;

        case la_semijoin:
            actmap = actcol_map_copy (ACT(L(ori)));
            break;

        case la_project:
        {
            unsigned int i = 0;

            actmap = create_actcol_map ();

            assert (cse->sem.proj.count == ori->sem.proj.count);

            /* check for consistency */
            for (i = 0; i < cse->sem.proj.count; i++) {
                 if (cse->sem.proj.items[i].old !=
                     ACTCOL (L(ori), ori->sem.proj.items[i].old))
                     PFoops (OOPS_FATAL,
                             "projection name not found");
            }

            /* very restricted due to alignment of columns */
            /* adjustment */
            for (i = 0; i < cse->sem.proj.count; i++) {
                 if (cse->sem.proj.items[i].old ==
                     ACTCOL (L(ori), ori->sem.proj.items[i].old)) {
                     INACTCOL (actmap,
                               actcol (cse->sem.proj.items[i].new,
                                       ori->sem.proj.items[i].new));
                 }
            }
        }   break;

        case la_select:
        case la_pos_select:
            actmap = actcol_map_copy (ACT(L(ori)));
            break;

        case la_disjunion:
        case la_intersect:
        case la_difference:
            actmap = actcol_map_copy (ACT(L(ori)));
            break;

        case la_distinct:
            actmap = actcol_map_copy (ACT(L(ori)));
            break;

        case la_fun_1to1:
            actmap = actcol_map_copy (ACT(L(ori)));
            INACTCOL (actmap,
                      actcol (cse->sem.fun_1to1.res,
                              ori->sem.fun_1to1.res));
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            actmap = actcol_map_copy (ACT(L(ori)));
            INACTCOL(actmap,
                     actcol (cse->sem.binary.res, ori->sem.binary.res));
            break;

        case la_bool_not:
            actmap = actcol_map_copy (ACT(L(ori)));
            INACTCOL(actmap,
                     actcol (cse->sem.unary.res, ori->sem.unary.res));
            break;

        case la_aggr:
            actmap = create_actcol_map ();

            for (unsigned int i = 0; i < cse->sem.aggr.count; i++)
                INACTCOL (actmap,
                          actcol (cse->sem.aggr.aggr[i].res,
                                  ori->sem.aggr.aggr[i].res));

            assert ((cse->sem.aggr.part == col_NULL &&
                     ori->sem.aggr.part == col_NULL) ||
                    (cse->sem.aggr.part != col_NULL &&
                     ori->sem.aggr.part != col_NULL));

            /* if grouping is performed on the same attribute as the
               aggregate function is applied don't add this item to
               the mapping structure */
            if ((cse->sem.aggr.part != col_NULL))
                INACTCOL (actmap,
                          actcol (cse->sem.aggr.part, ori->sem.aggr.part));
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
           actmap = actcol_map_copy (ACT(L(ori)));
           INACTCOL (actmap,
                     actcol (cse->sem.sort.res, ori->sem.sort.res));
           break;

        case la_rowid:
            actmap = actcol_map_copy (ACT(L(ori)));
            INACTCOL (actmap,
                      actcol (cse->sem.rowid.res, ori->sem.rowid.res));
            break;

        case la_type:
            actmap = actcol_map_copy (ACT(L(ori)));
            INACTCOL (actmap,
                      actcol (cse->sem.type.res, ori->sem.type.res));
            break;

        case la_type_assert:
            actmap = actcol_map_copy (ACT(L(ori)));
            break;

        case la_cast:
            actmap = actcol_map_copy (ACT(L(ori)));
            INACTCOL (actmap,
                      actcol (cse->sem.type.res, ori->sem.type.res));
            break;

        case la_step:
        case la_guide_step:
            actmap = create_actcol_map ();
            INACTCOL (actmap,
                      actcol (cse->sem.step.item_res, ori->sem.step.item_res),
                      actcol (cse->sem.step.iter, ori->sem.step.iter));
            break;

        case la_step_join:
        case la_guide_step_join:
            actmap = actcol_map_copy (ACT(R(ori)));
            INACTCOL (actmap,
                      actcol (cse->sem.step.item_res, ori->sem.step.item_res));
            break;

        case la_doc_index_join:
            actmap = actcol_map_copy (ACT(R(ori)));
            INACTCOL (actmap,
                      actcol (
                          cse->sem.doc_join.item_res,
                          ori->sem.doc_join.item_res));
            break;

        case la_doc_tbl:
            actmap = actcol_map_copy (ACT(L(ori)));
            INACTCOL (actmap,
                      actcol (
                          cse->sem.doc_tbl.res,
                          ori->sem.doc_tbl.res));
            break;

        case la_doc_access:
            actmap = actcol_map_copy (ACT(R(ori)));
            INACTCOL (actmap,
                      actcol (
                          cse->sem.doc_access.res,
                          ori->sem.doc_access.res));
            break;

        case la_twig:
            actmap = create_actcol_map ();
            INACTCOL (actmap,
                      actcol (
                          cse->sem.iter_item.item, ori->sem.iter_item.item),
                      actcol (
                          cse->sem.iter_item.iter, ori->sem.iter_item.iter));
            break;

        case la_fcns:
            actmap = create_actcol_map ();
            break;

        case la_attribute:
        case la_processi:
            actmap = create_actcol_map ();
            INACTCOL (actmap,
                      actcol (cse->sem.iter_item1_item2.item1,
                              cse->sem.iter_item1_item2.item1),
                      actcol (cse->sem.iter_item1_item2.item2,
                              ori->sem.iter_item1_item2.item2),
                      actcol (cse->sem.iter_item1_item2.iter,
                              ori->sem.iter_item1_item2.iter));
            break;

        case la_docnode:
            actmap = create_actcol_map ();
            INACTCOL (actmap,
                      actcol (cse->sem.docnode.iter, ori->sem.docnode.iter));
            break;

        case la_content:
            actmap = create_actcol_map ();
            INACTCOL (actmap,
                      actcol (cse->sem.iter_pos_item.item,
                              ori->sem.iter_pos_item.item),
                      actcol (cse->sem.iter_pos_item.pos,
                              ori->sem.iter_pos_item.pos),
                      actcol (cse->sem.iter_pos_item.iter,
                              ori->sem.iter_pos_item.iter));
            break;

        case la_element:
        case la_textnode:
        case la_comment:
            actmap = create_actcol_map ();
            INACTCOL (actmap,
                      actcol (
                          cse->sem.iter_item.item, ori->sem.iter_item.item),
                      actcol (
                          cse->sem.iter_item.iter, ori->sem.iter_item.iter));
            break;

        case la_merge_adjacent:
            actmap = create_actcol_map ();
            INACTCOL (actmap,
                      actcol (cse->sem.merge_adjacent.iter_res,
                              ori->sem.merge_adjacent.iter_res),
                      actcol (cse->sem.merge_adjacent.pos_res,
                              ori->sem.merge_adjacent.pos_res),
                      actcol (cse->sem.merge_adjacent.item_res,
                              ori->sem.merge_adjacent.item_res));
            break;

        case la_roots:
            actmap = actcol_map_copy (ACT(L(ori)));
            break;

        case la_fragment:
        case la_empty_frag:
        case la_frag_union:
        case la_frag_extract:
            /* this operator doesn't have schema information and thus no
             * projection list */
            actmap = create_actcol_map ();
            break;

        case la_error:
        case la_cache:
            actmap = actcol_map_copy (ACT(R(ori)));
            break;

        case la_nil:
            actmap = create_actcol_map ();
            break;

        case la_trace:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            /* this operator doesn't have schema information and thus no
             * projection list */
            actmap = create_actcol_map ();
            break;

        case la_rec_fix:
            actmap = actcol_map_copy (ACT(R(ori)));
            break;

        case la_rec_param:
            /* this operator doesn't have schema information and thus no
             * projection list */
            actmap = create_actcol_map ();
            break;

        case la_rec_arg:
            actmap = actcol_map_copy (ACT(L(ori)));
            break;

        case la_rec_base:
            actmap = create_actcol_map ();

            assert (ori->schema.count == cse->schema.count);

            for (unsigned int i = 0; i < ori->schema.count; i++) {
                INACTCOL (actmap,
                          actcol (
                             cse->schema.items[i].name,
                             ori->schema.items[i].name));
            }
            break;

        case la_fun_call:
            actmap = create_actcol_map ();
            /* the schema doesn't change for this operator */
            for (unsigned int i = 0; i < ori->schema.count; i++) {
                INACTCOL (actmap,
                          actcol (
                             ori->schema.items[i].name,
                             ori->schema.items[i].name));
            }
            break;
        case la_fun_param:
            /* simply copy the schema of the left operator */
            actmap = actcol_map_copy (ACT(L(ori)));
            break;
        case la_fun_frag_param:
            /* since this operator doesn't has an associated schema,
             * we create an empty map */
            actmap = create_actcol_map ();
            break;
        case la_proxy:
        case la_proxy_base:
        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "This operator (%s) shouldn't appear in the plan",
                    ID[ori->kind]);

        case la_string_join:
            actmap = create_actcol_map ();
            INACTCOL (actmap,
                      actcol (
                          cse->sem.string_join.item_res,
                          ori->sem.string_join.item_res),
                      actcol (
                          cse->sem.string_join.iter_res,
                          ori->sem.string_join.iter_res));
            break;

        case la_dummy:
            /* there should be no dummy operator in this plan */
            PFoops (OOPS_FATAL,
                    "In this plan dummy operators should not occur");
            break;
    }

    INACT (ori, actmap);
}

/**
 * Core of common subexpression elimination.
 */
static void
la_cse (PFla_op_t *n)
{
    PFarray_t *a = NULL;

    assert (n);

    /* descend only once */
    if (SEEN (n))
        return;

    /* bottom up traversal to construct the
     * DAG from the logical algebra plan
     */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        la_cse (n->child[i]);

    /* at this point every child of n is linked to a node
     * in the CSE plan */

    /*
     * Fetch the subexpressions for this node kind that
     * we encountered so far (maintain one array for each
     * node type). If we have not yet seen any
     * subexpressions of that kind, we will have to
     * initialize the respective array first.
     */
    a = *(PFarray_t **) PFarray_at (subexps, n->kind);

    if (!a)
        *(PFarray_t **) PFarray_at (subexps, n->kind)
            = a = PFarray (sizeof (PFla_op_t *), 15);

    PFla_op_t *temp = NULL;

    /* see if we already saw this subexpression */
    for (unsigned int i = 0; i < PFarray_last (a); i++) {
        if (match (n, ORI(*(PFla_op_t **) PFarray_at (a, i)))) {
            temp = *(PFla_op_t **) PFarray_at (a, i);
        }
    }

    /* check if we already saw the subexpression */
    if (!temp) {

        /* we haven't seen this operator before, so
         * we create a new one, reuse temp since is
         * not set */
        temp = new_operator (n);

        /* consistency check, no column in the schema
         * has the name col_NULL*/
        for (unsigned int i = 0; i < temp->schema.count; i++) {
             if (temp->schema.items[i].name == col_NULL)
                 PFoops (OOPS_FATAL,
                         "(NULL) found in schema of %s", ID[temp->kind]);
        }

        /* if we haven't seen it add it to
         * the seen expressions */
        *(PFla_op_t **) PFarray_add (a) = temp;


        /* mark n as the original node triggered the
         * insertion of the cse node temp */
        INORI (temp, n);
    }

    /* map n to the node in the cse plan */
    INCSE (n, temp);

    /* adjust the projection list of the original operator */
    adjust_operator (n, temp);

    SEEN (n) = true;
}

/**
 * Remove all dummy operator from algebra plan.
 * We swapped this out from CSE to reduce
 * complexity.
 */
static void
remove_dummies (PFla_op_t *n)
{
    assert (n);

    /* descend only once */
    if (SEEN (n))
        return;

    /* top-down traversal to remove the dummies */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++) {
        while (n->child[i]->kind == la_dummy)
            n->child[i] = L(n->child[i]);

        remove_dummies (n->child[i]);
    }

    SEEN (n) = true;
}

/**
 * Eliminate common subexpressions in logical algebra tree and
 * convert the expression @em tree into a @em DAG.  (Actually,
 * the input often is already a DAG due to the way it was built
 * in core2alg.brg.  This function will detect @em any common
 * subexpression, though.)
 *
 * @param  root logical algebra tree
 * @return the equivalent of @a root, with common subexpressions
 *         translated into @em sharing in a DAG.
 */
PFla_op_t *
PFalgopt_cse (PFla_op_t *root)
{
    /* initialize maps */
    cse_map = PFarray (sizeof (ori_cse_map_t), 256);
    actcol_map = PFarray (sizeof (ori_actcol_map_t), 256);
    ori_map = PFarray (sizeof (cse_ori_map_t), 256);

    /* initialize subexpression array (and clear it) */
    subexps = PFcarray (sizeof (PFarray_t *), 128);

    /* first remove the dummies */
    remove_dummies (root);
    PFla_dag_reset (root);

    /* eliminate subexpressions */
    la_cse (root);

    /* search the root in the cse plan
     * and return the cse root */
    return CSE (root);
}

/* vim:set shiftwidth=4 expandtab: */

