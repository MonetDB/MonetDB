/**
 * @file
 *
 * Compile Algebra expression tree into MIL algebra.
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include <assert.h>

/* strlen et al. */
#include <string.h>

#include "pathfinder.h"
#include "ma_gen.h"

/** twig-generated node type identifiers */
#include "ma_gen.symbols.h"

/** twig: type of tree node */
#define TWIG_NODE PFalg_op_t

/** twig: max number of children under a parse tree node */
#define TWIG_MAXCHILD PFALG_OP_MAXCHILD

static int TWIG_ID[] = {
      [aop_lit_tbl]      = lit_tbl    /**< literal table */
    , [aop_disjunion]    = disjunion  /**< union two relations w/ same schema */
    , [aop_cross]        = cross      /**< cross product (Cartesian product) */
    , [aop_eqjoin]       = eqjoin     /**< cross product (Cartesian product) */
    , [aop_project]      = project    /**< projection and renaming operator */
    , [aop_select]       = select_    /**< select by attribute = true */
    , [aop_rownum]       = rownum     /**< consecutive number generation */

    , [aop_serialize]    = serialize  /**< serialize algebra expression below
                                           (This is mainly used explicitly match
                                           the expression root during the Twig
                                           pass.) */
    , [aop_num_add]      = num_add      /**< arithmetic plus operator */
    , [aop_num_subtract] = num_subtract /**< arithmetic plus operator */
    , [aop_num_multiply] = num_multiply /**< arithmetic plus operator */
    , [aop_num_divide]   = num_divide   /**< arithmetic plus operator */

    , [aop_num_gt]       = num_gt       /**< numeric greater than operator */
    , [aop_num_eq]       = num_eq       /**< numeric greater than operator */

    , [aop_bool_not]     = not          /**< boolean negation */

    , [aop_cast]         = cast         /**< algebra cast operator */
};

/** twig: setup twig */
#include "twig.h"

/* undefine the twig node ids because we introduce
 * MIL tree constructor functions of the same name below
 */
#undef lit_tbl
#undef disjunion
#undef cross
#undef eqjoin
#undef project
#undef select_
#undef rownum
#undef serialize
#undef num_add
#undef num_subtract
#undef num_multiply
#undef num_divide
#undef num_gt
#undef num_eq
#undef not
#undef cast

/* ----------------------- End of twig setup -------------------- */

#include "milalgebra_mnemonic.h"


/*
 * With each algebra expression node, we keep an environment of
 * MIL algebra expressions that implement the algebra expression.
 * The environment is an array of mapping entries. Each entry
 * contains an attribute name (field @a att), an algebra type
 * (field @a ty; remember that there is a BAT for each algebra
 * type that might occur in this BAT), and the MIL algebra expression
 * tree that implements this attribute/type combination.
 */
struct enventry_t {
    PFalg_att_t           att;  /**< attribute name */
    PFalg_simple_type_t   ty;   /**< algebra type */
    PFma_op_t            *ma;   /**< MIL algebra expression */
};
typedef struct enventry_t enventry_t;

/**
 * Allocate a new environment
 */
static PFarray_t *
new_env (void)
{
    return PFarray (sizeof (enventry_t));
}

/**
 * Lookup MIL algebra expression in the environment @a env, given
 * the attribute/type combination @a att/@a ty.  Returns @c NULL
 * if no entry was found.
 */
static PFma_op_t **
lookup (PFarray_t *env, PFalg_att_t att, PFalg_simple_type_t ty)
{
    enventry_t   *e;
    unsigned int  i;

    assert (env);

    for (i = 0; i < PFarray_last (env); i++) {
        e = (enventry_t *) PFarray_at (env, i);
        if (!strcmp (e->att, att) && e->ty == ty)
            return &(e->ma);
    }

    /* create new entry in environment if we couldn't find one */
    *((enventry_t *) PFarray_add (env))
        = (enventry_t) { .att = PFstrdup (att), .ty = ty, .ma = NULL };
    
    /* return this new entry */
    return &(((enventry_t *) PFarray_at (env, PFarray_last (env) - 1))->ma);
}
#define LOOKUP(a,b,c) lookup ((a)->env, (b), (c))

static void
copy_env (PFalg_op_t *dest, PFalg_op_t *src)
{
    unsigned int i;

    if (dest->env)
        PFinfo (OOPS_WARNING,
                "Overwriting existing environment in ma_gen_impl.c:copy_env.");

    dest->env = new_env ();

    for (i = 0; i < PFarray_last (src->env); i++)
        *((enventry_t *) PFarray_add (dest->env))
            = *((enventry_t *) PFarray_at (src->env, i));
}


static PFma_op_t *
literal (PFalg_atom_t atom)
{
    switch (atom.type) {

        case aat_nat:  return lit_oid (atom.val.nat);
        case aat_int:  return lit_int (atom.val.int_);
        case aat_str:  return lit_str (atom.val.str);
        case aat_node: return lit_oid (atom.val.node);
        case aat_dec:  return lit_dbl (atom.val.dec);
        case aat_dbl:  return lit_dbl (atom.val.dbl);
        case aat_bln:  return lit_bit (atom.val.bln);

        default:       PFoops (OOPS_FATAL,
                               "A relational algebra type has not yet "
                               "been implemented.");
                       break;
    }
}

static PFmil_type_t
implty (PFalg_simple_type_t t)
{
    switch (t) {
        case aat_nat:    return m_oid;
        case aat_int:    return m_int;
        case aat_str:    return m_str;
        case aat_node:   return m_oid;
        case aat_dec:    return m_dbl;
        case aat_dbl:    return m_dbl;
        case aat_bln:    return m_bit;
    }

    PFoops (OOPS_FATAL, "Cannot determine implementation type of %x", t);
}

/**
 * Lookup type of attribute @a attname in algebra expression @a n.
 */
static PFalg_type_t
attr_type (PFalg_op_t *n, PFalg_att_t attname)
{
    int i;

    for (i = 0; i < n->schema.count; i++)
        if (!strcmp (n->schema.items[i].name, attname))
            return n->schema.items[i].type;

    PFoops (OOPS_FATAL,
            "Unable to determine type of attribute `%s': attribute not found.",
            attname);
}

/**
 * Test if the given type is a monomorphic type and return true
 * in that case. Otherwise return false.
 */
static bool
is_monomorphic (PFalg_type_t type)
{
    PFalg_simple_type_t t;

    for (t = 1; t; t <<= 1)
        if (t == type)
            return true;

    return false;
}


/**
 * Compile Algebra expression tree into MIL tree
 *
 * @param a Algebra expression tree
 */
PFma_op_t *
PFma_gen (PFalg_op_t *a)
{
    assert (a);

#if 0
    /* set reference counters and clear usage counters */
    set_refctr (a);
    clear_usectr (a);

    /* initialize variable milprog, with a `no operation' node */
    milprog = nop ();
#endif

    /* invoke compilation */
    (void) rewrite (a, 0);

    if (!(a->ma))
        PFoops (OOPS_FATAL, "MIL algebra generation failed.");

    return a->ma;
}
