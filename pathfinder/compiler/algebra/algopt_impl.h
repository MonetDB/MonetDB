/**
 * @file
 *
 * Rewrite/optimize algebra expression tree.
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

#include "pathfinder.h"
#include "algopt.h"

/** twig-generated node type identifiers */
#include "algopt.symbols.h"

/** twig: type of tree node */
#define TWIG_NODE PFalg_op_t

/** twig: max number of children under a parse tree node */
#define TWIG_MAXCHILD PFALG_OP_MAXCHILD

static int TWIG_ID[] = {
      [aop_lit_tbl]      = lit_tbl    /**< literal table */
    , [aop_doc_tbl]      = doc_tbl    /**< document table */

    , [aop_disjunion]    = disjunion  /**< union two relations w/ same
				       * schema */
    , [aop_difference]   = difference
    , [aop_intersect]    = intersect

    , [aop_cross]        = cross      /**< cross product (Cartesian product) */
    , [aop_eqjoin]       = eqjoin     /**< equi-join */
    , [aop_scjoin]       = scjoin     /**< staircase join */

    , [aop_rownum]       = rownum     /**< consecutive number generation */

    , [aop_project]      = project    /**< projection and renaming operator */
    , [aop_select]       = select_
    , [aop_sum]          = sum
    , [aop_count]        = count_
    , [aop_distinct]     = distinct

    , [aop_type]         = type
    , [aop_cast]         = cast         /**< algebra cast operator */

    , [aop_num_add]      = num_add      /**< arithmetic plus operator */
    , [aop_num_subtract] = num_subtract /**< arithmetic minus operator */
    , [aop_num_multiply] = num_multiply /**< arithmetic times operator */
    , [aop_num_divide]   = num_divide   /**< arithmetic divide operator */
    , [aop_num_modulo]   = num_modulo   /**< arithmetic modulo operator */
    , [aop_num_neg]      = num_neg      /**< arithmetic negation operator */

    , [aop_num_gt]       = num_gt       /**< numeric equal operator */
    , [aop_num_eq]       = num_eq       /**< numeric equal operator */

    , [aop_bool_and]     = and          /**< boolean and */
    , [aop_bool_or]      = or           /**< boolean or */
    , [aop_bool_not]     = not          /**< boolean negation */

    , [aop_element]      = element      /**< element construction */
    , [aop_attribute]    = attribute    /**< attribute construction */
    , [aop_textnode]     = textnode     /**< text node construction */
    , [aop_docnode]      = docnode      /**< document node construction */
    , [aop_comment]      = comment      /**< comment construction */
    , [aop_processi]     = processi     /**< pi construction */

    , [aop_concat]       = strconcat
    , [aop_merge_adjacent] = merge_adjacent
    
    , [aop_seqty1]       = seqty1
    , [aop_all]          = all

    , [aop_roots]        = roots_
    , [aop_fragment]     = fragment
    , [aop_frag_union]   = frag_union
    , [aop_empty_frag]   = empty_frag

    , [aop_serialize]    = serialize  /**< serialize algebra expression below
                                           (This is mainly used explicitly
                                           match the expression root during
                                           the Twig pass.) */

};

/** twig: setup twig */
#include "twig.h"

/* undefine the twig node ids because we introduce
 * MIL tree constructor functions of the same name below
 */
#undef lit_tbl
#undef doc_tbl
#undef disjunion
#undef difference
#undef intersect
#undef cross
#undef eqjoin
#undef scjoin
#undef rownum
#undef project
#undef select_
#undef sum
#undef count_
#undef distinct
#undef type
#undef cast
#undef num_add
#undef num_subtract
#undef num_multiply
#undef num_divide
#undef num_modulo
#undef num_neg
#undef num_gt
#undef num_eq
#undef and
#undef or
#undef not
#undef element
#undef attribute
#undef textnode
#undef docnode
#undef comment
#undef processi
#undef strconcat
#undef merge_adjacent
#undef seqty1
#undef all
#undef roots_
#undef fragment
#undef frag_union
#undef empty_frag
#undef serialize

/* ----------------------- End of twig setup -------------------- */

#include "algebra_mnemonic.h"
#include "string.h"

/**
 * Rewrite/optimize algebra expression tree. Returns modified tree.
 */
PFalg_op_t *
PFalgopt (PFalg_op_t *root)
{
    assert (root);

    return rewrite (root, 0);
}
