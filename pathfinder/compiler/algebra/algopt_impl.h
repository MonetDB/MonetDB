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
    , [aop_disjunion]    = disjunion  /**< union two relations w/ same schema */
    , [aop_cross]        = cross      /**< cross product (Cartesian product) */
    , [aop_eqjoin]       = eqjoin     /**< equi-join */
    , [aop_project]      = project    /**< projection and renaming operator */
    , [aop_rownum]       = rownum     /**< consecutive number generation */

    , [aop_serialize]    = serialize  /**< serialize algebra expression below
                                           (This is mainly used explicitly match
                                           the expression root during the Twig
                                           pass.) */
    , [aop_num_add]      = num_add      /**< arithmetic plus operator */
    , [aop_num_subtract] = num_subtract /**< arithmetic plus operator */
    , [aop_num_multiply] = num_multiply /**< arithmetic plus operator */
    , [aop_num_divide]   = num_divide   /**< arithmetic plus operator */

    , [aop_num_gt]       = num_gt       /**< numeric equal operator */
    , [aop_num_eq]       = num_eq       /**< numeric equal operator */

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
