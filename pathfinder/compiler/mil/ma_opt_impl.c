/**
 * @file
 *
 * MIL algebra optimization/simplification helper routines.
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
#include "ma_opt.h"
#include "milalgebra.h"

/** twig-generated node type identifiers */
#include "milgen.symbols.h"

/** twig: type of tree node */
#define TWIG_NODE PFma_op_t

/** twig: max number of children under a parse tree node */
#define TWIG_MAXCHILD MILALGEBRA_MAXCHILD

static int TWIG_ID[] = {
      [ma_serialize] = serialize
    , [ma_new]       = new
    , [ma_insert]    = insert_
    , [ma_seqbase]   = seqbase
    , [ma_project]   = project
    , [ma_reverse]   = reverse
    , [ma_sort]      = sort
    , [ma_ctrefine]  = ctrefine
    , [ma_join]      = join
    , [ma_leftjoin]  = leftjoin
    , [ma_cross]     = cross
    , [ma_mirror]    = mirror
    , [ma_kunique]   = kunique
    , [ma_mark_grp]  = mark_grp
    , [ma_mark]      = mark
    , [ma_append]    = append
    , [ma_count]     = count
    , [ma_oid]       = oid
    , [ma_moid]      = moid
    , [ma_mint]      = mint
    , [ma_madd]      = madd

    , [ma_lit_oid]   = lit_oid
    , [ma_lit_int]   = lit_int
    , [ma_lit_str]   = lit_str
    , [ma_lit_bit]   = lit_bit
    , [ma_lit_dbl]   = lit_dbl
};

/* undefine the twig node ids because we introduce
 * MIL tree constructor functions of the same name below
 */
#undef serialize
#undef new
#undef insert_
#undef seqbase
#undef project
#undef reverse
#undef sort
#undef ctrefine
#undef join
#undef leftjoin
#undef cross
#undef mirror
#undef kunique
#undef mark_grp
#undef mark
#undef append
#undef count
#undef oid
#undef moid
#undef mint
#undef madd
#undef lit_oid
#undef lit_int
#undef lit_str
#undef lit_bit
#undef lit_dbl

/** twig: setup twig */
#include "twig.h"

/* ----------------------- End of twig setup -------------------- */

/**
 * Optimize/simplify MIL algebra tree.
 */
PFma_op_t *
PFma_opt (PFma_op_t *a)
{
    assert (a);

    return rewrite (a, 0);
}

/* vim:set shiftwidth=4 expandtab: */
