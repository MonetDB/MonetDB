/**
 * @file
 *
 * Compile MIL Algebra expression tree into MIL commands.
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
#include "milgen.h"
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

#include "mil_mnemonic.h"

/**
 * We collect the MIL program during compilation here.
 */
static PFmil_t *milprog = NULL;

/**
 * ``Execute'' MIL code (i.e., collect it in variable #milprog).
 *
 * Twig has been designed as a code generator, aiming at immediate
 * printing of generated code. Instead of printing the code directly,
 * we collect all the generated snippets via calls to execute().
 * Effectively, execute() builds up a MIL tree representing a long
 * expression sequence, with the tree root in #milprog.
 *
 * Call execute() with an arbitrary number of MIL commands (as
 * #PFmil_t pointers).
 *
 * In other Twig passes, compilation results have been collected by
 * references within the source tree (e.g., the ``core'' member variable
 * in struct #PFpnode_t). The algebra, however, is not necessarily a
 * real tree, but may be any directed graph. For each sub-expression,
 * we want to compute its value only once, when the expression is needed
 * the first time. For subsequent uses, we reference the ``old'' variable,
 * before finally deleting the MIL variable after the sub-expression has
 * been referenced the last time.
 *
 * If we built up a MIL tree parallel to the algebra tree, we could not
 * really ensure that computation is done before the first reference,
 * and that destroying is done after the last. By collecting code via
 * execute(), we are sure that MIL code will be output and processed
 * in exactly the same order as we generated it during compilation.
 */
#define execute(...) milprog = seq (milprog, __VA_ARGS__)

/**
 * ``Invent'' new MIL variable name.
 *
 * This function will return a unique string on each call that can
 * be used as a valid Monet identifier.
 */
static PFmil_ident_t
new_var (void)
{
    static unsigned int varno = 0;
    PFmil_ident_t ret = PFmalloc (sizeof ("a000000"));

    sprintf ((char *) ret, "a%06u", varno++);

    return ret;
}

#if 0
static PFmil_t *
literal (PFma_val_t v)
{
    switch (v.kind) {
        case v_oid:    return lit_oid (v.val.o);
        case v_int:    return lit_int (v.val.i);
        case v_dbl:    return lit_dbl (v.val.d);
        case v_str:    return lit_str (v.val.s);
        case v_bit:    return lit_bit (v.val.b);
    }

    PFoops (OOPS_FATAL,
            "illegal value in PFma_val_t.kind");
}
#endif

/**
 * Compile MIL Algebra expression tree into MIL command tree
 *
 * @param a MIL Algebra expression tree
 */
PFmil_t *
PFmilgen (PFma_op_t *a)
{
    assert (a);

    /* set reference counters and clear usage counters */
    /* FIXME
    set_refctr (a);
    clear_usectr (a);
    */

    /* initialize variable milprog, with a `no operation' node */
    milprog = nop ();

    /* invoke compilation */
    (void) rewrite (a, 0);

    /* return MIL code collected in variable milprog */
    return milprog;
}
