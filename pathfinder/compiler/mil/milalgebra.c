/**
 * @file
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
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include <assert.h>

#include "pathfinder.h"
#include "milalgebra.h"
#include "mem.h"
#include "oops.h"

/**
 * Demote type @a t: If @a t is @c void, return @c oid. Otherwise return
 * @a t itself.
 */
#define demote(t) (((t) == m_void) ? m_oid : (t))

/**
 * Generic constructor to create a new MIL algebra leaf node.
 */
static PFma_op_t *
leaf (enum PFma_kind_t kind, PFma_type_t t)
{
    PFma_op_t *ret = PFmalloc (sizeof (PFma_op_t));

    /* [void,void] BATs are not allowed in MonetDB */
    assert (t.kind == type_atom
            || t.kind == type_none
            || !(t.ty.bat.htype == m_void && t.ty.bat.ttype == m_void));

    ret->kind   = kind;
    ret->type   = t;
    ret->refctr = 0;
    ret->usectr = 0;
    ret->varname = NULL;

    for (unsigned int i = 0; i < MILALGEBRA_MAXCHILD; i++)
        ret->child[i] = NULL;

    return ret;
}

/**
 * Generic constructor to create a new MIL algebra tree node with one child.
 */
static PFma_op_t *
wire1 (enum PFma_kind_t kind, PFma_type_t t, const PFma_op_t *n)
{
    PFma_op_t *ret = leaf (kind, t);

    assert (n);

    ret->child[0] = (PFma_op_t *) n;

    return ret;
}

/**
 * Generic constructor to create a new MIL algebra tree node with
 * two children.
 */
static PFma_op_t *
wire2 (enum PFma_kind_t kind, PFma_type_t t,
       const PFma_op_t *n1, const PFma_op_t *n2)
{
    PFma_op_t *ret = wire1 (kind, t, n1);

    assert (n2);

    ret->child[1] = (PFma_op_t *) n2;

    return ret;
}

/**
 * Generic constructor to create a new MIL algebra tree node with
 * two children.
 */
static PFma_op_t *
wire3 (enum PFma_kind_t kind, PFma_type_t t,
       const PFma_op_t *n1, const PFma_op_t *n2, const PFma_op_t *n3)
{
    PFma_op_t *ret = wire2 (kind, t, n1, n2);

    assert (n3);

    ret->child[2] = (PFma_op_t *) n3;

    return ret;
}

/**
 * Generic constructor to create a new MIL algebra tree node with
 * two children.
 */
static PFma_op_t *
wire4 (enum PFma_kind_t kind, PFma_type_t t,
       const PFma_op_t *n1, const PFma_op_t *n2, const PFma_op_t *n3,
       const PFma_op_t *n4)
{
    PFma_op_t *ret = wire3 (kind, t, n1, n2, n3);

    assert (n4);

    ret->child[3] = (PFma_op_t *) n4;

    return ret;
}

/**
 * Generic constructor to create a new MIL algebra tree node with
 * two children.
 */
static PFma_op_t *
wire5 (enum PFma_kind_t kind, PFma_type_t t,
       const PFma_op_t *n1, const PFma_op_t *n2, const PFma_op_t *n3,
       const PFma_op_t *n4, const PFma_op_t *n5)
{
    PFma_op_t *ret = wire4 (kind, t, n1, n2, n3, n4);

    assert (n5);

    ret->child[4] = (PFma_op_t *) n5;

    return ret;
}

/**
 * Generic constructor to create a new MIL algebra tree node with
 * two children.
 */
static PFma_op_t *
wire6 (enum PFma_kind_t kind, PFma_type_t t,
       const PFma_op_t *n1, const PFma_op_t *n2, const PFma_op_t *n3,
       const PFma_op_t *n4, const PFma_op_t *n5, const PFma_op_t *n6)
{
    PFma_op_t *ret = wire5 (kind, t, n1, n2, n3, n4, n5);

    assert (n6);

    ret->child[5] = (PFma_op_t *) n6;

    return ret;
}

/**
 * Generic constructor to create a new MIL algebra tree node with
 * two children.
 */
static PFma_op_t *
wire7 (enum PFma_kind_t kind, PFma_type_t t,
       const PFma_op_t *n1, const PFma_op_t *n2, const PFma_op_t *n3,
       const PFma_op_t *n4, const PFma_op_t *n5, const PFma_op_t *n6,
       const PFma_op_t *n7)
{
    PFma_op_t *ret = wire6 (kind, t, n1, n2, n3, n4, n5, n6);

    assert (n7);

    ret->child[6] = (PFma_op_t *) n7;

    return ret;
}

static PFma_op_t *
lit_val (PFma_val_t v)
{
    PFma_op_t        *ret = NULL;
    enum PFmil_kind_t ty;
    PFma_type_t       t;

    switch (v.type) {
        case m_oid:  ty = ma_lit_oid; break;
        case m_int:  ty = ma_lit_int; break;
        case m_dbl:  ty = ma_lit_dbl; break;
        case m_str:  ty = ma_lit_str; break;
        case m_bit:  ty = ma_lit_bit; break;
        default: PFoops (OOPS_FATAL, "Error in milalgebra.c:lit_val");
    }

    t = (PFma_type_t) { .kind = type_atom, .ty.atom = v.type };

    ret = leaf (ty, t);
    /*
                (PFma_type_t) { .kind = type_atom, .ty.atom = v.type });
    */
    ret->sem.lit_val = v;

    return ret;
}

PFma_op_t *
PFma_lit_oid (oid o)
{
    return lit_val ((PFma_val_t) { .type = m_oid, .val.o = o });
}

PFma_op_t *
PFma_lit_int (int i)
{
    return lit_val ((PFma_val_t) { .type = m_int, .val.i = i });
}

PFma_op_t *
PFma_lit_str (char *s)
{
    return lit_val ((PFma_val_t) { .type = m_str, .val.s = s });
}

PFma_op_t *
PFma_lit_bit (bool b)
{
    return lit_val ((PFma_val_t) { .type = m_bit, .val.b = b });
}

PFma_op_t *
PFma_lit_dbl (double d)
{
    return lit_val ((PFma_val_t) { .type = m_dbl, .val.d = d });
}


/**
 * MIL new() operator: construct a new BAT
 *
 * @param htype Head type of the new BAT (first argument of MonetDB's
 *              new() function.
 * @param ttype Tail type of the new BAT (first argument of MonetDB's
 *              new() function.
 */
PFma_op_t *
PFma_new (PFmil_type_t htype, PFmil_type_t ttype)
{
    PFma_op_t *ret = leaf (ma_new,
                           (PFma_type_t) { .kind = type_bat,
                                           .ty = {
                                               .bat = { .htype = htype,
                                                        .ttype = ttype } } } );

    ret->sem.new.htype = htype;
    ret->sem.new.ttype = ttype;

    return ret;
}

/**
 * MIL insert(a,b,c) operator: Insert head and tail value into a BAT.
 */
PFma_op_t *
PFma_insert (const PFma_op_t *n,
             const PFma_op_t *hvalue, const PFma_op_t *tvalue)
{
    /* values to insert must match BAT schema */
    assert (hvalue->type.kind == type_atom
            && demote (n->type.ty.bat.htype) == demote (hvalue->type.ty.atom));
    assert (tvalue->type.kind == type_atom
            && demote (n->type.ty.bat.ttype) == demote (tvalue->type.ty.atom));

    return wire3 (ma_insert, n->type, n, hvalue, tvalue);
}

/**
 * seqbase: Set sequence base of a BAT.
 */
PFma_op_t *
PFma_seqbase (const PFma_op_t *n, const PFma_op_t *seqbase)
{
    /*
     * The seqbase operator is only allowed on [void,any] BATs.
     * We cannot exactly derive the void property, though. Hence, we also
     * accept oid here.
     */
    assert (n->type.kind == type_bat && demote (n->type.ty.bat.htype) == m_oid);

    return wire2 (ma_seqbase, n->type, n, seqbase);
}

/**
 * MIL project(): Set tail to a constant value.
 */
PFma_op_t *
PFma_project (const PFma_op_t *n1, const PFma_op_t *n2)
{
    assert (n1->type.kind == type_bat && n2->type.kind == type_atom);

    return wire2 (ma_project,
                  (PFma_type_t) { .kind = type_bat,
                                  .ty = {
                                      .bat = {
                                          .htype = n1->type.ty.bat.htype,
                                          .ttype = n2->type.ty.atom } } },
                  n1, n2);
}

/**
 * MIL reverse(): Swap head and tail.
 */
PFma_op_t *
PFma_reverse (const PFma_op_t *n)
{
    assert (n->type.kind == type_bat);

    /* swap head and tail types */
    return wire1 (ma_reverse,
                  (PFma_type_t) { .kind = type_bat,
                                  .ty = {
                                      .bat = {
                                          .htype = n->type.ty.bat.ttype,
                                          .ttype = n->type.ty.bat.htype } } },
                  n);

}

/**
 * MIL sort(): Sort BAT by head values.
 */
PFma_op_t *
PFma_sort (const PFma_op_t *n)
{
    assert (n->type.kind == type_bat);

    /*
     * sort() sorts its argument by head values. This makes void columns
     * be turned into oid columns.
     */
    return wire1 (ma_sort,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = {
                          .bat = {
                              .htype = demote (n->type.ty.bat.htype),
                              .ttype = demote (n->type.ty.bat.ttype) } } },
                  n);
}

/**
 * MIL CTrefine(): Refine sorting of a BAT by another column.
 * (This allows for lexicographic sorting by a sequence of
 * attributes, see the MonetDB xtables module.)
 */
PFma_op_t *
PFma_ctrefine (const PFma_op_t *a, const PFma_op_t *b)
{
    assert (a->type.kind == type_bat);
    assert (b->type.kind == type_bat);

    /* CTrefine(BAT[oid,any], BAT[oid,any]) : BAT[oid,oid] */
    assert (demote (a->type.ty.bat.htype) == m_oid);
    assert (demote (b->type.ty.bat.htype) == m_oid);

    return wire2 (ma_ctrefine,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = { .htype = m_oid, .ttype = m_oid } } },
                  a, b);
}

/**
 * MIL join(): Join two BATs.
 */
PFma_op_t *
PFma_join (const PFma_op_t *left, const PFma_op_t *right)
{
    assert (demote (left->type.ty.bat.ttype)
            == demote (right->type.ty.bat.htype));

    return wire2 (ma_join,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = {
                                  .htype = left->type.ty.bat.htype,
                                  .ttype = demote (left->type.ty.bat.ttype) }
                  } },
                  left,
                  right);
}

/**
 * MIL leftjoin(): Join two BATs, keeping order of left BAT.
 */
PFma_op_t *
PFma_leftjoin (const PFma_op_t *left, const PFma_op_t *right)
{
    assert (demote (left->type.ty.bat.ttype)
            == demote (right->type.ty.bat.htype));

    return wire2 (ma_leftjoin,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = {
                                  .htype = left->type.ty.bat.htype,
                                  .ttype = demote (left->type.ty.bat.ttype) }
                  } },
                  left,
                  right);
}

/**
 * MIL cross(): Cross product between two BATs.
 */
PFma_op_t *
PFma_cross (const PFma_op_t *left, const PFma_op_t *right)
{
    assert (demote (left->type.ty.bat.ttype)
            == demote (right->type.ty.bat.htype));

    return wire2 (ma_cross,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = {
                                  .htype = demote (left->type.ty.bat.htype),
                                  .ttype = demote (left->type.ty.bat.ttype) }
                  } },
                  left,
                  right);
}

/**
 * MIL mirror(): Copy head into tail.
 */
PFma_op_t *
PFma_mirror (const PFma_op_t *n)
{
    return wire1 (ma_mirror,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = {
                          .htype = demote (n->type.ty.bat.htype),
                          .ttype = n->type.ty.bat.htype } } },
                  n);
}

/**
 * MIL kunique() (``key unique''): Eliminate duplicates by looking
 * at head values only.
 */
PFma_op_t *
PFma_kunique (const PFma_op_t *n)
{
    assert (n->type.kind == type_bat);

    /* FIXME: correct? */
    return wire1 (ma_kunique,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = {
                          .htype = n->type.ty.bat.htype,
                          .ttype = demote (n->type.ty.bat.ttype) } } },
                  n);
}

/**
 * MIL mark_grp(): Generate sequential numbers in a grouped fashion.
 * (Re-start numbering for every partition. See the mark_grp()
 * function in the pf_support module.)
 */
PFma_op_t *
PFma_mark_grp (const PFma_op_t *b, const PFma_op_t *g)
{
    assert (b->type.kind == type_bat && g->type.kind == type_bat);
    assert (demote (b->type.ty.bat.ttype) == m_oid);
    assert (demote (g->type.ty.bat.htype) == m_oid);
    assert (demote (g->type.ty.bat.ttype) == m_oid);

    return wire2 (ma_mark_grp,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = { .htype = b->type.ty.bat.ttype,
                                       .ttype = m_oid } } },
                  b, g);
}

/**
 * MIL mark(): Generate sequential numbers in BAT tail. (This
 * typically makes the tail a void column.)
 */
PFma_op_t *
PFma_mark (const PFma_op_t *n, const PFma_op_t *base)
{
    assert (n->type.kind == type_bat);
    assert (base->type.kind == type_atom && base->type.ty.atom == m_oid);

    return wire2 (ma_mark,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = { .htype = n->type.ty.bat.htype,
                                       .ttype = n->type.ty.bat.htype == m_void
                                                ? m_oid : m_void } } },
                  n, base);
}

/**
 * MIL append(a,b): Append BAT b to BAT a. In MonetDB, this is a
 * synonym for insert(a,b). Pathfinder, however, only knows Operators
 * with a fixed arity, so we named this one `append', the other one
 * `insert'.
 */
PFma_op_t *
PFma_append (const PFma_op_t *a, const PFma_op_t *b)
{
    assert (a->type.kind == type_bat && b->type.kind == type_bat);
    assert (demote (a->type.ty.bat.htype) == demote (b->type.ty.bat.htype));
    assert (demote (a->type.ty.bat.ttype) == demote (b->type.ty.bat.ttype));

    /* Be defensive here: Turn any oid into a void. */
    return wire2 (ma_append,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = {
                          .bat = {
                              .htype = demote (a->type.ty.bat.htype),
                              .ttype = demote (a->type.ty.bat.ttype) } } },
                  a, b);
}

#if 0
PFma_op_t *
PFma_shift (const PFma_op_t *n, const PFma_op_t *offset)
{
    assert (n->type.kind == type_bat);
    assert (demote (n->type.ty.bat.htype) == m_oid);
    assert (offset->type.kind == type_atom && offset->type.ty.atom == m_int);

    return wire2 (ma_shift,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = { .htype = n->type.ty.bat.htype,
                                       .ttype = n->type.ty.bat.ttype } } },
                  n, offset);
}
#endif

PFma_op_t *
PFma_count (const PFma_op_t *n)
{
    assert (n->type.kind == type_bat);

    return wire1 (ma_count,
                  (PFma_type_t) { .kind = type_atom, .ty = { .atom = m_int } },
                  n);
}

PFma_op_t *
PFma_oid (const PFma_op_t *n)
{
    assert (n->type.kind == type_atom);

    return wire1 (ma_oid,
                  (PFma_type_t) { .kind = type_atom, .ty = { .atom = m_oid } },
                  n);
}

PFma_op_t *
PFma_moid (const PFma_op_t *n)
{
    assert (n->type.kind == type_bat);

    return wire1 (ma_moid,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = { .htype = n->type.ty.bat.htype,
                                       .ttype = m_oid } } },
                  n);
}

PFma_op_t *
PFma_mint (const PFma_op_t *n)
{
    assert (n->type.kind == type_bat);

    return wire1 (ma_mint,
                  (PFma_type_t) {
                      .kind = type_bat,
                      .ty = { .bat = { .htype = n->type.ty.bat.htype,
                                       .ttype = m_int } } },
                  n);
}

PFma_op_t *
PFma_madd (const PFma_op_t *n1, const PFma_op_t *n2)
{
    assert (n1->type.kind == type_bat || n2->type.kind == type_bat);
    assert (
        (n1->type.kind == type_bat ? n1->type.ty.bat.ttype : n1->type.ty.atom)
        == (n2->type.kind == type_bat
            ? n2->type.ty.bat.ttype : n2->type.ty.atom));

    if (n1->type.kind == type_bat)
        return
            wire2 (ma_madd,
                   (PFma_type_t) {
                       .kind = type_bat,
                       .ty = {
                           .bat = {
                               .htype = n1->type.ty.bat.htype == m_void
                                         && (n2->type.kind == type_atom
                                             || n2->type.ty.bat.htype == m_void)
                                        ? m_void
                                        : demote (n1->type.ty.bat.htype),
                               .ttype = n1->type.ty.bat.ttype } } },
                   n1, n2);
    else
        return
            wire2 (ma_madd,
                   (PFma_type_t) {
                       .kind = type_bat,
                       .ty = {
                           .bat = {
                               .htype = n2->type.ty.bat.htype == m_void
                                         && (n1->type.kind == type_atom
                                             || n1->type.ty.bat.htype == m_void)
                                        ? m_void
                                        : demote (n2->type.ty.bat.htype),
                               .ttype = n2->type.ty.bat.ttype } } },
                   n1, n2);
}




PFma_op_t *PFma_serialize (const PFma_op_t *pos,
                           const PFma_op_t *item_int,
                           const PFma_op_t *item_str,
                           const PFma_op_t *item_dec,
                           const PFma_op_t *item_dbl,
                           const PFma_op_t *item_bln,
                           const PFma_op_t *item_node)
{
    return wire7 (ma_serialize,
                  (PFma_type_t) { .kind = type_none },
                  pos,
                  item_int, item_str, item_dec, item_dbl, item_bln, item_node);
}
