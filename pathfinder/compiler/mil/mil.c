/**
 * @file
 *
 * Constructor functions to handle MIL tree
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

#include <assert.h>

#include "pathfinder.h"

#include "mil.h"

#include "mem.h"

/**
 * Construct a MIL tree leaf node with given node kind.
 *
 * @param k Kind of the newly constructed node.
 * @return A MIL tree node type, with kind set to @a k, and all children
 *         set to @c NULL.
 */
static PFmil_t *
leaf (PFmil_kind_t k)
{
    int i;
    PFmil_t *ret = PFmalloc (sizeof (PFmil_t));

    ret->kind = k;

    for (i = 0; i < MIL_MAXCHILD; i++)
        ret->child[i] = NULL;

    return ret;
}

/**
 * Construct a MIL tree node with given node kind and one child.
 *
 * @param k Kind of the newly constructed node.
 * @param n Child node to attach to the new node.
 * @return A MIL tree node type, with kind set to @a k, the first child
 *         set to @a n, and all remaining children set to @c NULL.
 */
static PFmil_t *
wire1 (PFmil_kind_t k, const PFmil_t *n)
{
    PFmil_t *ret = leaf (k);
    ret->child[0] = (PFmil_t *) n;
    return ret;
}

/**
 * Construct a MIL tree node with given node kind and two children.
 *
 * @param k Kind of the newly constructed node.
 * @param n1 First child node to attach to the new node.
 * @param n2 Second child node to attach to the new node.
 * @return A MIL tree node type, with kind set to @a k, the first children
 *         set to @a n1 and @a n2, and all remaining children set to @c NULL.
 */
static PFmil_t *
wire2 (PFmil_kind_t k, const PFmil_t *n1, const PFmil_t *n2)
{
    PFmil_t *ret = wire1 (k, n1);
    ret->child[1] = (PFmil_t *) n2;
    return ret;
}

/**
 * Construct a MIL tree node with given node kind and three children.
 *
 * @param k Kind of the newly constructed node.
 * @param n1 First child node to attach to the new node.
 * @param n2 Second child node to attach to the new node.
 * @param n3 Third child node to attach to the new node.
 * @return A MIL tree node type, with kind set to @a k, the first children
 *         set to @a n1, @a n2, and @a n3. All remaining children set
 *         to @c NULL.
 */
static PFmil_t *
wire3 (PFmil_kind_t k, const PFmil_t *n1, const PFmil_t *n2, const PFmil_t *n3)
{
    PFmil_t *ret = wire2 (k, n1, n2);
    ret->child[2] = (PFmil_t *) n3;
    return ret;
}

/**
 * Construct a MIL tree node with given node kind.
 */
static PFmil_t *
wire4 (PFmil_kind_t k, const PFmil_t *n1, const PFmil_t *n2, const PFmil_t *n3,
       const PFmil_t *n4)
{
    PFmil_t *ret = wire3 (k, n1, n2, n3);
    ret->child[3] = (PFmil_t *) n4;
    return ret;
}

/**
 * Construct a MIL tree node with given node kind.
 */
static PFmil_t *
wire5 (PFmil_kind_t k, const PFmil_t *n1, const PFmil_t *n2, const PFmil_t *n3,
       const PFmil_t *n4, const PFmil_t *n5)
{
    PFmil_t *ret = wire4 (k, n1, n2, n3, n4);
    ret->child[4] = (PFmil_t *) n5;
    return ret;
}

/**
 * Construct a MIL tree node with given node kind.
 */
static PFmil_t *
wire6 (PFmil_kind_t k, const PFmil_t *n1, const PFmil_t *n2, const PFmil_t *n3,
       const PFmil_t *n4, const PFmil_t *n5, const PFmil_t *n6)
{
    PFmil_t *ret = wire5 (k, n1, n2, n3, n4, n5);
    ret->child[5] = (PFmil_t *) n6;
    return ret;
}

/**
 * Construct a MIL tree node with given node kind.
 */
static PFmil_t *
wire7 (PFmil_kind_t k, const PFmil_t *n1, const PFmil_t *n2, const PFmil_t *n3,
       const PFmil_t *n4, const PFmil_t *n5, const PFmil_t *n6,
       const PFmil_t *n7)
{
    PFmil_t *ret = wire6 (k, n1, n2, n3, n4, n5, n6);
    ret->child[6] = (PFmil_t *) n7;
    return ret;
}

/**
 * Create a MIL tree node representing a literal integer.
 * (The result will be a MIL leaf node, with kind #m_lit_int and
 * semantic value @a i.)
 *
 * @param i The integer value to represent in MIL
 */
PFmil_t *
PFmil_lit_int (int i)
{
    PFmil_t *ret = leaf (m_lit_int);
    ret->sem.i = i;
    return ret;
}

/**
 * Create a MIL tree node representing a literal string.
 * (The result will be a MIL leaf node, with kind #m_lit_str and
 * semantic value @a s.)
 *
 * @param s The string value to represent in MIL
 */
PFmil_t *
PFmil_lit_str (const char *s)
{
    PFmil_t *ret = leaf (m_lit_str);

    assert (s);

    ret->sem.s = (char *) s;
    return ret;
}

/**
 * Create a MIL tree node representing a literal oid.
 * (The result will be a MIL leaf node, with kind #m_lit_oid and
 * semantic value @a o.)
 *
 * @param o The oid to represent in MIL
 */
PFmil_t *
PFmil_lit_oid (oid o)
{
    PFmil_t *ret = leaf (m_lit_oid);
    ret->sem.o = o;
    return ret;
}

/**
 * Create a MIL tree node representing a literal double.
 * (The result will be a MIL leaf node, with kind #m_lit_dbl and
 * semantic value @a d.)
 *
 * @param d The double value to represent in MIL
 */
PFmil_t *
PFmil_lit_dbl (double d)
{
    PFmil_t *ret = leaf (m_lit_dbl);
    ret->sem.d = d;
    return ret;
}

/**
 * Create a MIL tree node representing a literal boolean.
 * (The result will be a MIL leaf node, with kind #m_lit_bit and
 * semantic value @a b.)
 *
 * @param b The boolean value to represent in MIL
 */
PFmil_t *
PFmil_lit_bit (bool b)
{
    PFmil_t *ret = leaf (m_lit_bit);
    ret->sem.b = b;
    return ret;
}

/**
 * Create a MIL tree node representing a variable.
 * (The result will be a MIL leaf node, with kind #m_var and
 * semantic value @a name (in the @c ident field).)
 *
 * @param name Name of the variable.
 */
PFmil_t *
PFmil_var (const PFmil_ident_t name)
{
    PFmil_t *ret = leaf (m_var);
    ret->sem.ident = name;
    return ret;
}

/**
 * MIL `no operation'. Does nothing during processing, nothing is
 * even printed in milprint.c
 */
PFmil_t *
PFmil_nop (void)
{
    return leaf (m_nop);
}

/**
 * MIL keyword `nil'.
 */
PFmil_t *
PFmil_nil (void)
{
    return leaf (m_nil);
}

/**
 * Construct MIL type identifier
 */
PFmil_t *
PFmil_type (PFmil_type_t t)
{
    PFmil_t *ret = leaf (m_type);

    ret->sem.t = t;

    return ret;
}

/**
 * MIL if-then-else clause
 */
PFmil_t *
PFmil_if (const PFmil_t *cond, const PFmil_t *e1, const PFmil_t *e2)
{
    return wire3 (m_if, cond, e1, e2);
}

/**
 * Construct a combined variable declaration and its assignment.
 * (Declare variable @a v and assign result of @a e to it.)
 *
 * @param v The variable in the assignment. Must be of kind #m_var.
 * @param e Expression to assign to @a v.
 */
PFmil_t *
PFmil_assgn (const PFmil_t *v, const PFmil_t *e)
{
    /* left hand side of an assignment must be a variable */
    assert (v->kind == m_var);

    return wire2 (m_assgn, v, e);
}

/** MIL new() statement */
PFmil_t *
PFmil_new (const PFmil_t *head, const PFmil_t *tail)
{
    /* arguments must be types */
    assert (head->kind == m_type);
    assert (tail->kind == m_type);

    return wire2 (m_new, head, tail);
}

/**
 * A sequence of MIL statements
 *
 * @note
 *   Normally you should not need to invoke this function directly.
 *   Use the wrapper macro #PFmil_seq (or its mnemonic variant #seq)
 *   instead. It will automatically calculate @a count for you, so
 *   you will only have to pass a list of arguments to that (variable
 *   argument list) macro.
 *
 * @param count Number of MIL statements in the array that follows
 * @param stmts Array of exactly @a count MIL statement nodes.
 * @return A chain of sequence nodes (#m_seq), representing the
 *         sequence of all statements passed.
 */
PFmil_t *
PFmil_seq_ (int count, const PFmil_t **stmts)
{
    assert (count > 0);

    if (count == 1)
        return (PFmil_t *) stmts[0];
    else
        return wire2 (m_seq, stmts[0], PFmil_seq_ (count - 1, stmts + 1));
}

/**
 * Monet seqbase() function.
 */
PFmil_t *
PFmil_seqbase (const PFmil_t *bat, const PFmil_t *base)
{
    return wire2 (m_seqbase, bat, base);
}

PFmil_t *
PFmil_key (const PFmil_t *bat, bool key)
{
    PFmil_t *ret = wire1 (m_key, bat);

    ret->sem.b = key;

    return ret;
}

/**
 * Monet order() function.
 */
PFmil_t *
PFmil_order (const PFmil_t *bat)
{
    return wire1 (m_order, bat);
}

/**
 * Monet select() function.
 */
PFmil_t *
PFmil_select (const PFmil_t *bat, const PFmil_t *value)
{
    return wire2 (m_select, bat, value);
}

/**
 * Monet select() function.
 */
PFmil_t *
PFmil_select2 (const PFmil_t *bat, const PFmil_t *v1, const PFmil_t *v2)
{
    return wire3 (m_select2, bat, v1, v2);
}

/**
 * Monet uselect() function.
 */
PFmil_t *
PFmil_uselect (const PFmil_t *bat, const PFmil_t *value)
{
    return wire2 (m_uselect, bat, value);
}

/**
 * Monet insert() function to insert a single BUN (3 arguments).
 */
PFmil_t *
PFmil_insert (const PFmil_t *bat, const PFmil_t *head, const PFmil_t *tail)
{
    return wire3 (m_insert, bat, head, tail);
}

/**
 * Monet insert() function to insert a whole BAT at once (2 arguments).
 */
PFmil_t *
PFmil_binsert (const PFmil_t *dest, const PFmil_t *src)
{
    return wire2 (m_binsert, dest, src);
}

/**
 * Monet append() function to append one BAT[void,any] to another.
 *
 * Example:
 *
 *  void | int            void | int       void | int
 * ------+-----  append  ------+-----  =  ------+-----
 *   0@0 |  10             0@0 |  20        0@0 |  10
 *   1@0 |  11             1@0 |  21        1@0 |  11
 *                                          2@0 |  20
 *                                          3@0 |  21
 */
PFmil_t *
PFmil_bappend (const PFmil_t *dest, const PFmil_t *src)
{
    return wire2 (m_bappend, dest, src);
}

/**
 * Monet project() function.
 */
PFmil_t *
PFmil_project (const PFmil_t *bat, const PFmil_t *value)
{
    return wire2 (m_project, bat, value);
}

/**
 * Monet mark() function.
 */
PFmil_t *
PFmil_mark (const PFmil_t *bat, const PFmil_t *value)
{
    return wire2 (m_mark, bat, value);
}

/**
 * Monet mark_grp() function.
 */
PFmil_t *
PFmil_mark_grp (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mark_grp, a, b);
}

/**
 * Monet fetch() function.
 */
PFmil_t *
PFmil_fetch (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_fetch, a, b);
}

/**
 * Set access restrictions on a BAT
 */
PFmil_t *
PFmil_access (const PFmil_t *bat, PFmil_access_t restriction)
{
    PFmil_t *ret = wire1 (m_access, bat);

    ret->sem.access = restriction;

    return ret;
}

/**
 * Monet cross product operator
 */
PFmil_t *
PFmil_cross (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_cross, a, b);
}

/**
 * Monet join operator
 */
PFmil_t *
PFmil_join (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_join, a, b);
}

/**
 * Monet join operator
 */
PFmil_t *
PFmil_leftjoin (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_leftjoin, a, b);
}

/**
 * Monet reverse operator, swap head/tail
 */
PFmil_t *
PFmil_reverse (const PFmil_t *a)
{
    return wire1 (m_reverse, a);
}

/**
 * Monet mirror operator, mirror head into tail
 */
PFmil_t *
PFmil_mirror (const PFmil_t *a)
{
    return wire1 (m_mirror, a);
}

/**
 * Monet kunique operator
 */
PFmil_t *
PFmil_kunique (const PFmil_t *a)
{
    return wire1 (m_kunique, a);
}

/**
 * Monet kunion operator
 */
PFmil_t *
PFmil_kunion (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_kunion, a, b);
}

/**
 * Monet kdiff operator
 */
PFmil_t *
PFmil_kdiff (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_kdiff, a, b);
}

/**
 * Monet merged_union operator
 */
PFmil_t *
PFmil_merged_union (const PFmil_t *a)
{
    return wire1 (m_merged_union, a);
}

/**
 * build argument lists for variable argument list functions
 */
PFmil_t *
PFmil_arg (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_arg, a, b);
}

/**
 * Monet copy operator, returns physical copy of a BAT.
 */
PFmil_t *
PFmil_copy (const PFmil_t *a)
{
    return wire1 (m_copy, a);
}

/**
 * Monet sort function, sorts a BAT by its head value.
 */
PFmil_t *
PFmil_sort (const PFmil_t *a)
{
    return wire1 (m_sort, a);
}

/**
 * Monet CTgroup function.
 */
PFmil_t *
PFmil_ctgroup (const PFmil_t *a)
{
    return wire1 (m_ctgroup, a);
}

/**
 * Monet CTmap function.
 */
PFmil_t *
PFmil_ctmap (const PFmil_t *a)
{
    return wire1 (m_ctmap, a);
}

/**
 * Monet CTextend function.
 */
PFmil_t *
PFmil_ctextend (const PFmil_t *a)
{
    return wire1 (m_ctextend, a);
}

/**
 * Monet ctrefine function.
 */
PFmil_t *
PFmil_ctrefine (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_ctrefine, a, b);
}

/**
 * Monet CTderive function.
 */
PFmil_t *
PFmil_ctderive (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_ctderive, a, b);
}

/**
 * Monet max operator, return maximum tail value
 */
PFmil_t *
PFmil_max (const PFmil_t *a)
{
    return wire1 (m_max, a);
}

/**
 * Monet count operator, return number of items in @a a.
 */
PFmil_t *
PFmil_count (const PFmil_t *a)
{
    return wire1 (m_count, a);
}

/**
 * Monet grouped count operator `{count}()'
 */
PFmil_t *
PFmil_gcount (const PFmil_t *a)
{
    return wire1 (m_gcount, a);
}

/**
 * Type cast.
 */
PFmil_t *
PFmil_cast (const PFmil_t *type, const PFmil_t *e)
{
    assert (type); assert (e); assert (type->kind == m_type);

    return wire2 (m_cast, type, e);
}

/**
 * Multiplexed type cast.
 */
PFmil_t *
PFmil_mcast (const PFmil_t *type, const PFmil_t *e)
{
    assert (type); assert (e); assert (type->kind == m_type);

    return wire2 (m_mcast, type, e);
}

/**
 * Arithmetic add operator
 */
PFmil_t *
PFmil_add (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_add, a, b);
}

/**
 * Multiplexed arithmetic plus operator
 */
PFmil_t *
PFmil_madd (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_madd, a, b);
}

/**
 * Multiplexed arithmetic subtract operator
 */
PFmil_t *
PFmil_msub (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_msub, a, b);
}

/**
 * Multiplexed arithmetic multiply operator
 */
PFmil_t *
PFmil_mmult (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mmult, a, b);
}

/**
 * Multiplexed arithmetic divide operator
 */
PFmil_t *
PFmil_mdiv (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mdiv, a, b);
}

/**
 * Multiplexed arithmetic modulo operator
 */
PFmil_t *
PFmil_mmod (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mmod, a, b);
}

/**
 * Multiplexed comparison operator (greater than)
 */
PFmil_t *
PFmil_mgt (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mgt, a, b);
}

/**
 * Multiplexed comparison operator (equality)
 */
PFmil_t *
PFmil_meq (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_meq, a, b);
}

/**
 * Multiplexed boolean negation
 */
PFmil_t *
PFmil_mnot (const PFmil_t *a)
{
    return wire1 (m_mnot, a);
}

/**
 * Multiplexed numeric negation
 */
PFmil_t *
PFmil_mneg (const PFmil_t *a)
{
    return wire1 (m_mneg, a);
}

/**
 * Multiplexed boolean operator `and'
 */
PFmil_t *
PFmil_mand (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mand, a, b);
}

/**
 * Multiplexed boolean operator `or'
 */
PFmil_t *
PFmil_mor (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mor, a, b);
}

/** Multiplexed isnil() operator `[isnil]()' */
PFmil_t *
PFmil_misnil (const PFmil_t *a)
{
    return wire1 (m_misnil, a);
}

/** Multiplexed ifthenelse() operator `[ifthenelse]()' */
PFmil_t *
PFmil_mifthenelse (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c)
{
    return wire3 (m_mifthenelse, a, b, c);
}

PFmil_t *
PFmil_bat (const PFmil_t *a)
{
    return wire1 (m_bat, a);
}

/**
 * Create a new working set
 */
PFmil_t *
PFmil_new_ws (void)
{
    return leaf (m_new_ws);
}

/**
 * Positional multijoin with a working set
 */
PFmil_t *
PFmil_mposjoin (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c)
{
    return wire3 (m_mposjoin, a, b, c);
}

/**
 * Multijoin with a working set
 */
PFmil_t *
PFmil_mvaljoin (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c)
{
    return wire3 (m_mvaljoin, a, b, c);
}

/* ---------- staircase join variants ---------- */

/* ---- ancestor axis ---- */

/** ancestor axis without node test (.../ancestor::node()) */
PFmil_t * PFmil_llscj_anc (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_anc, iter, item, frag, ws, ord);
}

/** ancestor axis with node test element() (.../ancestor::element()) */
PFmil_t * PFmil_llscj_anc_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_elem, iter, item, frag, ws, ord);
}

/** ancestor axis with node test text() (.../ancestor::text()) */
PFmil_t * PFmil_llscj_anc_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_text, iter, item, frag, ws, ord);
}

/** ancestor axis with node test comment() (.../ancestor::comment()) */
PFmil_t * PFmil_llscj_anc_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_comm, iter, item, frag, ws, ord);
}

/** ancestor axis with node test proc-instr() (.../ancestor::proc-instr()) */
PFmil_t * PFmil_llscj_anc_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_pi, iter, item, frag, ws, ord);
}

/** ancestor axis with full QName (.../ancestor::ns:loc) */
PFmil_t * PFmil_llscj_anc_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_anc_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** ancestor axis with only local name (.../ancestor::*:local) */
PFmil_t * PFmil_llscj_anc_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_anc_elem_loc, iter, item, frag, ws, ord, loc);
}

/** ancestor axis with only ns test (.../ancestor::ns:*) */
PFmil_t * PFmil_llscj_anc_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_anc_elem_ns, iter, item, frag, ws, ord, ns);
}

/** ancestor axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_anc_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_anc_pi_targ, iter, item, frag, ws, ord, target);
}

/* ---- ancestor-or-self axis ---- */

/** ancestor-or-self axis without node test (.../ancestor-or-self::node()) */
PFmil_t * PFmil_llscj_anc_self (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_self, iter, item, frag, ws, ord);
}

/** ancestor-or-self axis with node test element() (.../ancestor-or-self::element()) */
PFmil_t * PFmil_llscj_anc_self_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_self_elem, iter, item, frag, ws, ord);
}

/** ancestor-or-self axis with node test text() (.../ancestor-or-self::text()) */
PFmil_t * PFmil_llscj_anc_self_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_self_text, iter, item, frag, ws, ord);
}

/** ancestor-or-self axis with node test comment() (.../ancestor-or-self::comment()) */
PFmil_t * PFmil_llscj_anc_self_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_self_comm, iter, item, frag, ws, ord);
}

/** ancestor-or-self axis with node test proc-instr() (.../ancestor-or-self::proc-instr()) */
PFmil_t * PFmil_llscj_anc_self_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_anc_self_pi, iter, item, frag, ws, ord);
}

/** ancestor-or-self axis with full QName (.../ancestor-or-self::ns:loc) */
PFmil_t * PFmil_llscj_anc_self_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_anc_self_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** ancestor-or-self axis with only local name (.../ancestor-or-self::*:local) */
PFmil_t * PFmil_llscj_anc_self_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_anc_self_elem_loc, iter, item, frag, ws, ord, loc);
}

/** ancestor-or-self axis with only ns test (.../ancestor-or-self::ns:*) */
PFmil_t * PFmil_llscj_anc_self_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_anc_self_elem_ns, iter, item, frag, ws, ord, ns);
}

/** ancestor-or-self axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_anc_self_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_anc_self_pi_targ, iter, item, frag, ws, ord, target);
}



/* ---- child axis ---- */

/** child axis without node test (.../child::node()) */
PFmil_t * PFmil_llscj_child (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_child, iter, item, frag, ws, ord);
}

/** child axis with node test element() (.../child::element()) */
PFmil_t * PFmil_llscj_child_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_child_elem, iter, item, frag, ws, ord);
}

/** child axis with node test text() (.../child::text()) */
PFmil_t * PFmil_llscj_child_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_child_text, iter, item, frag, ws, ord);
}

/** child axis with node test comment() (.../child::comment()) */
PFmil_t * PFmil_llscj_child_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_child_comm, iter, item, frag, ws, ord);
}

/** child axis with node test proc-instr() (.../child::proc-instr()) */
PFmil_t * PFmil_llscj_child_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_child_pi, iter, item, frag, ws, ord);
}

/** child axis with full QName (.../child::ns:loc) */
PFmil_t * PFmil_llscj_child_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_child_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** child axis with only local name (.../child::*:local) */
PFmil_t * PFmil_llscj_child_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_child_elem_loc, iter, item, frag, ws, ord, loc);
}

/** child axis with only ns test (.../child::ns:*) */
PFmil_t * PFmil_llscj_child_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_child_elem_ns, iter, item, frag, ws, ord, ns);
}

/** child axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_child_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_child_pi_targ, iter, item, frag, ws, ord, target);
}


/* ---- descendant axis ---- */

/** descendant axis without node test (.../descendant::node()) */
PFmil_t * PFmil_llscj_desc (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_desc, iter, item, frag, ws, ord);
}

/** descendant axis with node test element() (.../descendant::element()) */
PFmil_t * PFmil_llscj_desc_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_elem, iter, item, frag, ws, ord);
}

/** descendant axis with node test text() (.../descendant::text()) */
PFmil_t * PFmil_llscj_desc_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_text, iter, item, frag, ws, ord);
}

/** descendant axis with node test comment() (.../descendant::comment()) */
PFmil_t * PFmil_llscj_desc_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_comm, iter, item, frag, ws, ord);
}

/** descendant axis with node test proc-instr() (.../descendant::proc-instr()) */
PFmil_t * PFmil_llscj_desc_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_pi, iter, item, frag, ws, ord);
}

/** descendant axis with full QName (.../descendant::ns:loc) */
PFmil_t * PFmil_llscj_desc_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_desc_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** descendant axis with only local name (.../descendant::*:local) */
PFmil_t * PFmil_llscj_desc_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_desc_elem_loc, iter, item, frag, ws, ord, loc);
}

/** descendant axis with only ns test (.../descendant::ns:*) */
PFmil_t * PFmil_llscj_desc_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_desc_elem_ns, iter, item, frag, ws, ord, ns);
}

/** descendant axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_desc_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_desc_pi_targ, iter, item, frag, ws, ord, target);
}

/* ---- descendant-or-self axis ---- */

/** descendant-or-self axis without node test (.../descendant-or-self::node()) */
PFmil_t * PFmil_llscj_desc_self (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_self, iter, item, frag, ws, ord);
}

/** descendant-or-self axis with node test element() (.../descendant-or-self::element()) */
PFmil_t * PFmil_llscj_desc_self_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_self_elem, iter, item, frag, ws, ord);
}

/** descendant-or-self axis with node test text() (.../descendant-or-self::text()) */
PFmil_t * PFmil_llscj_desc_self_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_self_text, iter, item, frag, ws, ord);
}

/** descendant-or-self axis with node test comment() (.../descendant-or-self::comment()) */
PFmil_t * PFmil_llscj_desc_self_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_self_comm, iter, item, frag, ws, ord);
}

/** descendant-or-self axis with node test proc-instr() (.../descendant-or-self::proc-instr()) */
PFmil_t * PFmil_llscj_desc_self_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_desc_self_pi, iter, item, frag, ws, ord);
}

/** descendant-or-self axis with full QName (.../descendant-or-self::ns:loc) */
PFmil_t * PFmil_llscj_desc_self_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_desc_self_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** descendant-or-self axis with only local name (.../descendant-or-self::*:local) */
PFmil_t * PFmil_llscj_desc_self_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_desc_self_elem_loc, iter, item, frag, ws, ord, loc);
}

/** descendant-or-self axis with only ns test (.../descendant-or-self::ns:*) */
PFmil_t * PFmil_llscj_desc_self_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_desc_self_elem_ns, iter, item, frag, ws, ord, ns);
}

/** descendant-or-self axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_desc_self_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_desc_self_pi_targ, iter, item, frag, ws, ord, target);
}


/* ---- following axis ---- */

/** following axis without node test (.../following::node()) */
PFmil_t * PFmil_llscj_foll (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_foll, iter, item, frag, ws, ord);
}

/** following axis with node test element() (.../following::element()) */
PFmil_t * PFmil_llscj_foll_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_elem, iter, item, frag, ws, ord);
}

/** following axis with node test text() (.../following::text()) */
PFmil_t * PFmil_llscj_foll_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_text, iter, item, frag, ws, ord);
}

/** following axis with node test comment() (.../following::comment()) */
PFmil_t * PFmil_llscj_foll_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_comm, iter, item, frag, ws, ord);
}

/** following axis with node test proc-instr() (.../following::proc-instr()) */
PFmil_t * PFmil_llscj_foll_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_pi, iter, item, frag, ws, ord);
}

/** following axis with full QName (.../following::ns:loc) */
PFmil_t * PFmil_llscj_foll_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_foll_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** following axis with only local name (.../following::*:local) */
PFmil_t * PFmil_llscj_foll_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_foll_elem_loc, iter, item, frag, ws, ord, loc);
}

/** following axis with only ns test (.../following::ns:*) */
PFmil_t * PFmil_llscj_foll_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_foll_elem_ns, iter, item, frag, ws, ord, ns);
}

/** following axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_foll_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_foll_pi_targ, iter, item, frag, ws, ord, target);
}


/* ---- following-sibling axis ---- */

/** following-sibling axis without node test (.../following-sibling::node()) */
PFmil_t * PFmil_llscj_foll_sibl (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_sibl, iter, item, frag, ws, ord);
}

/** following-sibling axis with node test element() (.../following-sibling::element()) */
PFmil_t * PFmil_llscj_foll_sibl_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_sibl_elem, iter, item, frag, ws, ord);
}

/** following-sibling axis with node test text() (.../following-sibling::text()) */
PFmil_t * PFmil_llscj_foll_sibl_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_sibl_text, iter, item, frag, ws, ord);
}

/** following-sibling axis with node test comment() (.../following-sibling::comment()) */
PFmil_t * PFmil_llscj_foll_sibl_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_sibl_comm, iter, item, frag, ws, ord);
}

/** following-sibling axis with node test proc-instr() (.../following-sibling::proc-instr()) */
PFmil_t * PFmil_llscj_foll_sibl_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_foll_sibl_pi, iter, item, frag, ws, ord);
}

/** following-sibling axis with full QName (.../following-sibling::ns:loc) */
PFmil_t * PFmil_llscj_foll_sibl_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_foll_sibl_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** following-sibling axis with only local name (.../following-sibling::*:local) */
PFmil_t * PFmil_llscj_foll_sibl_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_foll_sibl_elem_loc, iter, item, frag, ws, ord, loc);
}

/** following-sibling axis with only ns test (.../following-sibling::ns:*) */
PFmil_t * PFmil_llscj_foll_sibl_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_foll_sibl_elem_ns, iter, item, frag, ws, ord, ns);
}

/** following-sibling axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_foll_sibl_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_foll_sibl_pi_targ, iter, item, frag, ws, ord, target);
}


/* ---- parent axis ---- */

/** parent axis without node test (.../parent::node()) */
PFmil_t * PFmil_llscj_parent (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_parent, iter, item, frag, ws, ord);
}

/** parent axis with node test element() (.../parent::element()) */
PFmil_t * PFmil_llscj_parent_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_parent_elem, iter, item, frag, ws, ord);
}

/** parent axis with node test text() (.../parent::text()) */
PFmil_t * PFmil_llscj_parent_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_parent_text, iter, item, frag, ws, ord);
}

/** parent axis with node test comment() (.../parent::comment()) */
PFmil_t * PFmil_llscj_parent_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_parent_comm, iter, item, frag, ws, ord);
}

/** parent axis with node test proc-instr() (.../parent::proc-instr()) */
PFmil_t * PFmil_llscj_parent_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_parent_pi, iter, item, frag, ws, ord);
}

/** parent axis with full QName (.../parent::ns:loc) */
PFmil_t * PFmil_llscj_parent_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_parent_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** parent axis with only local name (.../parent::*:local) */
PFmil_t * PFmil_llscj_parent_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_parent_elem_loc, iter, item, frag, ws, ord, loc);
}

/** parent axis with only ns test (.../parent::ns:*) */
PFmil_t * PFmil_llscj_parent_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_parent_elem_ns, iter, item, frag, ws, ord, ns);
}

/** parent axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_parent_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_parent_pi_targ, iter, item, frag, ws, ord, target);
}


/* ---- preceding axis ---- */

/** preceding axis without node test (.../preceding::node()) */
PFmil_t * PFmil_llscj_prec (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_prec, iter, item, frag, ws, ord);
}

/** preceding axis with node test element() (.../preceding::element()) */
PFmil_t * PFmil_llscj_prec_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_elem, iter, item, frag, ws, ord);
}

/** preceding axis with node test text() (.../preceding::text()) */
PFmil_t * PFmil_llscj_prec_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_text, iter, item, frag, ws, ord);
}

/** preceding axis with node test comment() (.../preceding::comment()) */
PFmil_t * PFmil_llscj_prec_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_comm, iter, item, frag, ws, ord);
}

/** preceding axis with node test proc-instr() (.../preceding::proc-instr()) */
PFmil_t * PFmil_llscj_prec_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_pi, iter, item, frag, ws, ord);
}

/** preceding axis with full QName (.../preceding::ns:loc) */
PFmil_t * PFmil_llscj_prec_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_prec_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** preceding axis with only local name (.../preceding::*:local) */
PFmil_t * PFmil_llscj_prec_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_prec_elem_loc, iter, item, frag, ws, ord, loc);
}

/** preceding axis with only ns test (.../preceding::ns:*) */
PFmil_t * PFmil_llscj_prec_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_prec_elem_ns, iter, item, frag, ws, ord, ns);
}

/** preceding axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_prec_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_prec_pi_targ, iter, item, frag, ws, ord, target);
}


/* ---- preceding-sibling axis ---- */

/** preceding-sibling axis without node test (.../preceding-sibling::node()) */
PFmil_t * PFmil_llscj_prec_sibl (const PFmil_t *iter, const PFmil_t *item,
                             const PFmil_t *frag, const PFmil_t *ws,
                             const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_sibl, iter, item, frag, ws, ord);
}

/** preceding-sibling axis with node test element() (.../preceding-sibling::element()) */
PFmil_t * PFmil_llscj_prec_sibl_elem (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_sibl_elem, iter, item, frag, ws, ord);
}

/** preceding-sibling axis with node test text() (.../preceding-sibling::text()) */
PFmil_t * PFmil_llscj_prec_sibl_text (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_sibl_text, iter, item, frag, ws, ord);
}

/** preceding-sibling axis with node test comment() (.../preceding-sibling::comment()) */
PFmil_t * PFmil_llscj_prec_sibl_comm (const PFmil_t *iter, const PFmil_t *item,
                                  const PFmil_t *frag, const PFmil_t *ws,
                                  const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_sibl_comm, iter, item, frag, ws, ord);
}

/** preceding-sibling axis with node test proc-instr() (.../preceding-sibling::proc-instr()) */
PFmil_t * PFmil_llscj_prec_sibl_pi (const PFmil_t *iter, const PFmil_t *item,
                                const PFmil_t *frag, const PFmil_t *ws,
                                const PFmil_t *ord)
{
    return wire5 (m_llscj_prec_sibl_pi, iter, item, frag, ws, ord);
}

/** preceding-sibling axis with full QName (.../preceding-sibling::ns:loc) */
PFmil_t * PFmil_llscj_prec_sibl_elem_nsloc (const PFmil_t *iter,const PFmil_t *item,
                                        const PFmil_t *frag,
                                        const PFmil_t *ws,
                                        const PFmil_t *ord,
                                        const PFmil_t *ns, const PFmil_t *loc)
{
    return wire7 (m_llscj_prec_sibl_elem_nsloc, iter, item, frag, ws, ord, ns, loc);
}

/** preceding-sibling axis with only local name (.../preceding-sibling::*:local) */
PFmil_t * PFmil_llscj_prec_sibl_elem_loc (const PFmil_t *iter, const PFmil_t *item,
                                      const PFmil_t *frag, const PFmil_t *ws,
                                      const PFmil_t *ord,
                                      const PFmil_t *loc)
{
    return wire6 (m_llscj_prec_sibl_elem_loc, iter, item, frag, ws, ord, loc);
}

/** preceding-sibling axis with only ns test (.../preceding-sibling::ns:*) */
PFmil_t * PFmil_llscj_prec_sibl_elem_ns (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *ns)
{
    return wire6 (m_llscj_prec_sibl_elem_ns, iter, item, frag, ws, ord, ns);
}

/** preceding-sibling axis on pis with target (.../processing-instruction("target")) */
PFmil_t * PFmil_llscj_prec_sibl_pi_targ (const PFmil_t *iter, const PFmil_t *item,
                                     const PFmil_t *frag, const PFmil_t *ws,
                                     const PFmil_t *ord,
                                     const PFmil_t *target)
{
    return wire6 (m_llscj_prec_sibl_pi_targ, iter, item, frag, ws, ord, target);
}





PFmil_t * PFmil_string_join (const PFmil_t *strs, const PFmil_t *sep)
{
    return wire2 (m_string_join, strs, sep);
}

PFmil_t *
PFmil_chk_order (const PFmil_t *v)
{
    return wire1 (m_chk_order, v);
}

PFmil_t *
PFmil_is_fake_project (const PFmil_t *v)
{
    return wire1 (m_is_fake_project, v);
}

PFmil_t *
PFmil_get_fragment (const PFmil_t *v)
{
    return wire1 (m_get_fragment, v);
}

PFmil_t *
PFmil_set_kind (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_set_kind, a, b);
}

PFmil_t *
PFmil_sc_desc (const PFmil_t *ws, const PFmil_t *iter,
               const PFmil_t *item, const PFmil_t *live)
{
    return wire4 (m_sc_desc, ws, iter, item, live);
}

PFmil_t *
PFmil_doc_tbl (const PFmil_t *ws, const PFmil_t *item)
{
    return wire2 (m_doc_tbl, ws, item);
}


PFmil_t *
PFmil_declare (const PFmil_t *v)
{
    assert (v->kind == m_var);

    return wire1 (m_declare, v);
}

PFmil_t *
PFmil_print (const PFmil_t *args)
{
    return wire1 (m_print, args);
}

PFmil_t *
PFmil_col_name (const PFmil_t *bat, const PFmil_t *name)
{
    return wire2 (m_col_name, bat, name);
}

PFmil_t *
PFmil_ser (const PFmil_t *args)
{
    return wire1 (m_serialize, args);
}

/* vim:set shiftwidth=4 expandtab: */
