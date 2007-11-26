/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

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
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>

#include "mil.h"

#include "mem.h"

#include "array.h"
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
 * Construct a MIL tree node with given node kind.
 */
static PFmil_t *
wire8 (PFmil_kind_t k, const PFmil_t *n1, const PFmil_t *n2, const PFmil_t *n3,
       const PFmil_t *n4, const PFmil_t *n5, const PFmil_t *n6,
       const PFmil_t *n7, const PFmil_t *n8)
{
    PFmil_t *ret = wire7 (k, n1, n2, n3, n4, n5, n6, n7);
    ret->child[7] = (PFmil_t *) n8;
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
 * Create a MIL tree node representing a literal 64bit integer.
 * (The result will be a MIL leaf node, with kind #m_lit_lng and
 * semantic value @a i.)
 *
 * @param i The integer value to represent in MIL
 */
PFmil_t *
PFmil_lit_lng (long long int l)
{
    PFmil_t *ret = leaf (m_lit_lng);
    ret->sem.l = l;
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

/** return the variable name as string */
char * PFmil_var_str (PFmil_ident_t name) {
    switch (name) {
        case PF_MIL_VAR_UNUSED:      return "unused";
        case PF_MIL_VAR_WS:          return "ws";
        case PF_MIL_VAR_WS_CONT:     return "WS";

        case PF_MIL_VAR_KIND_DOC:    return "DOCUMENT";
        case PF_MIL_VAR_KIND_ELEM:   return "ELEMENT";
        case PF_MIL_VAR_KIND_TEXT:   return "TEXT";
        case PF_MIL_VAR_KIND_COM:    return "COMMENT";
        case PF_MIL_VAR_KIND_PI:     return "PI";

        case PF_MIL_VAR_ATTR:        return "ATTR";
        case PF_MIL_VAR_ELEM:        return "ELEM";

        case PF_MIL_VAR_STR:         return "STR";
        case PF_MIL_VAR_INT:         return "INT";
        case PF_MIL_VAR_DBL:         return "DBL";
        case PF_MIL_VAR_DEC:         return "DEC";
        case PF_MIL_VAR_BOOL:        return "BOOL";

        case PF_MIL_VAR_PRE_SIZE:    return "PRE_SIZE";
        case PF_MIL_VAR_PRE_LEVEL:   return "PRE_LEVEL";
        case PF_MIL_VAR_PRE_KIND:    return "PRE_KIND";
        case PF_MIL_VAR_PRE_PROP:    return "PRE_PROP";
        case PF_MIL_VAR_PRE_CONT:    return "PRE_CONT";
        case PF_MIL_VAR_PRE_NID:     return "PRE_NID";
        case PF_MIL_VAR_NID_RID:     return "NID_RID";
        case PF_MIL_VAR_FRAG_ROOT:   return "FRAG_ROOT";
        case PF_MIL_VAR_ATTR_OWN:    return "ATTR_OWN";
        case PF_MIL_VAR_ATTR_QN:     return "ATTR_QN";
        case PF_MIL_VAR_ATTR_PROP:   return "ATTR_PROP";
        case PF_MIL_VAR_ATTR_CONT:   return "ATTR_CONT";
        case PF_MIL_VAR_QN_LOC:      return "QN_LOC";
        case PF_MIL_VAR_QN_URI:      return "QN_URI";
        case PF_MIL_VAR_PROP_VAL:    return "PROP_VAL";
        case PF_MIL_VAR_PROP_TEXT:   return "PROP_TEXT";
        case PF_MIL_VAR_PROP_COM:    return "PROP_COM";
        case PF_MIL_VAR_PROP_INS:    return "PROP_INS";
        case PF_MIL_VAR_PROP_TGT:    return "PROP_TGT";

        case PF_MIL_VAR_LE:          return "LE";
        case PF_MIL_VAR_LT:          return "LT";
        case PF_MIL_VAR_EQ:          return "EQ";
        case PF_MIL_VAR_GT:          return "GT";
        case PF_MIL_VAR_GE:          return "GE";

        case PF_MIL_VAR_TRACE_OUTER: return "trace_outer";
        case PF_MIL_VAR_TRACE_INNER: return "trace_inner";
        case PF_MIL_VAR_TRACE_ITER:  return "trace_iter";
        case PF_MIL_VAR_TRACE_MSG:   return "trace_msg";
        case PF_MIL_VAR_TRACE_ITEM:  return "trace_item";
        case PF_MIL_VAR_TRACE_TYPE:  return "trace_type";
        case PF_MIL_VAR_TRACE_REL:   return "trace_rel";

        case PF_MIL_VAR_TIME_LOAD:   return "time_load";
        case PF_MIL_VAR_TIME_QUERY:  return "time_query";
        case PF_MIL_VAR_TIME_PRINT:  return "time_print";

        default:
        {
            assert (name >= PF_MIL_RES_VAR_COUNT);
            name -= PF_MIL_RES_VAR_COUNT;
            assert (name < 10000);
            size_t len = sizeof ("a0000");
            char *res = PFmalloc (len);
            snprintf (res, len, "a%04u", name);
            return res;
        }
    }
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
 * MIL while statement
 */
PFmil_t *
PFmil_while (const PFmil_t *cond, const PFmil_t *e)
{
    return wire2 (m_while, cond, e);
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

/**
 * Monet seqbase() function.
 */
PFmil_t *
PFmil_seqbase_lookup (const PFmil_t *bat)
{
    return wire1 (m_sseqbase, bat);
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
 * Monet exist() function.
 */
PFmil_t *
PFmil_exist (const PFmil_t *bat, const PFmil_t *value)
{
    return wire2 (m_exist, bat, value);
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
 * Monet thetajoin() operator
 */
PFmil_t *
PFmil_thetajoin (const PFmil_t *a, const PFmil_t *b,
                 const PFmil_t *comp, const PFmil_t *size)
{
    return wire4 (m_thetajoin, a, b, comp, size);
}

/**
 * Monet htordered_unique_thetajoin PROC
 */
PFmil_t * PFmil_unq2_thetajoin (const PFmil_t *a, const PFmil_t *b,
                                const PFmil_t *comp)
{
    return wire3 (m_unq2_tjoin, a, b, comp);
}

/**
 * MIL ll_htordered_unique_thetajoin PROC
 */
PFmil_t * PFmil_unq1_thetajoin (const PFmil_t *a, const PFmil_t *b,
                                const PFmil_t *c, const PFmil_t *d,
                                const PFmil_t *comp)
{
    return wire5 (m_unq1_tjoin, a, b, c, d, comp);
}

/**
 * MIL combine_node_info PROC
 */
PFmil_t * PFmil_zip_nodes (const PFmil_t *a, const PFmil_t *b,
                           const PFmil_t *c, const PFmil_t *d,
                           const PFmil_t *e, const PFmil_t *f)
{
    return wire6 (m_zip_nodes, a, b, c, d, e, f);
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
 * Monet tunique operator
 */
PFmil_t *
PFmil_tunique (const PFmil_t *a)
{
    return wire1 (m_tunique, a);
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
 * Monet kintersect operator
 */
PFmil_t *
PFmil_kintersect (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_kintersect, a, b);
}

/**
 * Monet sintersect operator
 */
PFmil_t *
PFmil_sintersect (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_sintersect, a, b);
}

/**
 * Multi column intersection
 * (MIL ds_link() proc from the mkey module)
 */
PFmil_t *
PFmil_mc_intersect (const PFmil_t *a)
{
    return wire1 (m_mc_intersect, a);
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
 * MIL multi_merged_union operator
 */
PFmil_t *
PFmil_multi_merged_union (const PFmil_t *a)
{
    return wire1 (m_multi_mu, a);
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
PFmil_sort (const PFmil_t *a, bool dir_desc)
{
    if (dir_desc)
        return wire1 (m_sort_rev, a);
    else
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
 * Monet CTrefine function.
 */
PFmil_t *
PFmil_ctrefine (const PFmil_t *a, const PFmil_t *b, bool dir_desc)
{
    if (dir_desc)
        return wire2 (m_ctrefine_rev, a, b);
    else
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
 * Monet grouped count operator `{count}()' with two parameters
 */
PFmil_t *
PFmil_egcount (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_egcount, a, b);
}

/**
 * MIL avg() function
 */
PFmil_t * PFmil_avg (const PFmil_t *a)
{
    return wire1 (m_avg, a);
}

/**
 * Grouped avg function `{avg}()' (aka. ``pumped avg'')
 */
PFmil_t * PFmil_gavg (const PFmil_t *a)
{
    return wire1 (m_gavg, a);
}

/**
 * MIL max() function
 */
PFmil_t * PFmil_max (const PFmil_t *a)
{
    return wire1 (m_max, a);
}

/**
 * Grouped max function `{max}()' (aka. ``pumped max'')
 */
PFmil_t * PFmil_gmax (const PFmil_t *a)
{
    return wire1 (m_gmax, a);
}

/**
 * MIL min() function
 */
PFmil_t * PFmil_min (const PFmil_t *a)
{
    return wire1 (m_min, a);
}

/**
 * Grouped min function `{min}()' (aka. ``pumped min'')
 */
PFmil_t * PFmil_gmin (const PFmil_t *a)
{
    return wire1 (m_gmin, a);
}

/**
 * Monet sum operator
 */
PFmil_t *
PFmil_sum (const PFmil_t *a)
{
    return wire1 (m_sum, a);
}

/**
 * Monet grouped sum operator `{sum}()'
 */
PFmil_t *
PFmil_gsum (const PFmil_t *a)
{
    return wire1 (m_gsum, a);
}

/**
 * Monet grouped sum operator `{sum}()' with two parameters
 */
PFmil_t *
PFmil_egsum (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_egsum, a, b);
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
 * Arithmetic subtract operator
 */
PFmil_t *
PFmil_sub (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_sub, a, b);
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
 * Arithmetic divide operator
 */
PFmil_t *
PFmil_div (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_div, a, b);
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
 * Multiplexed operator abs
 */
PFmil_t *
PFmil_mabs (const PFmil_t *a)
{
    return wire1 (m_mabs, a);
}

/**
 * Multiplexed operator ceil
 */
PFmil_t *
PFmil_mceil (const PFmil_t *a)
{
    return wire1 (m_mceiling, a);
}

/**
 * Multiplexed operator floor
 */
PFmil_t *
PFmil_mfloor (const PFmil_t *a)
{
    return wire1 (m_mfloor, a);
}

/**
 * Multiplexed operator round_up
 */
PFmil_t *
PFmil_mround_up (const PFmil_t *a)
{
    return wire1 (m_mround_up, a);
}

/**
 * Greater than operator
 */
PFmil_t *
PFmil_gt (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_gt, a, b);
}

/**
 * Equal operator
 */
PFmil_t *
PFmil_eq (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_eq, a, b);
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
 * Multiplexed comparison operator (greater than)
 */
PFmil_t *
PFmil_mgt (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mgt, a, b);
}

/**
 * Multiplexed comparison operator (greater equal)
 */
PFmil_t *
PFmil_mge (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mge, a, b);
}

/**
 * Multiplexed comparison operator (less than)
 */
PFmil_t *
PFmil_mlt (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mlt, a, b);
}

/**
 * Multiplexed comparison operator (less equal)
 */
PFmil_t *
PFmil_mle (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mle, a, b);
}

/**
 * Multiplexed comparison operator (inequality)
 */
PFmil_t *
PFmil_mne (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mne, a, b);
}

/**
 * Boolean negation
 */
PFmil_t *
PFmil_not (const PFmil_t *a)
{
    return wire1 (m_not, a);
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

/** Operator `isnil()' */
PFmil_t *
PFmil_isnil (const PFmil_t *a)
{
    return wire1 (m_isnil, a);
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

/** Multiplexed search() function `[search](a,b)' */
PFmil_t *
PFmil_msearch (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_msearch, a, b);
}

/** Multiplexed string() function `[string](a,b)' */
PFmil_t *
PFmil_mstring (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mstring, a, b);
}

/** Multiplexed string() function `[string](a,b,c)' */
PFmil_t *
PFmil_mstring2 (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c)
{
    return wire3 (m_mstring2, a, b, c);
}

/** Multiplexed startsWith() function `[startsWith](a,b)' */
PFmil_t *
PFmil_mstarts_with (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mstarts_with, a, b);
}

/** Multiplexed endsWith() function `[endsWith](a,b)' */
PFmil_t *
PFmil_mends_with (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mends_with, a, b);
}

/** Multiplexed length() function `[length](a)' */
PFmil_t *
PFmil_mlength (const PFmil_t *a)
{
    return wire1 (m_mlength, a);
}

/** Multiplexed toUpper() function `[toUpper](a)' */
PFmil_t *
PFmil_mtoUpper (const PFmil_t *a)
{
    return wire1 (m_mtoUpper, a);
}

/** Multiplexed toLower() function `[toLower](a)' */
PFmil_t *
PFmil_mtoLower (const PFmil_t *a)
{
    return wire1 (m_mtoLower, a);
}

/** Multiplexed normSpace() function `[normSpace](a)' */
PFmil_t *
PFmil_mnorm_space (const PFmil_t *a)
{
    return wire1 (m_mnorm_space, a);
}

/** Multiplexed pcre_match() function `[pcre_match](a,b)' */
PFmil_t *
PFmil_mpcre_match (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_mpcre_match, a, b);
}

/** Multiplexed pcre_match() function `[pcre_match](a,b,c)' */
PFmil_t *
PFmil_mpcre_match_flag (const PFmil_t *a, const PFmil_t *b, const PFmil_t *c)
{
    return wire3 (m_mpcre_match_flag, a, b, c);
}

/** Multiplexed pcre_replace() function `[pcre_replace](a,b,c,d)' */
PFmil_t *
PFmil_mpcre_replace (const PFmil_t *a, const PFmil_t *b,
                     const PFmil_t *c, const PFmil_t *d)
{
    return wire4 (m_mpcre_replace, a, b, c, d);
}

PFmil_t *
PFmil_bat (const PFmil_t *a)
{
    return wire1 (m_bat, a);
}

/**
 * get the time
 */
PFmil_t *
PFmil_usec (void)
{
    return leaf (m_usec);
}

PFmil_t *
PFmil_catch (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_catch, a, b);
}

PFmil_t *
PFmil_error (const PFmil_t *a)
{
    return wire1 (m_error, a);
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
 * Free an existing working set
 */
PFmil_t *
PFmil_destroy_ws (const PFmil_t *ws)
{
    return wire1 (m_destroy_ws, ws);
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




PFmil_t * PFmil_merge_adjacent (const PFmil_t *iter, const PFmil_t *pre,
                                const PFmil_t *pfrag, const PFmil_t *ws)
{
    return wire4 (m_merge_adjacent, iter, pre, pfrag, ws);
}

PFmil_t * PFmil_string_join (const PFmil_t *strs, const PFmil_t *sep)
{
    return wire2 (m_string_join, strs, sep);
}

PFmil_t *
PFmil_assert_order (const PFmil_t *v)
{
    return wire1 (m_assert_order, v);
}

PFmil_t *
PFmil_chk_order (const PFmil_t *v)
{
    return wire1 (m_chk_order, v);
}

PFmil_t *
PFmil_materialize (const PFmil_t *v1, const PFmil_t *v2)
{
    return wire2 (m_materialize, v1, v2);
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
PFmil_attribute (const PFmil_t *qn, const PFmil_t *str, const PFmil_t *ws)
{
    return wire3 (m_attr_constr, qn, str, ws);
}

PFmil_t * PFmil_element (const PFmil_t *qn_iter, const PFmil_t *qn_item,
                         const PFmil_t *iter,
                         const PFmil_t *pre, const PFmil_t *pfrag,
                         const PFmil_t *attr, const PFmil_t *afrag,
                         const PFmil_t *ws)
{
    return wire8 (m_elem_constr, qn_iter, qn_item,
                  iter, pre, pfrag, attr, afrag, ws);
}

PFmil_t *
PFmil_empty_element (const PFmil_t *qn, const PFmil_t *ws)
{
    return wire2 (m_elem_constr_e, qn, ws);
}

PFmil_t *
PFmil_textnode (const PFmil_t *item, const PFmil_t *ws)
{
    return wire2 (m_text_constr, item, ws);
}

PFmil_t *
PFmil_add_qname (const PFmil_t *prefix, const PFmil_t *uri,
                 const PFmil_t *local, const PFmil_t *ws)
{
    return wire4 (m_add_qname, prefix, uri, local, ws);
}

PFmil_t *
PFmil_add_qnames (const PFmil_t *prefix, const PFmil_t *uri,
                  const PFmil_t *local, const PFmil_t *ws)
{
    return wire4 (m_add_qnames, prefix, uri, local, ws);
}

PFmil_t *
PFmil_add_content (const PFmil_t *item_str, const PFmil_t *ws,
                   const PFmil_t *container)
{
    return wire3 (m_add_content, item_str, ws, container);
}

PFmil_t *
PFmil_check_qnames (const PFmil_t *str)
{
    return wire1 (m_chk_qnames, str);
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
PFmil_comment (const char *fmt, ...)
{
    PFmil_t *ret = leaf (m_comment);
    PFarray_t *a = PFarray(sizeof (char));
    va_list args;

    va_start (args, fmt);
    PFarray_vprintf (a, fmt, args);
    va_end (args);

    ret->sem.s = PFarray_at(a, 0);

    return ret;
}

PFmil_t *
PFmil_ser (const PFmil_t *args)
{
    return wire1 (m_serialize, args);
}

PFmil_t *
PFmil_trace (const PFmil_t *args)
{
    return wire1 (m_trace, args);
}

PFmil_t *
PFmil_module (const PFmil_t *module)
{
    return wire1 (m_module, module);
}

PFmil_t *
PFmil_upd (const PFmil_t *args)
{
    return wire1 (m_update_tape, args);
}

/* vim:set shiftwidth=4 expandtab: */
