/**
 * @file
 *
 * Constructor functions to handle MIL tree
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

/**
 * Construct a MIL variable assignment (Assign result of expression
 * @a e to variable @a v).
 *
 * @param v The variable in the assignment. Must be of kind #m_var.
 * @param e Expression to assign to @a v.
 */
PFmil_t *
PFmil_reassgn (const PFmil_t *v, const PFmil_t *e)
{
    /* left hand side of an assignment must be a variable */
    assert (v->kind == m_var);

    return wire2 (m_reassgn, v, e);
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
 * Monet ctrefine function.
 */
PFmil_t *
PFmil_ctrefine (const PFmil_t *a, const PFmil_t *b)
{
    return wire2 (m_ctrefine, a, b);
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

PFmil_t *
PFmil_ser (const char *prefix,
           const bool has_nat_part, const bool has_int_part,
           const bool has_str_part, const bool has_node_part,
           const bool has_dec_part, const bool has_dbl_part,
           const bool has_bln_part)
{
    PFmil_t *ret = leaf (m_serialize);

    ret->sem.ser.prefix = (char *) prefix;
    ret->sem.ser.has_nat_part  = has_nat_part;
    ret->sem.ser.has_int_part  = has_int_part;
    ret->sem.ser.has_str_part  = has_str_part;
    ret->sem.ser.has_node_part = has_node_part;
    ret->sem.ser.has_dec_part  = has_dec_part;
    ret->sem.ser.has_dbl_part  = has_dbl_part;
    ret->sem.ser.has_bln_part  = has_bln_part;

    return ret;
}
