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
wire1 (PFmil_kind_t k, PFmil_t *n)
{
    PFmil_t *ret = leaf (k);
    ret->child[0] = n;
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
wire2 (PFmil_kind_t k, PFmil_t *n1, PFmil_t *n2)
{
    PFmil_t *ret = wire1 (k, n1);
    ret->child[1] = n2;
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
wire3 (PFmil_kind_t k, PFmil_t *n1, PFmil_t *n2, PFmil_t *n3)
{
    PFmil_t *ret = wire2 (k, n1, n2);
    ret->child[2] = n3;
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
PFmil_lit_str (char *s)
{
    PFmil_t *ret = leaf (m_lit_str);
    ret->sem.s = s;
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
 * Create a MIL tree node representing a variable.
 * (The result will be a MIL leaf node, with kind #m_var and
 * semantic value @a name (in the @c ident field).)
 *
 * @param name Name of the variable.
 */
PFmil_t *
PFmil_var (PFmil_ident_t name)
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
 * Construct a MIL variable assignment (Assign result of expression
 * @a e to variable @a v).
 *
 * @param v The variable in the assignment. Must be of kind #m_var.
 * @param e Expression to assign to @a v.
 */
PFmil_t *
PFmil_assgn (PFmil_t *v, PFmil_t *e)
{
    /* left hand side of an assignment must be a variable */
    assert (v->kind == m_var);

    return wire2 (m_assgn, v, e);
}

/** MIL new() statement */
PFmil_t *
PFmil_new (PFmil_t *head, PFmil_t *tail)
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
PFmil_seq_ (int count, PFmil_t **stmts)
{
    assert (count > 0);

    if (count == 1)
        return stmts[0];
    else
        return wire2 (m_seq, stmts[0], PFmil_seq_ (count - 1, stmts + 1));
}

/**
 * Monet seqbase() function.
 */
PFmil_t *
PFmil_seqbase (PFmil_t *bat, PFmil_t *base)
{
    return wire2 (m_seqbase, bat, base);
}

/**
 * Monet insert() function to insert a single BUN (3 arguments).
 */
PFmil_t *
PFmil_insert (PFmil_t *bat, PFmil_t *head, PFmil_t *tail)
{
    return wire3 (m_insert, bat, head, tail);
}

/**
 * Monet insert() function to insert a whole BAT at once (2 arguments).
 */
PFmil_t *
PFmil_binsert (PFmil_t *dest, PFmil_t *src)
{
    return wire2 (m_insert, dest, src);
}

/**
 * Monet project() function.
 */
PFmil_t *
PFmil_project (PFmil_t *bat, PFmil_t *value)
{
    return wire2 (m_project, bat, value);
}

/**
 * Monet mark() function.
 */
PFmil_t *
PFmil_mark (PFmil_t *bat, PFmil_t *value)
{
    return wire2 (m_mark, bat, value);
}

/**
 * Set access restrictions on a BAT
 */
PFmil_t *
PFmil_access (PFmil_t *bat, PFmil_access_t restriction)
{
    PFmil_t *ret = wire1 (m_access, bat);

    ret->sem.access = restriction;

    return ret;
}

/**
 * Monet join operator
 */
PFmil_t *
PFmil_join (PFmil_t *a, PFmil_t *b)
{
    return wire2 (m_join, a, b);
}

/**
 * Monet reverse operator, swap head/tail
 */
PFmil_t *
PFmil_reverse (PFmil_t *a)
{
    return wire1 (m_reverse, a);
}

/**
 * Monet copy operator, returns physical copy of a BAT.
 */
PFmil_t *
PFmil_copy (PFmil_t *a)
{
    return wire1 (m_copy, a);
}

/**
 * Monet max operator, return maximum tail value
 */
PFmil_t *
PFmil_max (PFmil_t *a)
{
    return wire1 (m_max, a);
}

/**
 * Type cast.
 */
PFmil_t *
PFmil_cast (PFmil_t *type, PFmil_t *e)
{
    assert (type); assert (e); assert (type->kind == m_type);

    return wire2 (m_cast, type, e);
}

/**
 * Multiplexed type cast.
 */
PFmil_t *
PFmil_mcast (PFmil_t *type, PFmil_t *e)
{
    assert (type); assert (e); assert (type->kind == m_type);

    return wire2 (m_mcast, type, e);
}

/**
 * Arithmetic plus operator
 */
PFmil_t *
PFmil_plus (PFmil_t *a, PFmil_t *b)
{
    return wire2 (m_plus, a, b);
}

/**
 * Multiplexed arithmetic plus operator
 */
PFmil_t *
PFmil_mplus (PFmil_t *a, PFmil_t *b)
{
    return wire2 (m_mplus, a, b);
}
