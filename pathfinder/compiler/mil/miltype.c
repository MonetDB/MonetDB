/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Determine MIL implementation type for all core tree nodes.
 *
 * $Id$
 */

#include "miltype.h"
#include "milty.h"
#include "types.h"
#include "subtyping.h"

/**
 * Determine implementation type used in MIL to represent
 * an XQuery type @a t.
 *
 * The implementation type consists of two aspects:
 *  - The quantifier: Is it a simple value or a sequence?
 *  - The prime type: What is the type if we disregard the quantifer?
 *
 * The former can be determined by a simple subtype check:
 * If the type is a subtype of { xs:AnyItem? }, it is a
 * simple value, otherwise a sequence.
 *
 * The latter is determined by checking the following subtype
 * relations:
 *  - prime(t) <: xs:boolean   results in 'bit'
 *  - prime(t) <: xs:integer   results in 'int'
 *  - prime(t) <: xs:string    results in 'str'
 *  - prime(t) <: node         results in 'node'
 *  - in all other cases, the prime implementation type is 'item'.
 *
 * @todo Implement decimal type
 *
 * @param t type in the XQuery type system
 * @return implementation type in MIL/Monet
 */
PFmty_t
PFty2mty (PFty_t t)
{
    PFmty_t ret;

    /*
     * As a safety net use the oid type for empty sequence nodes.
     * The type oid is actually illegal for core expressions, but
     * reserved for things like .seqbase(1@0). So as soon as we
     * try to map anything to type oid, something went really
     * wrong.
     */
    if (PFty_subtype (t, PFty_empty ()))
    {
        ret.quant = mty_simple;
        ret.ty = mty_oid;
        return ret;
    }

    /* Is it a simple value or a sequence? */
    if (PFty_subtype (t, PFty_opt (PFty_item ())))
        ret.quant = mty_simple;
    else
        ret.quant = mty_sequence;

    /* Now determine the prime type */
    if (PFty_subtype (PFty_prime (t), PFty_boolean ()))
        ret.ty = mty_bit;
    else if (PFty_subtype (PFty_prime (t), PFty_integer ()))
        ret.ty = mty_int;
    else if (PFty_subtype (PFty_prime (t), PFty_double ()))
        ret.ty = mty_dbl;
    else if (PFty_subtype (PFty_prime (t), PFty_decimal ()))
        ret.ty = mty_dbl;
    else if (PFty_subtype (PFty_prime (t), PFty_string ()))
        ret.ty = mty_str;
    else if (PFty_subtype (PFty_prime (t), PFty_node ()))
        ret.ty = mty_node;
    else
        ret.ty = mty_item;

    return ret;
}

/**
 * Tag the core tree with implementation types used in MIL. This
 * implementation type is determined according to our mapping
 * strategy and using the PFty2mty() function. If a node is a
 * variable node, we also store the implementation type in the
 * variable data structure #PFvar_t. This function operates
 * recursively.
 *
 * @param n The root node of the core tree for a call from outside.
 *          The function will recursively call itself with all the
 *          other core tree nodes.
 */
void
PFmiltype (PFcnode_t * n)
{
    unsigned int i;

    for (i = 0; i < PFCNODE_MAXCHILD && n->child[i]; i++)
        PFmiltype (n->child[i]);

    n->impl_ty = PFty2mty (n->type);

    /* In case of a variable, store implementation type in #PFvar_t struct */
    if (n->kind == c_var)
        n->sem.var->impl_ty = n->impl_ty;
}

/* vim:set shiftwidth=4 expandtab: */
