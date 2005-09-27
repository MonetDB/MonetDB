/**
 * @file
 *
 * Properties of logical algebra expressions.
 *
 * We consider some properties that can be derived on the logical
 * level of our algebra, like key properties, or the information
 * that a column contains only constant values.  These properties
 * may still be helpful for physical optimization; we will thus
 * propagate any logical property to the physical tree as well.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include <assert.h>

#include "properties.h"

#include "oops.h"
#include "mem.h"

struct PFprop_t {
    PFarray_t   *constants;   /**< List of attributes marked constant,
                                   along with their corresponding values. */
};

struct const_t {
    PFalg_att_t  attr;
    PFalg_atom_t value;
};
typedef struct const_t const_t;

/**
 * Create new property container.
 */
PFprop_t *
PFprop (void)
{
    PFprop_t *ret = PFmalloc (sizeof (PFprop_t));

    /* allocate/initialize different slots */
    ret->constants = PFarray (sizeof (const_t));

    return ret;
}

/**
 * Test if @a attr is marked constant in container @a prop.
 */
bool
PFprop_const (const PFprop_t *prop, const PFalg_att_t attr)
{
    assert (prop);

    for (unsigned int i = 0; i < PFarray_last (prop->constants); i++)
        if (attr == ((const_t *) PFarray_at (prop->constants, i))->attr)
            return true;

    return false;
}

/**
 * Mark @a attr as constant with value @a value in container @a prop.
 */
void
PFprop_mark_const (PFprop_t *prop, const PFalg_att_t attr, PFalg_atom_t value)
{
    assert (prop);

#ifndef NDEBUG
    for (unsigned int i = 0; i < PFarray_last (prop->constants); i++)
        if (attr == ((const_t *) PFarray_at (prop->constants, i))->attr)
            PFoops (OOPS_FATAL,
                    "attribute `%s' already declared constant", 
                    PFatt_print (attr));
#endif

    *(const_t *) PFarray_add (prop->constants)
        = (const_t) { .attr = attr, .value = value };
}

/**
 * Lookup value of @a attr in property container @a prop.  Attribute
 * @a attr must be marked constant, otherwise the function will fail.
 */
PFalg_atom_t
PFprop_const_val (const PFprop_t *prop, const PFalg_att_t attr)
{
    for (unsigned int i = 0; i < PFarray_last (prop->constants); i++)
        if (attr == ((const_t *) PFarray_at (prop->constants, i))->attr)
            return ((const_t *) PFarray_at (prop->constants, i))->value;

    PFoops (OOPS_FATAL,
            "could not find attribute that is supposed to be constant: `%s'",
            PFatt_print (attr));

    assert(0); /* never reached due to "exit" in PFoops */
    return PFalg_lit_int (0); /* pacify picky compilers */
}

/**
 * Return number of attributes marked const.
 */
unsigned int
PFprop_const_count (const PFprop_t *prop)
{
    return PFarray_last (prop->constants);
}

/**
 * Return name of constant attribute number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_att_t
PFprop_const_at (PFprop_t *prop, unsigned int i)
{
    return ((const_t *) PFarray_at (prop->constants, i))->attr;
}

/**
 * Return value of constant attribute number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_atom_t
PFprop_const_val_at (PFprop_t *prop, unsigned int i)
{
    return ((const_t *) PFarray_at (prop->constants, i))->value;
}

/**
 * Infer properties about constant columns; worker for PFprop_infer().
 */
void
infer_const (PFla_op_t *n)
{
    /*
     * Several operates (at least) propagate constant columns
     * to their output 1:1.
     */
    switch (n->kind) {

        case la_cross:
        case la_eqjoin:
        case la_select:
        case la_type:
        case la_rownum:
        case la_intersect:
        case la_num_add:
        case la_num_subtract:
        case la_num_multiply:
        case la_num_divide:
        case la_num_modulo:
        case la_num_eq:
        case la_num_gt:
        case la_num_neg:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_distinct:

            /* propagate information from both input operators */
            for (unsigned int i = 0; i < PFPA_OP_MAXCHILD && n->child[i]; i++)
                for (unsigned int j = 0;
                        j < PFprop_const_count (n->child[i]->prop); j++)
                    if (!PFprop_const (n->prop,
                                       PFprop_const_at (n->child[i]->prop, j)))
                        PFprop_mark_const (
                                n->prop,
                                PFprop_const_at (n->child[i]->prop, j),
                                PFprop_const_val_at (n->child[i]->prop, j));
            break;

        default:
            break;
    }

    /*
     * Now consider more specific stuff from various rules.
     */
    switch (n->kind) {

        case la_lit_tbl:

            /* check for constant columns */
            for (unsigned int col = 0; col < n->schema.count; col++) {

                bool          constant = true;
                PFalg_atom_t  val;

                for (unsigned int row = 0; row < n->sem.lit_tbl.count; row++)
                    if (row == 0)
                        val = n->sem.lit_tbl.tuples[0].atoms[col];
                    else
                        if (!PFalg_atom_comparable (
                                    val, n->sem.lit_tbl.tuples[0].atoms[col])
                            || PFalg_atom_cmp (
                                    val, n->sem.lit_tbl.tuples[0].atoms[col])) {
                            constant = false;
                            break;
                        }

                if (constant)
                    PFprop_mark_const (n->prop, n->schema.items[col].name, val);
            }
            break;

        case la_select:

            /*
             * If the selection criterion itself is constant, we may
             * find better rules.
             */
            if (PFprop_const (n->child[0]->prop, n->sem.select.att)) {

                assert (PFalg_atom_comparable (
                            PFprop_const_val (n->child[0]->prop,
                                              n->sem.select.att),
                            PFalg_lit_bln (true)));

                if (!PFalg_atom_cmp (PFprop_const_val (n->child[0]->prop,
                                                       n->sem.select.att),
                                     PFalg_lit_bln (true))) {
                    /*
                     * We select on a column that is constant true.
                     * This means that the selection does nothing, and
                     * we might even infer more interesting properties.
                     */
                }
                else {
                    /* FIXME: result will be an empty table */
                    assert (0);
                }
            }
            else {
                /* the selection criterion itself will now also be const */
                PFprop_mark_const (
                        n->prop, n->sem.select.att, PFalg_lit_bln (true));
            }
            break;

        case la_project:

            /*
             * projection does not affect properties, except for the
             * column name change.
             */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                if (PFprop_const (n->child[0]->prop, n->sem.proj.items[i].old))
                    PFprop_mark_const (n->prop,
                                       n->sem.proj.items[i].new,
                                       PFprop_const_val (
                                           n->child[0]->prop,
                                           n->sem.proj.items[i].old));

            break;

        case la_cast:

            /*
             * propagate information from input operator, skipping the
             * column that we shall cast
             *
             * FIXME: Also actually do the cast (statically) for the
             *        respective column.
             */
            for (unsigned int j = 0;
                    j < PFprop_const_count (n->child[0]->prop); j++)
                if (PFprop_const_at (n->child[0]->prop, j) != n->sem.cast.att)
                    PFprop_mark_const (
                            n->prop,
                            PFprop_const_at (n->child[0]->prop, j),
                            PFprop_const_val_at (n->child[0]->prop, j));

            break;


        case la_cross:
        case la_eqjoin:
        case la_type:
        case la_rownum:
        case la_empty_tbl:
        case la_scjoin:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_serialize:
        case la_num_add:
        case la_num_subtract:
        case la_num_multiply:
        case la_num_divide:
        case la_num_modulo:
        case la_num_eq:
        case la_num_gt:
        case la_num_neg:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_sum:
        case la_count:
        case la_distinct:
        case la_element:
        case la_element_tag:
        case la_attribute:
        case la_textnode:
        case la_docnode:
        case la_comment:
        case la_processi:
        case la_concat:
        case la_merge_adjacent:
        case la_doc_access:
        case la_string_join:
        case la_seqty1:
        case la_all:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_doc_tbl:
        case la_dummy:
            break;
    }
}

void
PFprop_infer (PFla_op_t *n)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->prop)
        return;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        PFprop_infer (n->child[i]);

    /* initialize property container */
    n->prop = PFprop ();

    /* infer information on constant columns first */
    infer_const (n);
}


/* vim:set shiftwidth=4 expandtab: */
