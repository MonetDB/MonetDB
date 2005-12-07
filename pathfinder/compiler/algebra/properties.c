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

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** starting from p, make two steps left */
#define LL(p) L(L(p))
/** starting from p, make a step left, then a step right */
#define LR(p) R(L(p))
/** starting from p, make a step right, then a step left */
#define RL(p) L(R(p))
/** starting from p, make two steps right */
#define RR(p) R(R(p))

struct PFprop_t {
    PFarray_t *constants;   /**< List of attributes marked constant,
                                 along with their corresponding values. */
    PFarray_t *icol;        /**< List of attributes required by the
                                 parent operators. */
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

    /* initialize icol attribute list */
    ret->icol = PFarray (sizeof (PFalg_att_t));

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
                    PFatt_str (attr));
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
            PFatt_str (attr));

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
 * Infer properties about constant columns; worker for prop_infer().
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
        case la_intersect:
        case la_distinct:
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
        case la_rownum:
        case la_number:
        case la_type:
        case la_type_assert:

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
                if (PFprop_const_at (n->child[0]->prop, j) != n->sem.cast.res)
                    PFprop_mark_const (
                            n->prop,
                            PFprop_const_at (n->child[0]->prop, j),
                            PFprop_const_val_at (n->child[0]->prop, j));

            break;


        case la_serialize:
        case la_empty_tbl:
        case la_cross:
        case la_eqjoin:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_distinct:
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
        case la_rownum:
        case la_number:
        case la_type:
        case la_type_assert:
        case la_seqty1:
        case la_all:
        case la_scjoin:
        case la_doc_tbl:
        case la_doc_access:
        case la_element:
        case la_element_tag:
        case la_attribute:
        case la_textnode:
        case la_docnode:
        case la_comment:
        case la_processi:
        case la_merge_adjacent:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_cond_err:
        case la_concat:
        case la_string_join:
            break;
    }
}



/**
 * Return number of icols attributes.
 */
unsigned int
PFprop_icols_count (const PFprop_t *prop)
{
    return PFarray_last (prop->icol);
}

/**
 * Return name of icols attribute number @a i (in container @a prop).
 * (Needed, e.g., to iterate over icols columns.)
 */
PFalg_att_t
PFprop_icols_at (PFprop_t *prop, unsigned int i)
{
    return *(PFalg_att_t *) PFarray_at (prop->icol, i);
}

/* declare prop_infer_icol */
static void prop_infer_icol (PFla_op_t *n, PFarray_t *icol);

static PFarray_t *
intersect_ocol (PFarray_t *icol, PFalg_schema_t ocol)
{
    assert (icol);

    PFarray_t *res = PFarray (sizeof (PFalg_att_t));

    /* intersect attributes */
    for (unsigned int i = 0; i < PFarray_last (icol); i++) {
        for (unsigned int j = 0; j < ocol.count; j++)
            if (*(PFalg_att_t *) PFarray_at (icol, i) == ocol.items[j].name) {
                *(PFalg_att_t *) PFarray_add (res) = ocol.items[j].name;
                break;
            }
    }
    return res;
}

static PFarray_t *
union_ (PFarray_t *a, PFarray_t *b)
{
    assert (a);
    assert (b);

    PFarray_t *c = PFarray (sizeof (PFalg_att_t));
    unsigned int i, j;

    /* copy first list */
    for (i = 0; i < PFarray_last (a); i++)
        *(PFalg_att_t *) PFarray_add (c)
            = *(PFalg_att_t *) PFarray_at (a, i);

    /* append missing attributes */
    for (j = 0; j < PFarray_last (b); j++) {
        for (i = 0; i < PFarray_last (a); i++)
            if (*(PFalg_att_t *) PFarray_at (b, j) ==
                *(PFalg_att_t *) PFarray_at (a, i))
                break;
        if (i == PFarray_last (a))
            *(PFalg_att_t *) PFarray_add (c)
                = *(PFalg_att_t *) PFarray_at (b, j);
    }

    return c;
}

static PFarray_t *
union_single (PFarray_t *a, PFalg_att_t b)
{
    assert (a);

    PFarray_t *c = PFarray (sizeof (PFalg_att_t));

    /* copy first list */
    for (unsigned int i = 0; i < PFarray_last (a); i++)
        *(PFalg_att_t *) PFarray_add (c)
            = *(PFalg_att_t *) PFarray_at (a, i);

    /* check occurrence of b */
    for (unsigned int i = 0; i < PFarray_last (a); i++)
        if (*(PFalg_att_t *) PFarray_at (a, i) == b)
            return c;

    /* add missing attribute b */
    *(PFalg_att_t *) PFarray_add (c) = b;

    return c;
}

static PFarray_t *
diff (PFarray_t *a, PFarray_t *b)
{
    assert (a);
    assert (b);

    PFarray_t *c = PFarray (sizeof (PFalg_att_t));
    unsigned int i, j;

    /* append only non matching attributes */
    for (i = 0; i < PFarray_last (a); i++) {
        for (j = 0; j < PFarray_last (b); j++)
            if (*(PFalg_att_t *) PFarray_at (a, i) ==
                *(PFalg_att_t *) PFarray_at (b, j))
                break;
        if (j == PFarray_last (b))
            *(PFalg_att_t *) PFarray_add (c)
                = *(PFalg_att_t *) PFarray_at (a, i);
    }

    return c;
}

static PFarray_t *
diff_single (PFarray_t *a, PFalg_att_t b)
{
    assert (a);

    PFarray_t *c = PFarray (sizeof (PFalg_att_t));

    /* append only non matching attributes */
    for (unsigned int i = 0; i < PFarray_last (a); i++)
        if (*(PFalg_att_t *) PFarray_at (a, i) != b)
            *(PFalg_att_t *) PFarray_add (c)
                = *(PFalg_att_t *) PFarray_at (a, i);

    return c;
}

bool
PFprop_contains_att (PFarray_t *a, PFalg_att_t b)
{
    assert (a);

    /* check occurence of attribute b in a */
    for (unsigned int i = 0; i < PFarray_last (a); i++)
        if (*(PFalg_att_t *) PFarray_at (a, i) == b)
            return true;

    return false;
}

void
PFprop_prune_schema (PFla_op_t *n)
{
    PFalg_schema_t tmp;
    tmp.count = PFarray_last (n->prop->icol);
    tmp.items = PFmalloc (tmp.count * sizeof (*(tmp.items)));

    unsigned int count = 0;
    for (unsigned int i = 0; i < PFarray_last (n->prop->icol); i++)
        for (unsigned int j = 0; j < n->schema.count; j++)
            if (*(PFalg_att_t *) PFarray_at (n->prop->icol, i) ==
                    n->schema.items[j].name) {
                tmp.items[i] = n->schema.items[j];
                count++;
                break;
            }

    if (count != PFarray_last (n->prop->icol))
        PFoops (OOPS_FATAL,
                "Consistency check failed! "
                "icol contains non-existent columns");

    n->schema = tmp;
}

/**
 * worker for PFprop_infer
 * infers the icol property during the second run
 * (uses edge counter stored in n->state_label from the first run)
 */
static void
prop_infer_icol (PFla_op_t *n, PFarray_t *icol)
{
    /* for some logical algebra nodes
       we have infer the children separately */
    bool skip_children = false;
    /* inferred icol property for children */
    PFarray_t *inf_icol = NULL;

    assert (n);
    assert (icol);

    /* collect all icol properties */
    n->prop->icol = union_ (n->prop->icol, icol);

    /* nothing to do if we haven't collected
       all incoming schemas of that node */
    if (n->state_label > 1) {
        n->state_label--;
        return;
    }
    /* remove all unnecessary columns from icol */
    n->prop->icol = intersect_ocol (n->prop->icol, n->schema);

    /* infer icol property for children */
    switch (n->kind) {
        case la_serialize:
            /* infer empty list for fragments */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            prop_infer_icol (L(n), inf_icol);

            /* infer pos|item schema for query body */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_pos;
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_item;
            prop_infer_icol (R(n), inf_icol);

            skip_children = true;
            break;

        case la_lit_tbl:
        case la_empty_tbl:
            break;

        case la_cross:
            inf_icol = n->prop->icol;
            break;

        case la_eqjoin:
            /* add both join columns to the inferred icols */
            inf_icol = union_single (n->prop->icol, n->sem.eqjoin.att1);
            inf_icol = union_single (inf_icol, n->sem.eqjoin.att2);
            break;

        case la_project:
            inf_icol = PFarray (sizeof (PFalg_att_t));
            /* rename icol columns from new to old */
            for (unsigned int i = 0; i < PFarray_last (n->prop->icol); i++)
                for (unsigned int j = 0; j < n->sem.proj.count; j++)
                    if (*(PFalg_att_t *) PFarray_at (n->prop->icol, i) ==
                            n->sem.proj.items[j].new) {
                        *(PFalg_att_t *) PFarray_add (inf_icol)
                            = n->sem.proj.items[j].old;
                        break;
                    }
            break;

        case la_select:
            /* add selected column to the inferred icols */
            inf_icol = union_single (n->prop->icol, n->sem.select.att);
            break;

        case la_disjunion:
            /* disjoint union also works with less columns */
            inf_icol = n->prop->icol;
            break;

        case la_intersect:
            /* add both intersect columns to the inferred icols */
            inf_icol = union_single (n->prop->icol, att_iter);
            inf_icol = union_single (inf_icol, att_item);
            break;

        case la_difference:
            inf_icol = n->prop->icol;
            /* to support both scenarios where difference is 
               used ((a) missing iterations and (b) except)
               extend the icols with all ocols */
            for (unsigned int i = 0; i < n->schema.count; i++)
                inf_icol = union_single (inf_icol, n->schema.items[i].name);
            break;

        case la_distinct:
            inf_icol = n->prop->icol;
            /* to support both scenarios where distinct is 
               used ((a) distinct and (b) unique iterations)
               extend the icols with all ocols */
            for (unsigned int i = 0; i < n->schema.count; i++)
                inf_icol = union_single (inf_icol, n->schema.items[i].name);
            break;

        case la_num_add:
        case la_num_subtract:
        case la_num_multiply:
        case la_num_divide:
        case la_num_modulo:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_concat:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.binary.res))
                break;

            inf_icol = diff_single (inf_icol, n->sem.binary.res);
            inf_icol = union_single (inf_icol, n->sem.binary.att1);
            inf_icol = union_single (inf_icol, n->sem.binary.att2);
            break;

        case la_num_neg:
        case la_bool_not:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.unary.res))
                break;

            inf_icol = diff_single (inf_icol, n->sem.unary.res);
            inf_icol = union_single (inf_icol, n->sem.unary.att);
            break;

        case la_sum:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.sum.res))
                break;

            inf_icol = diff_single (inf_icol, n->sem.sum.res);
            inf_icol = union_single (inf_icol, n->sem.sum.att);
            /* only infer part if available and not constant */
            if (n->sem.sum.part != att_NULL ||
                PFprop_const (n->prop, n->sem.sum.part))
                inf_icol = union_single (inf_icol, n->sem.sum.part);
            break;

        case la_count:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.count.res))
                break;

            inf_icol = diff_single (inf_icol, n->sem.count.res);
            /* only infer part if available and not constant */
            if (n->sem.count.part != att_NULL ||
                PFprop_const (n->prop, n->sem.count.part))
                inf_icol = union_single (inf_icol, n->sem.count.part);
            break;

        case la_rownum:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.rownum.attname))
                break;

            inf_icol = diff_single (inf_icol, n->sem.rownum.attname);
            /* only infer input columns if not constant */
            for (unsigned int i = 0; i < n->sem.rownum.sortby.count; i++)
                if (PFprop_const (n->prop, 
                                  n->sem.rownum.sortby.atts[i]))
                    inf_icol = union_single (inf_icol, 
                                             n->sem.rownum.sortby.atts[i]);
            /* only infer part if available and not constant */
            if (n->sem.rownum.part != att_NULL ||
                PFprop_const (n->prop, n->sem.rownum.part))
                inf_icol = union_single (inf_icol, n->sem.rownum.part);
            break;

        case la_number:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.number.attname))
                break;

            inf_icol = diff_single (inf_icol, n->sem.number.attname);
            if (n->sem.number.part != att_NULL)
                inf_icol = union_single (inf_icol, n->sem.number.part);
            break;

        case la_type:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.type.res))
                break;

            inf_icol = diff_single (inf_icol, n->sem.type.res);
            inf_icol = union_single (inf_icol, n->sem.type.att);
            break;

        case la_type_assert:
            /* if n->sem.type_a.att is not present this operator 
               has to be pruned -- therefore we do not care about
               inferrring this column in icol. */
            inf_icol = n->prop->icol;
            break;

        case la_cast:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.cast.res))
                break;

            inf_icol = diff_single (inf_icol, n->sem.cast.res);
            inf_icol = union_single (inf_icol, n->sem.cast.att);
            break;

        case la_seqty1:
        case la_all:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, 
                                      n->sem.blngroup.res))
                break;

            inf_icol = diff_single (inf_icol, n->sem.blngroup.res);
            inf_icol = union_single (inf_icol, n->sem.blngroup.att);
            /* only infer part if available and not constant */
            if (n->sem.blngroup.part != att_NULL ||
                PFprop_const (n->prop, n->sem.blngroup.part))
                inf_icol = union_single (inf_icol, n->sem.blngroup.part);
            break;

        case la_scjoin:
            /* infer empty list for fragments */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            prop_infer_icol (L(n), inf_icol);

            /* infer iter|item schema for input relation */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_iter;
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_item;
            prop_infer_icol (R(n), inf_icol);

            skip_children = true;
            break;

        case la_doc_tbl:
            /* infer iter|item schema for input relation */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_iter;
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_item;
            
            break;

        case la_doc_access:
            inf_icol = n->prop->icol;

            /* do not infer input columns if operator is not required */
            /* FIXME: remove the hardcoded name */
            if (!PFprop_contains_att (n->prop->icol, att_res)) 
                break;

            /* FIXME: remove the hardcoded name */
            inf_icol = diff_single (inf_icol, att_res);
            inf_icol = union_single (inf_icol, n->sem.cast.att);
            break;

        case la_element:
            /* whenever the element itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
               
            /* infer empty list for fragments */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            prop_infer_icol (L(n), inf_icol);

            /* do only infer input columns if operator is not required */
            if (!PFprop_contains_att (n->prop->icol, att_item))
            { 
                /* infer current icol schema for the qnames as replacement */
                inf_icol = n->prop->icol;
                prop_infer_icol (RL(n), inf_icol);

                /* infer empty list for missing content */
                inf_icol = PFarray (sizeof (PFalg_att_t));
                prop_infer_icol (RR(n), inf_icol);
            } else {
                /* infer iter|item schema for element name relation */
                inf_icol = PFarray (sizeof (PFalg_att_t));
                *(PFalg_att_t *) PFarray_add (inf_icol) = att_iter;
                *(PFalg_att_t *) PFarray_add (inf_icol) = att_item;
                prop_infer_icol (RL(n), inf_icol);

                /* infer iter|pos|item schema for element content relation */
                inf_icol = PFarray (sizeof (PFalg_att_t));
                *(PFalg_att_t *) PFarray_add (inf_icol) = att_iter;
                *(PFalg_att_t *) PFarray_add (inf_icol) = att_pos;
                *(PFalg_att_t *) PFarray_add (inf_icol) = att_item;
                prop_infer_icol (RR(n), inf_icol);
            }

            skip_children = true;
            break;

        case la_attribute:
            /* whenever the attribute itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement (introduce projection res:qn instead) */
               
            inf_icol = diff_single (n->prop->icol, n->sem.attr.qn);
            inf_icol = union_single (inf_icol, n->sem.attr.qn);
            prop_infer_icol (L(n), inf_icol);

            inf_icol = PFarray (sizeof (PFalg_att_t));
            /* do only infer input columns if operator is not required */
            if (PFprop_contains_att (n->prop->icol, n->sem.attr.res))
            {
                *(PFalg_att_t *) PFarray_add (inf_icol) = att_iter;
                *(PFalg_att_t *) PFarray_add (inf_icol) = n->sem.attr.val;
            }
            prop_infer_icol (R(n), inf_icol);

            skip_children = true;
            break;

        case la_textnode:
            /* whenever the textnode is not needed (see icols) the
               textnode constructor can be replaced by a projection
               (res:item) */

            inf_icol = n->prop->icol;
            /* infer item instead of res */
            inf_icol = diff_single (inf_icol, n->sem.textnode.res);
            inf_icol = union_single (inf_icol, n->sem.textnode.item);
            break;

        case la_docnode:
        case la_comment:
        case la_processi:
            assert (!"not implemented yet?");

        case la_merge_adjacent:
            /* infer empty list for fragments */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            prop_infer_icol (L(n), inf_icol);

            /* infer iter|pos|item schema for element content relation */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_iter;
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_pos;
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_item;
            prop_infer_icol (R(n), inf_icol);

            skip_children = true;
            break;

        case la_roots:
            /* infer incoming icols for input relation */
            inf_icol = n->prop->icol;
            break;

        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
            /* infer empty list for fragments */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            break;

        case la_cond_err:
            /* infer incoming icols for input relation */
            inf_icol = n->prop->icol;
            prop_infer_icol (L(n), inf_icol);

            /* infer attribute that triggers error generation
               for error checking relation  */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            *(PFalg_att_t *) PFarray_add (inf_icol) = n->sem.err.att;
            prop_infer_icol (R(n), inf_icol);
            break;

        case la_string_join:
            /* infer iter|pos|item schema for first input relation */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_iter;
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_pos;
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_item;
            prop_infer_icol (L(n), inf_icol);

            /* infer iter|item schema for second input relation */
            inf_icol = PFarray (sizeof (PFalg_att_t));
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_iter;
            *(PFalg_att_t *) PFarray_add (inf_icol) = att_item;
            prop_infer_icol (R(n), inf_icol);

            skip_children = true;
            break;

        default:
            /* missing node should only occur in case la_roots */
            PFoops (OOPS_FATAL, 
                    "kind %i not supported in icol property inference.",
                    n->kind);
    }

    if (!skip_children)
        /* infer properties for children */
        for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
            prop_infer_icol (n->child[i], inf_icol);
}

/* worker for PFprop_infer */
static void
prop_infer (PFla_op_t *n)
{
    assert (n);
    /* count number of incoming edges
       (during first run) */
    n->state_label++;

    /* nothing to do if we already visited that node */
    if (n->prop)
        return;
    /* otherwise initialize edge counter (during first run) */
    else
        n->state_label = 1;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i]);

    /* initialize property container */
    n->prop = PFprop ();

    /* infer information on constant columns first */
    infer_const (n);
}

void
PFprop_infer (PFla_op_t *n)
{
    prop_infer (n);
    prop_infer_icol (n, PFarray (sizeof (PFalg_att_t)));
}

/* vim:set shiftwidth=4 expandtab: */
