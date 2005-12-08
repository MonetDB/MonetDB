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
    PFarray_t  *constants;   /**< List of attributes marked constant,
                                  along with their corresponding values. */
    PFalg_att_t icols;       /**< List of attributes required by the
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

    /* initialize icols attribute list */
    ret->icols = 0;

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


/* 
 * count number of icols attributes
 */
static unsigned int
icols_count (const PFprop_t *prop)
{
    PFalg_att_t icols = prop->icols;
    unsigned int counter = 0;

    while (icols) {
        counter += icols & 1;
        icols >>= 1;
    }
    return counter;
}
/**
 * Return icols attributes in an attlist.
 */
PFalg_attlist_t
PFprop_icols_to_attlist (const PFprop_t *prop)
{
    PFalg_attlist_t new_list;
    PFalg_att_t icols = prop->icols;
    unsigned int counter = 0, bit_shift = 1;

    new_list.count = icols_count (prop);
    new_list.atts = PFmalloc (new_list.count * sizeof (*(new_list.atts)));

    /* unfold icols into a list of attributes */
    while (icols && counter < new_list.count) {
        new_list.atts[counter] = prop->icols & bit_shift;
        bit_shift <<= 1;

        counter += icols & 1;
        icols >>= 1;
    }
    return new_list;
}

/* declare prop_infer_icols */
static void prop_infer_icols (PFla_op_t *n, PFalg_att_t icols);

static PFalg_att_t
intersect_ocol (PFalg_att_t icols, PFalg_schema_t schema_ocols)
{
    PFalg_att_t ocols = 0;

    /* intersect attributes */
    for (unsigned int j = 0; j < schema_ocols.count; j++)
        ocols |= schema_ocols.items[j].name;

    return icols & ocols;
}

static PFalg_att_t
union_ (PFalg_att_t a, PFalg_att_t b)
{
    return a | b;
}

static PFalg_att_t
diff (PFalg_att_t a, PFalg_att_t b)
{
    return a & (~b);
}

/* this could be handy for the optimization part */
static void
PFprop_prune_schema (PFla_op_t *n)
{
    assert (n && n->prop);

    PFalg_schema_t tmp;
    tmp.count = icols_count (n->prop);
    tmp.items = PFmalloc (tmp.count * sizeof (*(tmp.items)));

    unsigned int count = 0;
    for (unsigned int j = 0; j < n->schema.count; j++)
        if (n->prop->icols & n->schema.items[j].name) {
            tmp.items[count] = n->schema.items[j];
            count++;
            break;
        }

    if (count != icols_count (n->prop))
        PFoops (OOPS_FATAL,
                "Consistency check failed! "
                "icols contains non-existent columns");

    n->schema = tmp;
}

/**
 * worker for PFprop_infer
 * infers the icols property during the second run
 * (uses edge counter stored in n->state_label from the first run)
 */
static void
prop_infer_icols (PFla_op_t *n, PFalg_att_t icols)
{
    /* for some logical algebra nodes
       we have infer the children separately */
    bool skip_children = false;
    /* inferred icols property for children */
    PFalg_att_t inf_icols = 0;

    assert (n);

    /* collect all icols properties */
    n->prop->icols = union_ (n->prop->icols, icols);

    /* nothing to do if we haven't collected
       all incoming schemas of that node */
    if (n->state_label > 1) {
        n->state_label--;
        return;
    }
    /* remove all unnecessary columns from icols */
    n->prop->icols = intersect_ocol (n->prop->icols, n->schema);

    /* infer icols property for children */
    switch (n->kind) {
        case la_serialize:
            /* infer empty list for fragments */
            prop_infer_icols (L(n), 0);

            /* infer pos|item schema for query body */
            inf_icols = union_ (att_pos, att_item);
            prop_infer_icols (R(n), inf_icols);

            skip_children = true;
            break;

        case la_lit_tbl:
        case la_empty_tbl:
            break;

        case la_cross:
            inf_icols = n->prop->icols;
            break;

        case la_eqjoin:
            /* add both join columns to the inferred icols */
            inf_icols = union_ (union_ (n->prop->icols,
                                        n->sem.eqjoin.att1),
                                n->sem.eqjoin.att2);
            break;

        case la_project:
            inf_icols = n->prop->icols;
            /* rename icols columns from new to old */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                if (inf_icols & n->sem.proj.items[i].new)
                    inf_icols = union_ (diff (inf_icols,
                                              n->sem.proj.items[i].new),
                                        n->sem.proj.items[i].old);
            break;

        case la_select:
            /* add selected column to the inferred icols */
            inf_icols = union_ (n->prop->icols, n->sem.select.att);
            break;

        case la_disjunion:
            /* disjoint union also works with less columns */
            inf_icols = n->prop->icols;
            break;

        case la_intersect:
            /* add both intersect columns to the inferred icols */
            inf_icols = union_ (union_ (n->prop->icols, att_iter),
                                att_item);
            break;

        case la_difference:
            inf_icols = n->prop->icols;
            /* to support both scenarios where difference is 
               used ((a) missing iterations and (b) except)
               extend the icols with all ocols */
            for (unsigned int i = 0; i < n->schema.count; i++)
                inf_icols = union_ (inf_icols, n->schema.items[i].name);
            break;

        case la_distinct:
            inf_icols = n->prop->icols;
            /* to support both scenarios where distinct is 
               used ((a) distinct and (b) unique iterations)
               extend the icols with all ocols */
            for (unsigned int i = 0; i < n->schema.count; i++)
                inf_icols = union_ (inf_icols, n->schema.items[i].name);
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
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.binary.res))
                break;

            inf_icols = diff (inf_icols, n->sem.binary.res);
            inf_icols = union_ (inf_icols, n->sem.binary.att1);
            inf_icols = union_ (inf_icols, n->sem.binary.att2);
            break;

        case la_num_neg:
        case la_bool_not:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.unary.res))
                break;

            inf_icols = diff (inf_icols, n->sem.unary.res);
            inf_icols = union_ (inf_icols, n->sem.unary.att);
            break;

        case la_sum:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.sum.res))
                break;

            inf_icols = diff (inf_icols, n->sem.sum.res);
            inf_icols = union_ (inf_icols, n->sem.sum.att);
            /* only infer part if available and not constant */
            if (n->sem.sum.part != att_NULL ||
                PFprop_const (n->prop, n->sem.sum.part))
                inf_icols = union_ (inf_icols, n->sem.sum.part);
            break;

        case la_count:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.count.res))
                break;

            inf_icols = diff (inf_icols, n->sem.count.res);
            /* only infer part if available and not constant */
            if (n->sem.count.part != att_NULL ||
                PFprop_const (n->prop, n->sem.count.part))
                inf_icols = union_ (inf_icols, n->sem.count.part);
            break;

        case la_rownum:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.rownum.attname))
                break;

            inf_icols = diff (inf_icols, n->sem.rownum.attname);
            /* only infer input columns if not constant */
            for (unsigned int i = 0; i < n->sem.rownum.sortby.count; i++)
                if (PFprop_const (n->prop, 
                                  n->sem.rownum.sortby.atts[i]))
                    inf_icols = union_ (inf_icols, 
                                        n->sem.rownum.sortby.atts[i]);
            /* only infer part if available and not constant */
            if (n->sem.rownum.part != att_NULL ||
                PFprop_const (n->prop, n->sem.rownum.part))
                inf_icols = union_ (inf_icols, n->sem.rownum.part);
            break;

        case la_number:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.number.attname))
                break;

            inf_icols = diff (inf_icols, n->sem.number.attname);
            if (n->sem.number.part != att_NULL)
                inf_icols = union_ (inf_icols, n->sem.number.part);
            break;

        case la_type:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.type.res))
                break;

            inf_icols = diff (inf_icols, n->sem.type.res);
            inf_icols = union_ (inf_icols, n->sem.type.att);
            break;

        case la_type_assert:
            /* if n->sem.type_a.att is not present this operator 
               has to be pruned -- therefore we do not care about
               inferrring this column in icols. */
            inf_icols = n->prop->icols;
            break;

        case la_cast:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.cast.res))
                break;

            inf_icols = diff (inf_icols, n->sem.cast.res);
            inf_icols = union_ (inf_icols, n->sem.cast.att);
            break;

        case la_seqty1:
        case la_all:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            if (!(n->prop->icols & n->sem.blngroup.res))
                break;

            inf_icols = diff (inf_icols, n->sem.blngroup.res);
            inf_icols = union_ (inf_icols, n->sem.blngroup.att);
            /* only infer part if available and not constant */
            if (n->sem.blngroup.part != att_NULL ||
                PFprop_const (n->prop, n->sem.blngroup.part))
                inf_icols = union_ (inf_icols, n->sem.blngroup.part);
            break;

        case la_scjoin:
            /* infer empty list for fragments */
            prop_infer_icols (L(n), 0);

            /* infer iter|item schema for input relation */
            inf_icols = union_ (att_iter, att_item);
            prop_infer_icols (R(n), inf_icols);

            skip_children = true;
            break;

        case la_doc_tbl:
            /* infer iter|item schema for input relation */
            inf_icols = union_ (att_iter, att_item);
            
            break;

        case la_doc_access:
            inf_icols = n->prop->icols;

            /* do not infer input columns if operator is not required */
            /* FIXME: remove the hardcoded name */
            if (!(n->prop->icols & att_res)) 
                break;

            /* FIXME: remove the hardcoded name */
            inf_icols = diff (inf_icols, att_res);
            inf_icols = union_ (inf_icols, n->sem.cast.att);
            break;

        case la_element:
            /* whenever the element itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement */
               
            /* infer empty list for fragments */
            prop_infer_icols (L(n), 0);

            /* do only infer input columns if operator is not required */
            if (!(n->prop->icols & att_item))
            { 
                /* infer current icols schema for the qnames as replacement */
                inf_icols = n->prop->icols;
                prop_infer_icols (RL(n), inf_icols);

                /* infer empty list for missing content */
                prop_infer_icols (RR(n), 0);
            } else {
                /* infer iter|item schema for element name relation */
                inf_icols = union_ (att_iter, att_item);
                prop_infer_icols (RL(n), inf_icols);

                /* infer iter|pos|item schema for element content relation */
                inf_icols = union_ (union_ (att_iter, att_item), att_pos);
                prop_infer_icols (RR(n), inf_icols);
            }

            skip_children = true;
            break;

        case la_attribute:
            /* whenever the attribute itself is not needed column item
               is missing and therefore the name expression can be
               used as replacement (introduce projection res:qn instead) */
               
            inf_icols = diff (n->prop->icols, n->sem.attr.qn);
            inf_icols = union_ (inf_icols, n->sem.attr.qn);
            prop_infer_icols (L(n), inf_icols);

            /* do only infer input columns if operator is not required */
            if ((n->prop->icols & n->sem.attr.res))
            {
                inf_icols = union_ (att_iter, n->sem.attr.val);
                prop_infer_icols (R(n), inf_icols);
            } else 
                prop_infer_icols (R(n), 0);


            skip_children = true;
            break;

        case la_textnode:
            /* whenever the textnode is not needed (see icols) the
               textnode constructor can be replaced by a projection
               (res:item) */

            inf_icols = n->prop->icols;
            /* infer item instead of res */
            inf_icols = diff (inf_icols, n->sem.textnode.res);
            inf_icols = union_ (inf_icols, n->sem.textnode.item);
            break;

        case la_docnode:
        case la_comment:
        case la_processi:
            assert (!"not implemented yet?");

        case la_merge_adjacent:
            /* infer empty list for fragments */
            prop_infer_icols (L(n), 0);

            /* infer iter|pos|item schema for element content relation */
            inf_icols = union_ (union_ (att_iter, att_item), att_pos);
            prop_infer_icols (R(n), inf_icols);

            skip_children = true;
            break;

        case la_roots:
            /* infer incoming icols for input relation */
            inf_icols = n->prop->icols;
            break;

        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
            /* infer empty list for fragments */
            inf_icols = 0;
            break;

        case la_cond_err:
            /* infer incoming icols for input relation */
            inf_icols = n->prop->icols;
            prop_infer_icols (L(n), inf_icols);

            /* infer attribute that triggers error generation
               for error checking relation  */
            inf_icols = n->sem.err.att;
            prop_infer_icols (R(n), inf_icols);
            break;

        case la_string_join:
            /* infer iter|pos|item schema for first input relation */
            inf_icols = union_ (union_ (att_iter, att_item), att_pos);
            prop_infer_icols (L(n), inf_icols);

            /* infer iter|item schema for second input relation */
            inf_icols = union_ (att_iter, att_item);
            prop_infer_icols (R(n), inf_icols);

            skip_children = true;
            break;

        default:
            /* missing node should only occur in case la_roots */
            PFoops (OOPS_FATAL, 
                    "kind %i not supported in icols property inference.",
                    n->kind);
    }

    if (!skip_children)
        /* infer properties for children */
        for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
            prop_infer_icols (n->child[i], inf_icols);
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
    prop_infer_icols (n, 0);
}

/* vim:set shiftwidth=4 expandtab: */
