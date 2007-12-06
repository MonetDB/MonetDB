/**
 * @file
 *
 * Common subexpression elimination on the logical algebra.
 *
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
 * $Id
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include "oops.h"
#include "opt_algebra_cse.h"
#include "array.h"
#include "alg_dag.h"
#include "algebra.h"
#include "logical.h"
#include "logical_mnemonic.h"

/* compare types in staircase join operator nodes */
#include "subtyping.h"

#include <assert.h>
#include <string.h> /* strcmp */
#include <stdio.h>


/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a right step */
#define R(p) ((p)->child[1])

/** prune already checked nodes */
#define SEEN(p) ((p)->bit_dag)

/** lookup subtree with already generated CSE parts */
#define CSE(p)  (lookup_cse((cse_map),(p)))
#define INCSE(o,c) (insert_cse((cse_map), (o), (c)))

/** original nodes */
#define ORI(p)  (lookup_ori((ori_map),(p)))
#define INORI(c,o) (insert_ori((ori_map), (c), (o)))

/** lookup subtree with annotated real/effective
 * attributes. */
#define EFF(p)  (lookup_eff((eff_map),(p)))
#define INEFF(o,e) (insert_eff((eff_map), (o), (e)))

/* XOR */
#define XOR(a,b)    !(((a) && (b)) || ((!(a)) && (!(b))))

PFarray_t *cse_map;
PFarray_t *eff_map;
PFarray_t *ori_map;

/**
 * Subexpressions that we already saw.
 *
 * This is an array of arrays. We create a separate for
 * each algebra node kind we encounter. This speeds up
 * lookups when we search for an existing algebra node.
 */
static PFarray_t *subexps;

/** struct to map the original operators
 * to those generated during the CSE. */
struct ori_cse_map_t {
    PFla_op_t *ori;
    PFla_op_t *cse;
};
typedef struct ori_cse_map_t ori_cse_map_t;

/* structure to map the original attribute/type
 * pair to the effective attribute/type pair
 */
struct eff_map_t {
    PFalg_att_t ori_att;
    PFalg_att_t eff_att;
};
typedef struct eff_map_t eff_map_t;

/* structure to map the original operator to
 * their effective attributes.
 */
struct ori_eff_map_t {
    PFla_op_t *ori;
    PFarray_t *eff_map;
};
typedef struct ori_eff_map_t ori_eff_map_t;

/* structure tp map the operators in the CSE plan
 * to their original operators who caused the insertion.
 */
struct cse_ori_map_t {
    PFla_op_t *cse;
    PFla_op_t *ori;
};
typedef struct cse_ori_map_t cse_ori_map_t;

/* worker for 'CSE(p)': based on the original
 * subtree looks up the corresponding subtree with
 * already generated CSE subtrees.
 * When ori is NULL return NULL.
 */
static PFla_op_t *
lookup_cse (PFarray_t *map, PFla_op_t *ori)
{
    if (ori == NULL) return NULL;

    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((ori_cse_map_t *) (PFarray_at (map, i)))->ori == ori)
            return ((ori_cse_map_t *) PFarray_at (map, i))->cse;
    }

    PFoops (OOPS_FATAL, "Could not look up node (%i) (cse subtrees)", ori->kind);

    return NULL; /* satisfy picky compilers */
}

/* worker for 'ORI(p)': based on the cse subtree
 * looks up the corresponding subtree in the
 * original plan, caused the insertion.
 */
static PFla_op_t*
lookup_ori (PFarray_t *map, PFla_op_t *cse)
{
    assert (map);
    assert (cse);

    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((cse_ori_map_t *) (PFarray_at (map, i)))->cse == cse)
            return ((cse_ori_map_t *) PFarray_at (map, i))->ori;
    }
    assert (!"could not look up node (ori subtrees)");

    return NULL; /* satisfy picky compilers */
}
/* Worker for 'INORI(c,o)'.
 * Insert a structure that maps the operator in the
 * cse tree to its original operator.
 */
static void
insert_ori (PFarray_t *map, PFla_op_t *cse,
            PFla_op_t *ori)
{
    assert (map);
    assert (cse);
    assert (ori);

    /* check if the operator has already a mapping */
    cse_ori_map_t *temp = NULL;
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((cse_ori_map_t *) (PFarray_at (map, i)))->cse == cse) {
            temp = (cse_ori_map_t *) PFarray_at (map, i);
            break;
        }
    }

    if (temp) {
        PFoops (OOPS_FATAL, "No operator should be remapped.");
    }

    *(cse_ori_map_t *) PFarray_add (map) = (cse_ori_map_t)
        {
            .cse = cse,
            .ori = ori
        };
}



/* Worker for 'INCSE(o,c)'.
 * Inserts a mapping from the original nodes to their equivalents
 * in the CSE plan.*/
static void
insert_cse (PFarray_t *map, PFla_op_t *ori,
            PFla_op_t *cse)
{
    assert (map);
    assert (ori);
    assert (cse);

    /* check if the operator has already a mapping */
    ori_cse_map_t *temp = NULL;
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((ori_cse_map_t *) (PFarray_at (map, i)))->ori == ori) {
            temp = (ori_cse_map_t *) PFarray_at (map, i);
            break;
        }
    }

    if (temp) {
        PFoops (OOPS_FATAL, "No operator should be remapped.");
    }

    *(ori_cse_map_t *) PFarray_add (map) = (ori_cse_map_t)
        {
            .ori = ori,
            .cse = cse
        };
}

/* create a new unique name, based on the used
 * cols in @a n schema.
 */
static PFalg_att_t
create_unq_name (PFla_op_t *n, PFalg_att_t att)
{
    bool name_conflict = false;
    PFalg_att_t used_cols = att_NULL;
    PFalg_att_t new_col = att;

    for (unsigned int i = 0; i < n->schema.count; i++)
    {
        used_cols = used_cols | n->schema.items[i].name;
        if (n->schema.items[i].name == att) {
            name_conflict = true;
        }
    }

    if (name_conflict) {
        new_col = PFalg_ori_name (PFalg_unq_name (att, 0),
                            ~used_cols);
    }

    return new_col;
}

/* substitute the name in the schema with a new one */
static void
patch_schema (PFla_op_t *n, PFalg_att_t old, PFalg_att_t new)
{
    unsigned int i = 0;
    for (i = 0; i < n->schema.count; i++)
    {
        if (n->schema.items[i].name == old) {
            break;
        }
    }

    if (i >= n->schema.count)
        PFoops (OOPS_FATAL,
                "name (%s) not found in schema", PFatt_str (old));

    n->schema.items[i].name = new;
}


/* worker for 'INEFF(o,p)'.
 * Annotates an operator @a ori
 * with a projection. */
static void
insert_eff (PFarray_t *map, PFla_op_t *ori,
        PFarray_t *proj)
{
    assert (map);
    assert (ori);
    assert (proj);

    /* check if the operator has already a mapping */
    ori_eff_map_t *temp = NULL;
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((ori_eff_map_t *) (PFarray_at (map, i)))->ori == ori) {
            temp = (ori_eff_map_t *) PFarray_at (map, i);
            break;
        }
     }

    if (temp) {
        PFoops (OOPS_FATAL, "No operator should be remapped to"
            "other effective attributes");
    }

    *(ori_eff_map_t *) PFarray_add (map) =
        (ori_eff_map_t)
        {
            .ori = ori,
            .eff_map = proj
        };
}


/* worker for 'EFF(p)': based on the original
 * subtree looks up the corresponding subtree
 * with already annotated effective attributes. */
static PFarray_t *
lookup_eff (PFarray_t *map, PFla_op_t *ori)
{
    for (unsigned int i = 0; i < PFarray_last (map); i++) {
        if (((ori_eff_map_t *) (PFarray_at (map, i)))->ori == ori)
            return ((ori_eff_map_t *) PFarray_at (map, i))->eff_map;
    }

    return NULL; /* satisfy picky compilers */
}

/* Worker for 'eff_attribute (map, att)'.
 * Checks the map and returns the effective attribute
 */
static PFalg_att_t
eff_attribute_ (PFarray_t *map, PFalg_att_t att)
{
    for (unsigned int i = 0; i < PFarray_last (map);
            i++) {
        if ( (*(eff_map_t *)PFarray_at (map, i)).ori_att == att)
            return (*(eff_map_t *)PFarray_at (map, i)).eff_att;
    }

    /* assert ("name not found"); */
    return att_NULL; /* pacify picky compilers */
}

/* See 'eff_attribute_ (map, att)' above. */
static PFalg_att_t
eff_attribute (PFla_op_t *n, PFalg_att_t att)
{
    PFarray_t *map = EFF (n);

    return eff_attribute_ (map, att);
}

/**
 * Test the equality of two literal table tuples.
 *
 * @param a Tuple to test against tuple @a b.
 * @param b Tuple to test against tuple @a a.
 * @return Boolean value @c true, if the two tuples are equal.
 */
static bool
tuple_eq (PFalg_tuple_t a, PFalg_tuple_t b)
{
    unsigned int i;
    bool mismatch = false;

    /* schemata are not equal if they have a different number of attributes */
    if (a.count != b.count)
        return false;

    for (i = 0; i < a.count; i++) {
        if (a.atoms[i].type != b.atoms[i].type)
            break;

        switch (a.atoms[i].type) {
            /* if type is nat, compare nat member of union */
            case aat_nat:
                if (a.atoms[i].val.nat_ != b.atoms[i].val.nat_)
                    mismatch = true;
                break;
            /* if type is int, compare int member of union */
            case aat_int:
                if (a.atoms[i].val.int_ != b.atoms[i].val.int_)
                    mismatch = true;
                break;
            /* if type is str, compare str member of union */
            case aat_uA:
            case aat_str:
                if (strcmp (a.atoms[i].val.str, b.atoms[i].val.str))
                    mismatch = true;
                break;
            /* if type is float, compare float member of union */
            case aat_dec:
                if (a.atoms[i].val.dec_ != b.atoms[i].val.dec_)
                    mismatch = true;
                break;
            /* if type is double, compare double member of union */
            case aat_dbl:
                if (a.atoms[i].val.dbl != b.atoms[i].val.dbl)
                    mismatch = true;
                break;
            /* if type is double, compare double member of union */
            case aat_bln:
                if ((a.atoms[i].val.bln && !b.atoms[i].val.bln) ||
                    (!a.atoms[i].val.bln && b.atoms[i].val.bln))
                    mismatch = true;
                break;
            case aat_qname:
                if (PFqname_eq (a.atoms[i].val.qname, b.atoms[i].val.qname))
                    mismatch = true;
                break;

            /* anything else is actually bogus (e.g. there are no
             * literal nodes */
            default:
            {
                PFinfo (OOPS_WARNING, "literal value that do not make sense");
                mismatch = true;
            } break;
        }
        if (mismatch)
            break;
    }

    return (i == a.count);
}

static bool
eff_schema_eq (PFalg_schema_t schema1, PFalg_schema_t schema2,
            PFarray_t *effmap1, PFarray_t *effmap2)
{
    bool match = false;
    for (unsigned int i = 0; i < schema1.count; i++) {
        for (unsigned int j = 0; j < schema2.count; j++) {
            if (eff_attribute_ (effmap1, schema1.items[i].name) ==
                eff_attribute_ (effmap2, schema2.items[j].name))
                match = true;
        }

        if (!match) return false;
        match = false;
    }

    return true;
}

#if 0
static void
print_eff (PFarray_t *effmap)
{
    if (effmap == NULL) return;
    for (unsigned int i = 0; i < PFarray_last (effmap); i++)
        fprintf (stderr, "\t- %s -> %s\n",
            PFatt_str ((*(eff_map_t*)PFarray_at(effmap, i)).ori_att),
            PFatt_str ((*(eff_map_t*)PFarray_at(effmap, i)).eff_att)
        );
};

static void
print_schema (PFalg_schema_t schema)
{
    fprintf (stderr, "schema:\n");
    for (unsigned int i = 0; i < schema.count; i++) {
        fprintf (stderr, "\t->%s\n", PFatt_str (schema.items[i].name));
    }
}

static void
print_proj (unsigned int count, PFalg_proj_t *proj)
{
    for (unsigned int i = 0; i < count; i++) {
        fprintf (stderr, "old = %s, new = %s\n", PFatt_str (proj[i].old), PFatt_str (proj[i].new));
    }
}
#endif

/* Determine the differences between the original schema and the
 * schema used in the cse plan
 */
static void
eff_schema_patch (PFarray_t *eff_map, PFalg_schema_t schema, PFarray_t *eff_att_map)
{
    PFarray_t *ret = eff_map;

    for (unsigned int i = 0; i < schema.count; i++) {
        PFalg_att_t att = eff_attribute_ (eff_att_map, schema.items[i].name);
            *(eff_map_t *) PFarray_add (ret) =
                (eff_map_t) {
                    .ori_att = schema.items[i].name,
                    .eff_att = att
                };
    }
}

/* modify a schema according to the @a eff_att_map */
static void
apply_schema_patch (PFla_op_t *n, PFalg_schema_t schema, PFarray_t *eff_att_map)
{
    unsigned int j;
    for (unsigned int i = 0; i < schema.count; i++) {
        PFalg_att_t eff_att = eff_attribute_ (eff_att_map, schema.items[i].name);

        /* search through the entire schema to adjust the item */
        for (j = 0; j < n->schema.count; j++) {
            if (schema.items[i].name == n->schema.items[j].name) {
                n->schema.items[i].name = eff_att;
                break;
            }
        }

        if (j >= n->schema.count)
            PFoops (OOPS_FATAL, "Attribute (%s) not found in schema",
                    PFatt_str (schema.items[i].name));
    }
}

/* Apply a patch to a node to be inserted in the CSE plan to adjust
 * its schema to the schemata of the underlying node. */
static void
apply_patch (PFla_op_t *n, PFarray_t *assembly1, PFarray_t *assembly2)
{
    assert (n);

    switch (n->kind)
    {
        case la_serialize_seq:
        {
            n->sem.ser_seq.pos = eff_attribute_ (assembly2, n->sem.ser_seq.pos);
            n->sem.ser_seq.item = eff_attribute_ (assembly2, n->sem.ser_seq.item);
        } break;
        case la_lit_tbl:
        case la_empty_tbl:
        {
        } break;
        case la_attach:
        {
            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                                n->sem.attach.res);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_cross:
        case la_eqjoin:
        case la_semijoin:
        case la_thetajoin:
        {
            /* instead of modifying the attributes use a projection to leave the
             * join as it is */
        } break;
        case la_project:
        {
            for (unsigned int i = 0; i < n->sem.proj.count; i++) {
                n->sem.proj.items[i].old = eff_attribute_ (assembly1, n->sem.proj.items[i].old);
            }
        } break;
        case la_select:
        {
            n->sem.select.att = eff_attribute_ (assembly1,
                                                n->sem.select.att);

            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                                n->sem.select.att);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_disjunion:
        case la_intersect:
        case la_difference:
        {
        } break;
        case la_distinct:
        {
            apply_schema_patch (n, n->schema, assembly1);
        } break;
        case la_fun_1to1:
        {
            for (unsigned int i = 0; i < n->sem.fun_1to1.refs.count; i++) {
                PFalg_att_t att = n->sem.fun_1to1.refs.atts[i];

                n->sem.fun_1to1.refs.atts[i] = eff_attribute_ (
                                    assembly1, att);
            }

            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                            n->sem.fun_1to1.res);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_num_eq:
        case la_num_gt:
        {
            n->sem.binary.att1 = eff_attribute_ (assembly1,
                                                n->sem.binary.att1);
            n->sem.binary.att2 = eff_attribute_ (assembly1,
                                                n->sem.binary.att2);

            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                        n->sem.binary.res, n->sem.binary.att1,
                                        n->sem.binary.att2);

            apply_schema_patch (n, schema, assembly1);

        } break;
        case la_bool_and:
        case la_bool_or:
        {
            n->sem.binary.att1 = eff_attribute_ (assembly1,
                                            n->sem.binary.att1);
            n->sem.binary.att2 = eff_attribute_ (assembly1,
                                            n->sem.binary.att2);

            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                        n->sem.binary.res);
            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_bool_not:
        {
            n->sem.unary.att = eff_attribute_ (assembly1,
                                            n->sem.unary.att);
            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                        n->sem.unary.res);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        {
            n->sem.aggr.att = eff_attribute_ (assembly1,
                                                n->sem.aggr.att);
            n->sem.aggr.part = eff_attribute_ (assembly1,
                                                n->sem.aggr.part);

            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                                n->sem.aggr.att,
                                                n->sem.aggr.part,
                                                n->sem.aggr.res);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_rownum:
        case la_rowrank:
        case la_rank:
        {
            n->sem.sort.part = eff_attribute_ (assembly1,
                                                n->sem.sort.part);

            /* adjust ordering TODO */
            PFord_ordering_t ordering = PFordering ();

            for (unsigned int i = 0; i < PFord_count (n->sem.sort.sortby); i++) {
                PFalg_att_t ordname = PFord_order_col_at (
                                        n->sem.sort.sortby, i);
                bool dir = PFord_order_dir_at (
                                        n->sem.sort.sortby, i);

                ordering = PFord_refine (ordering,
                                        eff_attribute_ (assembly1, ordname),
                                        dir);
            }

            n->sem.sort.sortby = ordering;

            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                                n->sem.sort.res);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_rowid:
        {
            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                                n->sem.rowid.res);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_type:
        {
            assert (!"la_type not yet supported!");
        } break;
        case la_type_assert:
        case la_cast:
        {
            n->sem.type.att = eff_attribute_ (assembly1,
                                            n->sem.type.att);
            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                            n->sem.type.res);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_seqty1:
        {
            assert (!"la_seqty1 not yet supported!");
        } break;
        case la_all:
        {
            assert (!"la_all not yet supported!");
        } break;
        case la_step:
        {
            n->sem.step.iter = eff_attribute_ (assembly2, n->sem.step.iter);
            n->sem.step.item = eff_attribute_ (assembly2, n->sem.step.item);
        } break;
        case la_step_join:
        {
            assert (!"la_step_join not yet supported!");
        } break;
        case la_guide_step:
        {
            assert (!"la_guide_step not yet supported!");
        } break;
        case la_doc_index_join:
        {
            assert (!"la_doc_index_join not yet supported!");
        } break;
        case la_doc_tbl:
        {
            n->sem.doc_tbl.iter = eff_attribute_ (assembly1, n->sem.doc_tbl.iter);
            n->sem.doc_tbl.item = eff_attribute_ (assembly1, n->sem.doc_tbl.item);

            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                            n->sem.doc_tbl.item_res);

            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_doc_access:
        {
            n->sem.doc_access.att = eff_attribute_ (assembly2, n->sem.doc_access.att);

            PFalg_schema_t schema = PFalg_schema_diff (n->schema,
                                            n->sem.doc_access.res);
            apply_schema_patch (n, schema, assembly2);
        } break;
        case la_twig:
        case la_fcns:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
        {
        } break;
        case la_merge_adjacent:
        {
            n->sem.merge_adjacent.iter_in = eff_attribute_ (assembly2,
                                                   n->sem.merge_adjacent.iter_in);
            n->sem.merge_adjacent.pos_in = eff_attribute_ (assembly2,
                                                   n->sem.merge_adjacent.pos_in);
            n->sem.merge_adjacent.item_in = eff_attribute_ (assembly2,
                                                   n->sem.merge_adjacent.item_in);
        } break;
        case la_roots:
        {
            for (unsigned int i = 0; i < n->schema.count; i++) {
                n->schema.items[i].name = eff_attribute_ (assembly1, n->schema.items[i].name);
            }
        } break;
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        {
        } break;
        case la_cond_err:
        {
            PFalg_schema_t schema = n->schema;
            apply_schema_patch (n, schema, assembly1);
        } break;
        case la_nil:
        {
        } break;
        case la_trace:
        {
            assert (!"la_trace not yet supported");
        } break;
        case la_trace_msg:
        {
            assert (!"la_trace_msg not yet supported");
        } break;
        case la_trace_map:
        {
            assert (!"la_trace_map not yet supported");
        } break;
        case la_rec_fix:
        {
            assert (!"la_rec_fix not yet supported");
        } break;
        case la_rec_param:
        {
            assert (!"la_rec_param not yet supported");
        } break;
        case la_rec_arg:
        {
            assert (!"la_rec_arg not yet supported");
        } break;
        case la_rec_base:
        {
            assert (!"la_rec_base not yet supported");
        } break;
        case la_proxy:
        {
            assert (!"la_proxy not yet supported");
        } break;
        case la_proxy_base:
        {
            assert (!"la_proxy_base not yet supported");
        } break;
        case la_cross_mvd:
        {
            assert (!"la_cross_mvd not yet supported");
        } break;
        case la_eqjoin_unq:
        {
            assert (!"la_eqjoin_unq not yet supported");
        } break;
        case la_string_join:
        {
            n->sem.string_join.iter = eff_attribute_ (assembly1,
                                            n->sem.string_join.iter);
            n->sem.string_join.item = eff_attribute_ (assembly1,
                                            n->sem.string_join.item);
            n->sem.string_join.pos = eff_attribute_ (assembly1,
                                            n->sem.string_join.pos);

            n->sem.string_join.iter_sep = eff_attribute_ (assembly2,
                                            n->sem.string_join.iter_sep);
            n->sem.string_join.item_sep = eff_attribute_ (assembly2,
                                            n->sem.string_join.item_sep);
        } break;
        case la_dummy:
        {
        } break;
        default:
        {
            PFoops (OOPS_FATAL, "node (%i) unknown", n->kind);
        } break;
    };
}

static void
resolve_name_conflicts (PFla_op_t *n)
{
    PFalg_att_t old = att_NULL;
    PFalg_att_t new = att_NULL;

    switch (n->kind) {
        case la_serialize_seq:
        case la_empty_tbl:
        case la_lit_tbl:
        {
            /* no new attributes */
            return;
        } break;
        case la_attach:
        {
            old = n->sem.attach.res;
            new = create_unq_name (CSE (L (n)),
                            n->sem.attach.res);
        } break;
        case la_cross:
        case la_eqjoin:
        case la_semijoin:
        case la_thetajoin:
        case la_project:
        case la_select:
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_distinct:
            return;
        case la_fun_1to1:
        {
            old = n->sem.fun_1to1.res;
            new = create_unq_name (CSE (L (n)),
                            old);
            n->sem.fun_1to1.res = new;
        } break;
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        {
            old = n->sem.binary.res;
            new = create_unq_name (CSE (L (n)),
                            old);
            n->sem.binary.res = new;
        } break;
        case la_bool_not:
        {
            old = n->sem.unary.res;
            new = create_unq_name (CSE (L (n)),
                            old);
            n->sem.unary.res = new;
        } break;
        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        {
            old = n->sem.aggr.res;
            new = create_unq_name (CSE (L (n)),
                            old);
            n->sem.aggr.res = new;
        } break;
        case la_rownum:
        case la_rowrank:
        case la_rank:
        {
            old = n->sem.sort.res;
            new = create_unq_name (CSE (L (n)),
                            old);
            n->sem.sort.res = new;
        } break;
        case la_rowid:
        {
            old = n->sem.rowid.res;
            new = create_unq_name (CSE (L (n)),
                            old);
            n->sem.rowid.res = new;
        } break;
        case la_type:
        {
            assert (!"la_type not yet supported");
        } break;
        case la_type_assert:
        case la_cast:
        {
            old = n->sem.type.res;
            new = create_unq_name (CSE (L(n)),
                            old);

            n->sem.type.res = new;
        } break;
        case la_seqty1:
        {
            assert (!"la_seqty1 not yet supported");
        } break;
        case la_all:
        {
            assert (!"la_all not yet supported");
        } break;
        case la_step:
        {
            old = n->sem.step.item_res;
            new = create_unq_name (CSE (L (n)),
                                old);
            n->sem.step.item_res = new;
        } break;
        case la_step_join:
        {
            assert (!"la_step_join not yet supported");
        } break;
        case la_guide_step:
        {
            assert (!"la_guide_step not yet supported");
        } break;
        case la_doc_index_join:
        {
            assert (!"la_doc_index_join not yet supported");
        } break;
        case la_doc_tbl:
        {
            old = n->sem.doc_tbl.item_res;
            new = create_unq_name (CSE (L (n)),
                                old);
            n->sem.doc_tbl.item_res = new;
        } break;
        case la_doc_access:
        {
            old = n->sem.doc_access.res;
            new = create_unq_name (CSE (R (n)),
                                old);
            n->sem.doc_access.res = new;
        } break;
        case la_twig:
        case la_fcns:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
        case la_merge_adjacent:
        case la_roots:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_cond_err:
        case la_nil:
        {
            /* no new attributes */
            return;
        } break;
        case la_trace:
        {
            assert (!"la_trace not yet supported");
        } break;
        case la_trace_msg:
        {
            assert (!"la_trace_msg not yet supported");
        } break;
        case la_trace_map:
        {
            assert (!"la_trace_map not yet supported");
        } break;
        case la_rec_fix:
        {
            assert (!"la_rec_fix not yet supported");
        } break;
        case la_rec_param:
        {
            assert (!"la_rec_param not yet supported");
        } break;
        case la_rec_arg:
        {
            assert (!"la_rec_arg not yet supported");
        } break;
        case la_rec_base:
        {
            assert (!"la_rec_base not yet supported");
        } break;
        case la_proxy:
        {
            assert (!"la_proxy not yet supported");
        } break;
        case la_proxy_base:
        {
            assert (!"la_proxy_base not yet supported");
        } break;
        case la_cross_mvd:
        {
            assert (!"la_cross_mvd not yet supported");
        } break;
        case la_eqjoin_unq:
        {
            assert (!"la_eqjoin_unq not yet supported");
        } break;
        case la_string_join:
        {
            /* TODO */
            return;
        } break;
        case la_dummy:
        {
        } break;
        default:
            PFoops (OOPS_FATAL,
                    "node (%i) not supported", n->kind);
            break;
    }

    if (old != new)
        patch_schema (n, old, new);
}

/**
 * Test if two subexpressions are equal:
 * The subexpression in @a a is a node
 * of the original plan, @a b is a node from
 * the CSE plan.
 *
 * - both nodes must have the same kind,
 * - the node @a must have the same childs
 *   in his CSE plan as node @a b
 *   (In terms of C pointers).
 * - if the nodes carry additional semantic content, the
 *   content of both nodes must match.
 * - there must exist a projection in a child node, to
 *   project the items to the node b.
 */
static bool
subexp_eq (PFla_op_t *a, PFla_op_t *b)
{
    assert (a);
    assert (b);

    /* first we need the original node that
     * caused the insertion */
    /* the original value of will be automatically
     * restored when the function returns */
    b = ORI (b);

    /* shortcut for the trivial case */
    if (a == b)
        return true;

    /* see if a node kind is identical */
    if (a->kind != b->kind)
        return false;

    /* both nodes must have identical children in terms of
     * their underlying cse plans (C pointers) */
    if (!((CSE (L (a)) == CSE (L (b))) &&
          (CSE (R (a)) == CSE (R (b))))) {
        return false;
    }


    /* check if at least the number of schema items
     * is the same */
    if (a->schema.count != b->schema.count)
        return false;

    switch (a->kind) {
        case la_serialize_seq:
        {
            PFla_op_t *achild = R (a);
            PFla_op_t *bchild = R (b);

            PFalg_att_t apos = eff_attribute (achild,
                                    a->sem.ser_seq.pos);
            PFalg_att_t bpos = eff_attribute (bchild,
                                    b->sem.ser_seq.pos);

            PFalg_att_t aitem = eff_attribute (achild,
                                    a->sem.ser_seq.item);
            PFalg_att_t bitem = eff_attribute (bchild,
                                    b->sem.ser_seq.item);

            if (! ((apos == bpos) && (aitem == bitem)))
                return false;

            return true;
        } break;
        case la_lit_tbl:
        case la_empty_tbl:
        {
            if (a->sem.lit_tbl.count != b->sem.lit_tbl.count)
                return false;

            /* check only the equivalence of tuples and
             * types */
            for (unsigned int i = 0; i < a->schema.count; i++) {
                if ( !(a->schema.items[i].type ==
                        b->schema.items[i].type))
                    return false;
            }

            /* the items must have the same ordering */
            for (unsigned int i = 0; i < a->sem.lit_tbl.count; i++) {
                if (!(tuple_eq (a->sem.lit_tbl.tuples[i],
                            b->sem.lit_tbl.tuples[i])))
                    return false;
            }

            return true;
        } break;
        case la_attach:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);
            if ( !PFalg_atom_comparable (a->sem.attach.value, b->sem.attach.value))
                return false;

            if ( PFalg_atom_cmp (a->sem.attach.value, b->sem.attach.value) != 0)
                return false;

            PFalg_schema_t schema1 = PFalg_schema_diff (a->schema, a->sem.attach.res);
            PFalg_schema_t schema2 = PFalg_schema_diff (b->schema, b->sem.attach.res);

            if (!eff_schema_eq (schema1, schema2, EFF (achild), EFF (bchild)))
                return false;

            return true;
        } break;
        case la_cross:
        {
            return true;
        } break;
        case la_eqjoin:
        case la_semijoin:
        {
            PFla_op_t *alchild = L (a);
            PFla_op_t *archild = R (a);

            PFla_op_t *blchild = L (b);
            PFla_op_t *brchild = R (b);

            PFalg_att_t aatt1 = eff_attribute (alchild, a->sem.eqjoin.att1);
            PFalg_att_t aatt2 = eff_attribute (archild, a->sem.eqjoin.att2);
            PFalg_att_t batt1 = eff_attribute (blchild, b->sem.eqjoin.att1);
            PFalg_att_t batt2 = eff_attribute (brchild, b->sem.eqjoin.att2);

            if (!((aatt1 == batt1) && (aatt2 == batt2)))
                return false;


            return true;
        } break;
        case la_thetajoin:
        {
            PFla_op_t *alchild = L (a);
            PFla_op_t *archild = R (a);

            PFla_op_t *blchild = L (b);
            PFla_op_t *brchild = R (b);

            if (b->sem.thetajoin.count != a->sem.thetajoin.count)
                return false;

            for (unsigned int i = 0; i < a->sem.thetajoin.count; i++) {
                PFalg_att_t aleft = eff_attribute (alchild,
                                        a->sem.thetajoin.pred[i].left);
                PFalg_att_t bleft = eff_attribute (blchild,
                                        b->sem.thetajoin.pred[i].left);

                PFalg_att_t aright = eff_attribute (archild,
                                        a->sem.thetajoin.pred[i].right);
                PFalg_att_t bright = eff_attribute (brchild,
                                        b->sem.thetajoin.pred[i].right);

                if ( !((aleft == bleft) && (aright == bright) &&
                        (a->sem.thetajoin.pred[i].comp == b->sem.thetajoin.pred[i].comp)))
                        return false;
            }

            return true;
        } break;
        case la_project:
        {
            PFla_op_t *achild = L(a);
            PFla_op_t *bchild = L(b);

            if (a->sem.proj.count != b->sem.proj.count)
                return false;

            for (unsigned int i = 0; i < a->sem.proj.count; i++) {
                PFalg_att_t aproj = eff_attribute (achild,
                                    a->sem.proj.items[i].old);
                PFalg_att_t bproj = eff_attribute (bchild,
                                    b->sem.proj.items[i].old);
                if (aproj != bproj)
                    return false;
            }


            return true;
        } break;
        case la_select:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_att_t aatt = eff_attribute (achild, a->sem.select.att);
            PFalg_att_t batt = eff_attribute (bchild, b->sem.select.att);

            if ( !(aatt == batt))
                return false;

            PFalg_schema_t schema1 = a->schema;
            PFalg_schema_t schema2 = b->schema;

            if (!eff_schema_eq (schema1, schema2, EFF (achild), EFF (bchild)))
                return false;



            return true;
        } break;
        case la_disjunion:
        case la_intersect:
        case la_difference:
        {
            return true;
        } break;
        case la_distinct:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            if (!eff_schema_eq (a->schema, b->schema, EFF (achild), EFF (bchild)))
                return false;

            return true;
        } break;
        case la_fun_1to1:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            if (!(a->sem.fun_1to1.refs.count == b->sem.fun_1to1.refs.count))
                return false;

            for (unsigned int i = 0; i < a->sem.fun_1to1.refs.count; i++) {
                PFalg_att_t aref = eff_attribute (achild, a->sem.fun_1to1.refs.atts[i]);
                PFalg_att_t bref = eff_attribute (bchild, b->sem.fun_1to1.refs.atts[i]);

                if (!(aref == bref))
                    return false;
            }

            PFalg_schema_t schema1 = PFalg_schema_diff (a->schema, a->sem.fun_1to1.res);
            PFalg_schema_t schema2 = PFalg_schema_diff (b->schema, b->sem.fun_1to1.res);

            if (!eff_schema_eq (schema1, schema2, EFF (achild), EFF (bchild)))
                return false;

            return true;
        } break;
        case la_num_gt:
        case la_num_eq:
        case la_bool_and:
        case la_bool_or:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_att_t aatt1 = eff_attribute (achild, a->sem.binary.att1);
            PFalg_att_t aatt2 = eff_attribute (achild, a->sem.binary.att2);
            PFalg_att_t batt1 = eff_attribute (bchild, b->sem.binary.att1);
            PFalg_att_t batt2 = eff_attribute (bchild, b->sem.binary.att2);

            if (!((aatt1 == batt1) && (aatt2 == batt2)))
                return false;

            return true;
        } break;
        case la_bool_not:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_att_t aatt = eff_attribute (achild, a->sem.unary.att);
            PFalg_att_t batt = eff_attribute (bchild, b->sem.unary.att);

            if (!(aatt == batt))
                return false;

            return true;
        } break;
        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_att_t aatt = eff_attribute (achild, a->sem.aggr.att);
            PFalg_att_t batt = eff_attribute (bchild, b->sem.aggr.att);

            PFalg_att_t apart = eff_attribute (achild, a->sem.aggr.part);
            PFalg_att_t bpart = eff_attribute (bchild, b->sem.aggr.part);

            if (!((aatt == batt) && (apart == bpart)))
                return false;

            return true;
        } break;
        case la_rownum:
        case la_rowrank:
        case la_rank:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            /* check the schema without the created attname */
            PFalg_schema_t schema1 = PFalg_schema_diff (a->schema, a->sem.sort.res);
            PFalg_schema_t schema2 = PFalg_schema_diff (b->schema, b->sem.sort.res);

            /* check if the number of ordering items is the same */
            if (PFord_count (a->sem.sort.sortby) != PFord_count (b->sem.sort.sortby))
                return false;


            for (unsigned int i = 0; i < PFord_count (a->sem.sort.sortby); i++) {
                PFalg_att_t order1 = eff_attribute (achild, PFord_order_col_at (a->sem.sort.sortby, i));
                PFalg_att_t order2 = eff_attribute (bchild, PFord_order_col_at (b->sem.sort.sortby, i));

                if (!((order1 == order2) &&
                        PFord_order_dir_at (a->sem.sort.sortby, i) ==
                            PFord_order_dir_at (b->sem.sort.sortby, i)))
                    return false;
            }

            if (!(eff_attribute (achild, a->sem.sort.part)
                    == eff_attribute (bchild, b->sem.sort.part)))
                    return false;

            if (!eff_schema_eq (schema1, schema2, EFF (achild), EFF (bchild)))
                return false;

            return true;
        } break;
        case la_rowid:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_schema_t schema1 = PFalg_schema_diff (a->schema, a->sem.rowid.res);
            PFalg_schema_t schema2 = PFalg_schema_diff (b->schema, b->sem.rowid.res);

            if (!eff_schema_eq (schema1, schema2, EFF (achild), EFF (bchild)))
                return false;

            return true;
        } break;
        case la_type:
        {
            assert (!"la_type not yet supported");
        } break;
        case la_type_assert:
        case la_cast:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_att_t aatt = eff_attribute (achild, a->sem.type.att);
            PFalg_att_t batt = eff_attribute (bchild, b->sem.type.att);

            if (!((aatt == batt) && (a->sem.type.ty == b->sem.type.ty)))
                return false;

            PFalg_schema_t schema1 = PFalg_schema_diff (a->schema, a->sem.type.res);
            PFalg_schema_t schema2 = PFalg_schema_diff (b->schema, b->sem.type.res);

            if (!eff_schema_eq (schema1, schema2, EFF (achild), EFF (bchild)))
                return false;

            return true;
        } break;
        case la_seqty1:
        {
            assert (!"la_seqty1 not yet supported");
        } break;
        case la_all:
        {
            assert (!"la_all not yet supported");
        } break;
        case la_step:
        {
            PFla_op_t *achild = R(a);
            PFla_op_t *bchild = R(b);

            /* check only the attributes coming from
             * the underlying children */
            PFalg_att_t aiter = eff_attribute (achild,
                a->sem.step.iter);
            PFalg_att_t aitem = eff_attribute (achild,
                a->sem.step.item);

            PFalg_att_t biter = eff_attribute (bchild,
                b->sem.step.iter);
            PFalg_att_t bitem = eff_attribute (bchild,
                b->sem.step.item);

            if (!((aiter == biter) &&
                (aitem == bitem)))
                return false;

            if (!(a->sem.step.axis == b->sem.step.axis))
                return false;

            if (!PFty_eq (a->sem.step.ty, b->sem.step.ty))
                return false;


            return true;
        } break;
        case la_step_join:
        {
            assert (!"la_step_join not yet supported");
        } break;
        case la_guide_step:
        {
            assert (!"la_guide_step not yet supported");
        } break;
        case la_doc_index_join:
        {
            assert (!"la_doc_index_join not yet supported");
        } break;
        case la_doc_tbl:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_att_t aiter = eff_attribute (achild,
                a->sem.doc_tbl.iter);
            PFalg_att_t aitem = eff_attribute (achild,
                a->sem.doc_tbl.item);

            PFalg_att_t biter = eff_attribute (bchild,
                b->sem.doc_tbl.iter);
            PFalg_att_t bitem = eff_attribute (bchild,
                b->sem.doc_tbl.item);

            if (!((aiter == biter) &&
                  (aitem == bitem)))
                  return false;

            return true;
        } break;
        case la_doc_access:
        {
            PFla_op_t *achild = R (a);
            PFla_op_t *bchild = R (b);

            PFalg_att_t aatt = eff_attribute (achild, a->sem.doc_access.att);
            PFalg_att_t batt = eff_attribute (bchild, b->sem.doc_access.att);
            PFalg_att_t adoc_col = a->sem.doc_access.doc_col;
            PFalg_att_t bdoc_col = b->sem.doc_access.doc_col;

            if (!((aatt == batt) && (adoc_col == bdoc_col)))
                return false;


            return true;
        } break;
        case la_twig:
        case la_fcns:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
        {
            return false;
        } break;
        case la_merge_adjacent:
        {
            PFla_op_t *achild = R (a);
            PFla_op_t *bchild = R (b);

            PFalg_att_t aiter_in = eff_attribute (achild,
                                                a->sem.merge_adjacent.iter_in);
            PFalg_att_t biter_in = eff_attribute (bchild,
                                                b->sem.merge_adjacent.iter_in);
            PFalg_att_t apos_in = eff_attribute (achild,
                                                a->sem.merge_adjacent.pos_in);
            PFalg_att_t bpos_in = eff_attribute (bchild,
                                                b->sem.merge_adjacent.pos_in);
            PFalg_att_t aitem_in = eff_attribute (achild,
                                                b->sem.merge_adjacent.item_in);
            PFalg_att_t bitem_in = eff_attribute (bchild,
                                                b->sem.merge_adjacent.item_in);

            if ( !((aiter_in == biter_in) && (apos_in == bpos_in) &&
                    (aitem_in == bitem_in)) )
                return false;

            return true;
        } break;
        case la_roots:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_schema_t schema1 = a->schema;
            PFalg_schema_t schema2 = b->schema;
            if (!eff_schema_eq (schema1, schema2, EFF (achild), EFF (bchild)))
                return false;


            return true;
        } break;
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        {
            return true;
        } break;
        case la_cond_err:
        {
            PFla_op_t *achild = L (a);
            PFla_op_t *bchild = L (b);

            PFalg_att_t aatt = eff_attribute (achild,
                                    a->sem.err.att);
            PFalg_att_t batt = eff_attribute (bchild,
                                    b->sem.err.att);

            if (!(aatt == batt))
                return false;

            if (!(strcmp (a->sem.err.str, b->sem.err.str) == 0))
                return false;

            return true;
        } break;
        case la_nil:
        {
            return true;
        } break;
        case la_trace:
        {
            assert (!"la_trace not yet supported!");
        } break;
        case la_trace_msg:
        {
            assert (!"la_trace_msg not yet supported!");
        } break;
        case la_trace_map:
        {
            assert (!"la_trace_map not yet supported!");
        } break;
        case la_rec_fix:
        {
            assert (!"la_rec_fix not yet supported!");
        } break;
        case la_rec_param:
        {
            assert (!"la_rec_param not yet supported!");
        } break;
        case la_rec_arg:
        {
            assert (!"la_rec_arg not yet supported!");
        } break;
        case la_rec_base:
        {
            assert (!"la_rec_base not yet supported!");
        } break;
        case la_proxy:
        {
            assert (!"la_proxy not yet supported!");
        } break;
        case la_cross_mvd:
        {
            assert (!"la_cross_mvd not yet supported!");
        } break;
        case la_eqjoin_unq:

        {
            assert (!"la_eqjoin_unq not yet supported!");
        } break;
        case la_string_join:
        {
            PFla_op_t *alchild = L (a);
            PFla_op_t *archild = R (a);
            PFla_op_t *blchild = L (b);
            PFla_op_t *brchild = R (a);

            PFalg_att_t aiter = eff_attribute (alchild,
                                            a->sem.string_join.iter);
            PFalg_att_t biter = eff_attribute (blchild,
                                            b->sem.string_join.iter);
            PFalg_att_t aitem = eff_attribute (alchild,
                                            a->sem.string_join.item);
            PFalg_att_t bitem = eff_attribute (blchild,
                                            b->sem.string_join.item);
            PFalg_att_t apos = eff_attribute (alchild,
                                            a->sem.string_join.pos);
            PFalg_att_t bpos = eff_attribute (blchild,
                                            b->sem.string_join.pos);

            PFalg_att_t aiter_sep = eff_attribute (archild,
                                        a->sem.string_join.iter_sep);
            PFalg_att_t biter_sep = eff_attribute (brchild,
                                        b->sem.string_join.iter_sep);
            PFalg_att_t aitem_sep = eff_attribute (archild,
                                        a->sem.string_join.item_sep);
            PFalg_att_t bitem_sep = eff_attribute (brchild,
                                        b->sem.string_join.item_sep);

            if (!((aiter == biter) && (aitem == bitem) && (apos == bpos)
                    && (aiter_sep == biter_sep) && (aitem_sep == bitem_sep)))
                return false;

            return true;
        } break;
        case la_dummy:
        {
            assert (!"la_dummy not yet supported");
        } break;
        default:
        {
            PFoops (OOPS_FATAL, "node (%i) not supported", a->kind);
        } break;
    }

    assert (!"this should never be reached (subexpression bchild"
           "equality)");

    return false; /* satisfy picky compilers */
}

#if 0
static PFarray_t*
invert_effmap (PFarray_t *effmap)
{
    PFarray_t *ret = PFarray (sizeof (eff_map_t));
    for (unsigned int i = 0; i < PFarray_last (effmap); i++) {
        *(eff_map_t *) PFarray_add (ret) =
            (eff_map_t) {
                .ori_att = (*(eff_map_t *) PFarray_at (effmap, i)).eff_att,
                .eff_att = (*(eff_map_t *) PFarray_at (effmap, i)).ori_att
            };
    }
    return ret;
}
#endif

/* make difference between original and cse operator
 * explicit
 */
static PFarray_t*
patch_projections (PFla_op_t *a, PFla_op_t *b)
{
    PFarray_t *effmap;

    if (a->kind != b->kind)
        PFoops (OOPS_FATAL, "node kinds are not equal");

    effmap = PFarray (sizeof (eff_map_t));

    switch (a->kind) {
        case la_serialize_seq:
        {
            return NULL;
        } break;
        case la_lit_tbl:
        case la_empty_tbl:
        {
            for (unsigned int i = 0; i < a->schema.count; i++) {
               *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->schema.items[i].name,
                    .eff_att = b->schema.items[i].name
                };
            }
        } break;
        case la_attach:
        {
            PFla_op_t *child = L (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.attach.res,
                    .eff_att = b->sem.attach.res
                };

            PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.attach.res);

            eff_schema_patch (effmap, schema, EFF (child));

        } break;
        case la_cross:
        case la_eqjoin:
        case la_semijoin:
        case la_thetajoin:
        {
            PFla_op_t *lchild = L (a);
            PFla_op_t *rchild = R (a);

            bool found = false;
            PFalg_att_t fatt = att_NULL;

            for (unsigned int i = 0; i < a->schema.count; i++) {

                if (eff_attribute_ (EFF (lchild), a->schema.items[i].name) != att_NULL) {
                    fatt = eff_attribute_ (EFF (lchild), a->schema.items[i].name);
                    found = true;
                }

                if (!found)
                    if (eff_attribute_ (EFF (rchild), a->schema.items[i].name) != att_NULL) {
                        fatt = eff_attribute_ (EFF (rchild), a->schema.items[i].name);
                        found = true;
                    }

                if (fatt == att_NULL)
                    assert (!"this should not happen");

                *(eff_map_t *) PFarray_add (effmap) =
                    (eff_map_t)
                    {
                        .ori_att = a->schema.items[i].name,
                        .eff_att = fatt
                    };

                fatt = att_NULL;
                found = false;
            }
        } break;
        case la_project:
        {
           for(unsigned int i = 0; i < a->sem.proj.count; i++) {
                *(eff_map_t *) PFarray_add (effmap) =
                    (eff_map_t)
                    {
                        .ori_att = a->sem.proj.items[i].new,
                        .eff_att = b->sem.proj.items[i].new
                    };
           }
        } break;
        case la_select:
        {
            PFla_op_t *child = L (a);
            PFalg_schema_t schema = a->schema;

            eff_schema_patch (effmap, schema, EFF (child));

        } break;
        case la_disjunion:
        case la_intersect:
        case la_difference:
        {
            for (unsigned int i = 0; i < a->schema.count; i++) {
                *(eff_map_t *) PFarray_add (effmap) =
                    (eff_map_t)
                    {
                        .ori_att = a->schema.items[i].name,
                        .eff_att = b->schema.items[i].name
                    };
            }
        } break;
        case la_distinct:
        {
            PFla_op_t *child = L (a);

            eff_schema_patch (effmap, a->schema, EFF (child));
        } break;
        case la_fun_1to1:
        {
            PFla_op_t *child = L (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.fun_1to1.res,
                    .eff_att = b->sem.fun_1to1.res
                };

            PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.fun_1to1.res);

            eff_schema_patch (effmap, schema, EFF (child));

        } break;
        case la_num_eq:
        case la_num_gt:
        {
            PFla_op_t *child = L (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.binary.res,
                    .eff_att = b->sem.binary.res
                };

            PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.binary.res);

            eff_schema_patch (effmap, schema, EFF (child));

        } break;
        case la_bool_and:
        case la_bool_or:
        {
            PFla_op_t *child = L (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.binary.res,
                    .eff_att = b->sem.binary.res
                };

           PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.binary.res);

           eff_schema_patch (effmap, schema, EFF (child));
        } break;
        case la_bool_not:
        {
            PFla_op_t *child = L (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.unary.res,
                    .eff_att = b->sem.unary.res
                };

           PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.unary.res);

           eff_schema_patch (effmap, schema, EFF (child));
        } break;
        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        {
            PFla_op_t *child = L (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.aggr.res,
                    .eff_att = b->sem.aggr.res
                };

            PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.aggr.res);

            eff_schema_patch (effmap, schema, EFF (child));

        } break;
        case la_rownum:
        case la_rowrank:
        case la_rank:
        {
            PFla_op_t *child = L(a);
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.sort.res,
                    .eff_att = b->sem.sort.res
                };

            PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.sort.res);

            eff_schema_patch (effmap, schema, EFF(child));
        } break;
        case la_rowid:
        {

            PFla_op_t *child = L (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.rowid.res,
                    .eff_att = b->sem.rowid.res
                };

            PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.rowid.res);

            eff_schema_patch (effmap, schema, EFF(child));

        } break;
        case la_type:
        {
            assert (!"la_type not yet supported");
        } break;
        case la_type_assert:
        case la_cast:
        {
            PFla_op_t *child = L (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.type.res,
                    .eff_att = b->sem.type.res
                };

            PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.type.res);

            eff_schema_patch (effmap, schema, EFF (child));

        } break;
        case la_seqty1:
        {
            assert (!"la_seqty1 not yet supported");
        } break;
        case la_all:
        {
            assert (!"la_all not yet supported");
        } break;
        case la_step:
        {
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.step.iter,
                    .eff_att = b->sem.step.iter
                };
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.step.item_res,
                    .eff_att = b->sem.step.item_res
                };

        } break;
        case la_step_join:
        {
            assert (!"la_step_join not yet supported");
        } break;
        case la_guide_step:
        {
            assert (!"la_guide_step not yet supported");
        } break;
        case la_doc_index_join:
        {
            assert (!"la_doc_index_join not yet supported");
        } break;

        case la_doc_tbl:
        {
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.doc_tbl.iter,
                    .eff_att = b->sem.doc_tbl.iter
                };
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.doc_tbl.item_res,
                    .eff_att = b->sem.doc_tbl.item_res
                };
        } break;
        case la_doc_access:
        {
            PFla_op_t *child = R (a);

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.doc_access.res,
                    .eff_att = b->sem.doc_access.res
                };

            PFalg_schema_t schema = PFalg_schema_diff (a->schema, a->sem.doc_access.res);


            eff_schema_patch (effmap, schema, EFF (child));

        } break;
        case la_twig:
        {
            *(eff_map_t *)PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.iter_item.iter,
                    .eff_att = b->sem.iter_item.iter
                };
            *(eff_map_t *)PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.iter_item.item,
                    .eff_att = b->sem.iter_item.item
                };
        } break;
        case la_fcns:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
        {
            return NULL;
        } break;
        case la_merge_adjacent:
        {
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.merge_adjacent.iter_res,
                    .eff_att = b->sem.merge_adjacent.iter_res
                };

            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.merge_adjacent.pos_res,
                    .eff_att = b->sem.merge_adjacent.pos_res
                };
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.merge_adjacent.item_res,
                    .eff_att = b->sem.merge_adjacent.item_res
                };
        } break;
        case la_roots:
        {
            PFla_op_t *child = L (a);

            PFalg_schema_t schema = a->schema;

            eff_schema_patch (effmap, schema, EFF(child));
        } break;
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        {
            return NULL;
        } break;
        case la_cond_err:
        {
            PFla_op_t *child = L (a);

            PFalg_schema_t schema = a->schema;

            eff_schema_patch (effmap, schema, EFF (child));

        } break;
        case la_nil:
        {
            return NULL;
        } break;
        case la_trace:
        {
            assert (!"la_trace not yet supported");
        } break;
        case la_trace_msg:
        {
            assert (!"la_trace:msg not yet supported");
        } break;
        case la_trace_map:
        {
            assert (!"la_trace_map not yet supported");
        } break;
        case la_rec_fix:
        {
            assert (!"la_rec_fix not yet supported");
        } break;
        case la_rec_param:
        {
            assert (!"la_rec_param not yet supported");
        } break;
        case la_rec_arg:
        {
            assert (!"la_rec_arg not yet supported");
        } break;
        case la_rec_base:
        {
            assert (!"la_rec_base not yet supported");
        } break;
        case la_proxy:
        {
            assert (!"la_proxy not yet supported");
        } break;
        case la_proxy_base:
        {
            assert (!"la_proxy_base not yet supported");
        } break;
        case la_cross_mvd:
        {
            assert (!"la_cross_mvd not yet supported");
        } break;
        case la_eqjoin_unq:
        {
            assert (!"la_eq_join_unq not yet supported");
        } break;
        case la_string_join:
        {
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.string_join.iter_res,
                    .eff_att = b->sem.string_join.iter_res
                };
            *(eff_map_t *) PFarray_add (effmap) =
                (eff_map_t)
                {
                    .ori_att = a->sem.string_join.item_res,
                    .eff_att = b->sem.string_join.item_res
                };
        } break;
        case la_dummy:
        {
            assert (!"no dummy operator should be there");
        } break;
        default:
        {
            PFoops (OOPS_FATAL, "node (%i) not supported\n", a->kind);
        } break;
    }

    return effmap;
}

static PFla_op_t *
new_csenode (PFla_op_t *clone, PFla_op_t *left, PFla_op_t *right)
{

    switch (clone->kind) {
        case la_disjunion:
        case la_intersect:
        case la_difference:
        case la_semijoin:
        case la_thetajoin:
        case la_cross:
        case la_eqjoin:
        {
               PFarray_t *patch1 = EFF (left);
               PFarray_t *patch2 = EFF (right);

               PFalg_proj_t *proj1 = (PFalg_proj_t *) PFmalloc (
                            left->schema.count * sizeof (PFalg_proj_t));
               PFalg_proj_t *proj2 = (PFalg_proj_t *) PFmalloc (
                            right->schema.count * sizeof (PFalg_proj_t));

               bool differ = false;
               for (unsigned int i = 0; i < left->schema.count; i++) {
                   PFalg_att_t new = left->schema.items[i].name;
                   PFalg_att_t old = eff_attribute_ (patch1, left->schema.items[i].name);
                   if (new != old) differ = true;
                   proj1[i] = PFalg_proj (new, old);
               }

               for (unsigned int i = 0; i < right->schema.count; i++) {
                   PFalg_att_t new = right->schema.items[i].name;
                   PFalg_att_t old = eff_attribute_ (patch2, right->schema.items[i].name);
                   if (new != old) differ = true;
                   proj2[i] = PFalg_proj (new, old);
              }


               PFla_op_t *project1 = PFla_project_ (CSE (left), left->schema.count, proj1);
               PFla_op_t *project2 = PFla_project_ (CSE (right), right->schema.count, proj2);

               if (differ)
                   return duplicate (clone, project1, project2);
               return duplicate (clone, CSE (L (clone)), CSE (R (clone)));
        } break;
        default:
        {
            return duplicate (clone, CSE (L (clone)), CSE (R (clone)));
        } break;
    }

}


static PFla_op_t*
la_cse (PFla_op_t *n)
{
    PFarray_t *a = NULL;

    assert (n);

    /* descend only once */
    if (SEEN (n))
        return n;

    /* bottom up traversal to construct the
     * DAG from the logical algebra plan
     */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD &&
            n->child[i]; i++) {
        n->child[i] = la_cse (n->child[i]);
    }

    /* at this point every child of n is linked to a node
     * in the CSE plan */

    /*
     * Fetch the subexpressions for this node kind that
     * we encountered so far (maintain one array for each
     * node type). If we have not yet seen any
     * subexpressions of that kind, we will have to
     * initialize the respective array first.
     */
    a = *(PFarray_t **) PFarray_at (subexps, n->kind);

    if (!a)
        *(PFarray_t **) PFarray_at (subexps, n->kind)
            = a = PFarray (sizeof (PFla_op_t *));


    PFla_op_t *temp = NULL;

    /* see if we already saw this subexpression */
    for (unsigned int i = 0; i < PFarray_last (a); i++) {
        if (subexp_eq (n, *(PFla_op_t **) PFarray_at (a, i))) {
            temp = *(PFla_op_t **) PFarray_at (a, i);
        }
    }

    /* check if we already saw the subexpression */
    /* if not add it to the seen expressions */
    if (!temp) {
        PFla_op_t *clone = duplicate (n, L (n), R (n));

        PFarray_t *assembly1 = NULL;
        PFarray_t *assembly2 = NULL;

        assembly1 = EFF (L (n));
        assembly2 = EFF (R (n));

        resolve_name_conflicts (clone);
        apply_patch (clone, assembly1, assembly2);

        *(PFla_op_t **) PFarray_add (a) =
                temp = new_csenode (clone, L (n), R (n));

        /* mark n as the original node caused the
         * insertion */
        INORI (temp, n);

    }

    /* map n to the node in the cse plan */
    INCSE (n, temp);

    /* patch the attribute list */
    PFarray_t *effmap = patch_projections (n, temp);

    if (effmap) {
        INEFF (n, effmap);
    }

    SEEN (n) = true;

    return n;
}

/**
 * Remove all dummy operator from algebra plan.
 * We spapped this out from CSE to reduce
 * complexity.
 */
static PFla_op_t*
remove_dummies (PFla_op_t *n)
{
    assert (n);

    /* descend only once */
    if (SEEN (n))
        return n;

    /* top-down traversal to remove the dummies */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD &&
            n->child[i]; i++) {
        if ( n->child[i]->kind == la_dummy) {
            /* substitute each dummy operator with
             * its left child */
            n->child[i] = L(n->child[i]);
        }
    }

    SEEN (n) = true;
    return n;
}



/**
 * Eliminate common subexpressions in logical algebra tree and
 * convert the expression @em tree into a @em DAG.  (Actually,
 * the input often is already a tree due to the way it was built
 * in core2alg.brg.  This function will detect @em any common
 * subexpression, though.)
 *
 * @param n logical algebra tree
 * @return the equivalent of @a n, with common subexpressions
 *         translated into @em sharing in a DAG.
 */
PFla_op_t *
PFalgopt_cse (PFla_op_t *n)
{
    cse_map = PFarray (sizeof (ori_cse_map_t));
    eff_map = PFarray (sizeof (ori_eff_map_t));
    ori_map = PFarray (sizeof (cse_ori_map_t));

    subexps = PFarray (sizeof (PFarray_t *));

    PFla_op_t *res = NULL;

    /* first remove the dummies */
    res = remove_dummies (n);
    PFla_dag_reset (res);
    res = la_cse (res);
    PFla_dag_reset (res);

    /* search the root in the cse plan
     * and return the cse root */
    res = CSE (res);

    return res;
}

/* vim:set shiftwidth=4 expandtab: */
