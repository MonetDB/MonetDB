/**
 * @file
 *
 * Inference of domain property of logical algebra expressions.
 *
 * AND
 *
 * Inference of domain property of all native type columns
 * of logical algebra expressions. (Here we avoid the domain
 * property inference for all columns that have an XQuery type as
 * a large number of optimizations use the domain property only
 * for equi-join rewrites. This restricted variant is cheaper to infer
 * and to use -- in PFprop_subdom -- while still providing enough
 * information to optimize.)
 *
 * We use abstract domain identifiers (implemented as dom_t structs)
 * that stand for the value domains of relational columns.  We
 * introduce a new domain identifier whenever the active value
 * domain may be changed by an operator.  Some operators guarantee
 * the _inclusion_ or _disjointness_ of involved domains.  We
 * record those in the @subdoms@ and @disjdoms@ fields of the
 * #PFprop_t property container.  Subdomain relationships will
 * also be printed in the dot debugging output.  Both aspects will
 * be printed in the XML debugging output.
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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include pf_config.h first! */
#include "pf_config.h"
#include "pathfinder.h"
#include <assert.h>

#include "properties.h"
#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/**
 * Return domain of column @a col stored
 * in property container @a prop.
 */
dom_t *
PFprop_dom (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    if (!prop->domains) return NULL;

    for (unsigned int i = 0; i < PFarray_last (prop->domains); i++)
        if (col == ((dom_pair_t *) PFarray_at (prop->domains, i))->col)
            return ((dom_pair_t *) PFarray_at (prop->domains, i))->dom;

    return NULL;
}

/**
 * Return domain of column @a col in the domains of the
 * left child node (stored in property container @a prop)
 */
dom_t *
PFprop_dom_left (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    if (!prop->l_domains) return NULL;

    for (unsigned int i = 0; i < PFarray_last (prop->l_domains); i++)
        if (col == ((dom_pair_t *) PFarray_at (prop->l_domains, i))->col)
            return ((dom_pair_t *) PFarray_at (prop->l_domains, i))->dom;

    return NULL;
}

/**
 * Return domain of column @a col in the domains of the
 * right child nod (stored in property container @a prop)
 */
dom_t *
PFprop_dom_right (const PFprop_t *prop, PFalg_col_t col)
{
    assert (prop);
    if (!prop->r_domains) return NULL;

    for (unsigned int i = 0; i < PFarray_last (prop->r_domains); i++)
        if (col == ((dom_pair_t *) PFarray_at (prop->r_domains, i))->col)
            return ((dom_pair_t *) PFarray_at (prop->r_domains, i))->dom;

    return NULL;
}

/**
 * Writes domain represented by @a domain to character array @a f.
 */
void
PFprop_write_domain (PFarray_t *f, dom_t *domain)
{
    PFarray_printf (f, "%i", domain->id);
}

/**
 * Helper function for PFprop_write_dom_rel_dot
 *
 * Checks if domain at position @a pos is already printed.
 */
static bool
check_occurrence (PFarray_t *subdoms, unsigned int pos)
{
    dom_t *dom = ((subdom_t *) PFarray_at (subdoms, pos))->dom;

    for (unsigned int i = 0; i < PFarray_last (subdoms); i++)
        if (dom == ((subdom_t *) PFarray_at (subdoms, i))->subdom)
            return true;

    for (unsigned int i = 0; i < pos; i++)
        if (dom == ((subdom_t *) PFarray_at (subdoms, i))->dom)
            return true;

    return false;
}

/**
 * Write domain-subdomain relationships of property container @a prop
 * to in AT&T dot notation to character array @a f.
 */
void
PFprop_write_dom_rel_dot (PFarray_t *f, const PFprop_t *prop, int id)
{
    dom_t *dom, *subdom;

    assert (prop);
    if (prop->subdoms) {
        PFarray_printf (f,
                        "dr_header%i [label=\"domain -> "
                        "subdomain\\nrelationships (# = %i)\"];\n"
                        "node%i_1 -> dr_header%i [dir=forward,style=invis];\n",
                        id, PFarray_last (prop->subdoms), id, id);

        for (unsigned int i = 0; i < PFarray_last (prop->subdoms); i++) {
            dom = ((subdom_t *) PFarray_at (prop->subdoms, i))->dom;
            subdom = ((subdom_t *) PFarray_at (prop->subdoms, i))->subdom;

            if (!check_occurrence (prop->subdoms, i))
                PFarray_printf (
                    f,
                    "dom_rel%i_%i [label=\"%i\"];\n"
                    "dr_header%i -> dom_rel%i_%i [dir=forward];\n",
                    id, dom->id, dom->id, id, id, dom->id);

            PFarray_printf (
                f,
                "dom_rel%i_%i [label=\"%i\"];\n"
                "dom_rel%i_%i -> dom_rel%i_%i [dir=forward];\n",
                id, subdom->id, subdom->id, id, dom->id, id, subdom->id);
        }
    }
}

/**
 * Write domain-subdomain relationships of property container @a prop
 * to in XML notation to character array @a f.
 */
void
PFprop_write_dom_rel_xml (PFarray_t *f, const PFprop_t *prop)
{
    dom_t *a, *b;

    assert (prop);

    if (prop->subdoms) {
        PFarray_printf (f,
                        "  <dom_subdom_relationship count=\"%i\">\n",
                        PFarray_last (prop->subdoms));

        for (unsigned int i = 0; i < PFarray_last (prop->subdoms); i++) {
            a = ((subdom_t *) PFarray_at (prop->subdoms, i))->dom;
            b = ((subdom_t *) PFarray_at (prop->subdoms, i))->subdom;

            PFarray_printf (
                f,
                "     <relate dom=\"%i\" subdom=\"%i\"/>\n",
                a->id, b->id);
        }

        PFarray_printf (f, "  </dom_subdom_relationship>\n");
    }

    if (prop->disjdoms) {

        PFarray_printf (f,
                        "  <domain-disjointness count='%i'>\n",
                        PFarray_last (prop->disjdoms));

        for (unsigned int i = 0; i < PFarray_last (prop->disjdoms); i++) {

            a = ((disjdom_t *) PFarray_at (prop->disjdoms, i))->dom1;
            b = ((disjdom_t *) PFarray_at (prop->disjdoms, i))->dom2;

            PFarray_printf (
                f,
                "     <disjoint dom1='%i' dom2='%i'/>\n",
                a->id, b->id);
        }

        PFarray_printf (f, "  </domain-disjointness>\n");
    }
}

/**
 * Test if domain @a a is guaranteed to be disjoint from @a b.
 */
bool
PFprop_disjdom (const PFprop_t *p, dom_t *a, dom_t *b)
{
    assert (p);

    /* In case no properties are inferred and this function
       is called we are just restrictive and return no match */
    if (!p->disjdoms) return false;

    if (a == b)
        return false;

    for (unsigned int i = 0; i < PFarray_last (p->disjdoms); i++) {

        disjdom_t d = *(disjdom_t *) PFarray_at (p->disjdoms, i);

        if ((PFprop_subdom (p, a, d.dom1) && PFprop_subdom (p, b, d.dom2))
            || (PFprop_subdom (p, a, d.dom2) && PFprop_subdom (p, b, d.dom1)))
            return true;
    }

    return false;
}

/**
 * Test if domain @a subdom is a subdomain of the domain @a dom
 * (using the domain relationship list in property container @a prop).
 */
bool
PFprop_subdom (const PFprop_t *prop, dom_t *subdom, dom_t *dom)
{
    assert (prop);

    if (!subdom || !dom) return false;

    /* In case no properties are inferred and this function
       is called we are just restrictive and return no match */
    if (!prop->subdoms) return false;

    /* check trivial case of identity */
    if (subdom == dom)
        return true;
    else if (!subdom->super_dom)
        return false;
    /* and recursively call the check */
    else if (PFprop_subdom (prop, subdom->super_dom, dom))
        return true;

    if (subdom->union_doms)
        for (unsigned int i = 0; i < PFarray_last (subdom->union_doms); i++)
            if (PFprop_subdom (prop,
                               *(dom_t **) PFarray_at (subdom->union_doms, i),
                               dom))
                return true;

    return false;
}

/**
 * Worker that checks if a domain @a dom is in the list
 * of domains @a domains.
 */
static bool
in (PFarray_t *domains, dom_t *dom)
{
    for (unsigned int i = 0; i < PFarray_last (domains); i++)
        if (*(dom_t **) PFarray_at (domains, i) == dom)
            return true;
    return false;
}

/**
 * Worker that collects for a given domain all subdomains.
 */
static void
collect_super_doms (PFarray_t *domains, dom_t *dom)
{
    if (dom->super_dom &&
         !in (domains, dom->super_dom)) {
        *(dom_t **) PFarray_add (domains) = dom->super_dom;
        collect_super_doms (domains, dom->super_dom);
    }
    if (dom->union_doms) {
        dom_t *super_dom;
        for (unsigned int i = 0; i < PFarray_last (dom->union_doms); i++)
            if (!in (domains, *(dom_t **) PFarray_at (dom->union_doms, i))) {
                super_dom = *(dom_t **) PFarray_at (dom->union_doms, i);
                *(dom_t **) PFarray_add (domains) = super_dom;
                collect_super_doms (domains, super_dom);
            }
    }
}

/**
 * common_super_dom finds the lowest common domain of @a dom1 and
 * @a dom2 (using the domain relationship list in property container
 * @a prop).
 */
static dom_t *
common_super_dom (const PFprop_t *prop, dom_t *dom1, dom_t *dom2)
{
    PFarray_t *domains1, *domains2, *merge_doms;

    assert (prop);
    assert (prop->subdoms);

    /* check for problems first */
    if (!dom1 || !dom2)
        return NULL;

    /* check trivial cases of identity */
    if (dom1 == dom2)
        return dom1;
    /* and inclusion */
    else if (PFprop_subdom (prop, dom1, dom2))
        return dom1;
    else if (PFprop_subdom (prop, dom2, dom1))
        return dom2;

    /* collect all the super domains for each input */
    domains1 = PFarray (sizeof (dom_t), 10);
    collect_super_doms (domains1, dom1);
    domains2 = PFarray (sizeof (dom_t), 10);
    collect_super_doms (domains2, dom2);

    /* intersect both domain lists */
    merge_doms = PFarray (sizeof (dom_t),
                          PFarray_last (domains1) > PFarray_last (domains2)
                          ? PFarray_last (domains2)
                          : PFarray_last (domains1));
    for (unsigned int i = 0; i < PFarray_last (domains1); i++) {
        dom1 = *(dom_t **) PFarray_at (domains1, i);
        for (unsigned int j = 0; j < PFarray_last (domains2); j++) {
            dom2 = *(dom_t **) PFarray_at (domains2, j);
            if (dom1 == dom2)
                *(dom_t **) PFarray_add (merge_doms) = dom1;
        }
    }

    /* no common super domain exists */
    if (!PFarray_last (merge_doms))
        return NULL;

    dom1 = *(dom_t **) PFarray_at (merge_doms, 0);

    /* trivial case -- only one common domain */
    if (PFarray_last (merge_doms) == 1)
        return dom1;

    /* find the leaf node (of the domain tree) */
    for (unsigned int i = 1; i < PFarray_last (merge_doms); i++) {
        dom2 = *(dom_t **) PFarray_at (merge_doms, i);
        if (!dom1 /* undecided */ ||
            PFprop_subdom (prop, dom2, dom1))
            dom1 = dom2;
        else if (PFprop_subdom (prop, dom1, dom2))
            dom1 = dom1;
        else
            /* undecided */
            dom1 = NULL;
    }
    /* If we still stay undecided just report no
       common super domain. The only consequence is
       that we then might detect less subdomain
       relationships. */
    return dom1;
}

/**
 * For a list of guides find the smallest occurrence indicator.
 */
static unsigned int
find_guide_min (unsigned int count, PFguide_tree_t **guides)
{
    unsigned int min;

    assert (count);
    min = guides[0]->min;
    for (unsigned int i = 1; i < count; i++)
       min = min < guides[i]->min ? min : guides[i]->min;

   return min;
}

/* global type filter variable that restricts
   the domain property inference to a given type */
static PFalg_simple_type_t type_filter;

/* counter to assign each domain a rank for debugging purposes */
static unsigned int id;

/* reference to the special 'empty' domain */
static dom_t *EMPTYDOM;

/**
 * Constructor for a new domain.
 */
static dom_t *
new_dom (void)
{
    dom_t *dom = PFmalloc (sizeof (dom_t));

    dom->id         = id++; /* assign new debugging id */
    dom->super_dom  = NULL; /* no subdomain relationship yet */
    dom->union_doms = NULL; /* also no further subdomain relationship yet */

    return dom;
}

/**
 * Copy domains of children nodes to the property container.
 */
static void
copy_child_domains (PFla_op_t *n)
{
    if (L(n))
        for (unsigned int i = 0; i < L(n)->schema.count; i++) {
            *(dom_pair_t *) PFarray_add (n->prop->l_domains)
                = (dom_pair_t) { .col = L(n)->schema.items[i].name,
                                 .dom  = PFprop_dom (
                                             L(n)->prop,
                                             L(n)->schema.items[i].name)};
        }

    if (R(n))
        for (unsigned int i = 0; i < R(n)->schema.count; i++) {
            *(dom_pair_t *) PFarray_add (n->prop->r_domains)
                = (dom_pair_t) { .col = R(n)->schema.items[i].name,
                                 .dom  = PFprop_dom (
                                             R(n)->prop,
                                             R(n)->schema.items[i].name)};
        }
}

/**
 * Add a domain-subdomain relationship.
 */
static void
add_subdom_ (PFprop_t *prop, dom_t *dom, dom_t *subdom)
{
    assert (prop);
    assert (prop->subdoms);
    assert (dom);
    assert (subdom);

    *(subdom_t *) PFarray_add (prop->subdoms)
        = (subdom_t) { .dom = dom, .subdom = subdom };

    /* also add the domain-subdomain relationship
       in the domain itself to speed up calls to
       PFprop_subdom() */
    if (!subdom->super_dom)
        /* add parent domain */
        subdom->super_dom = dom;
    else {
        /* add further subdom */
        if (!subdom->union_doms)
            subdom->union_doms = PFarray (sizeof (dom_t), 3);
        *(dom_t **) PFarray_add (subdom->union_doms) = dom;
    }
}

/**
 * Add disjointness information for domains @a a and @a b.
 */
static void
add_disjdom_ (PFprop_t *prop, dom_t *a, dom_t *b)
{
    assert (prop);
    assert (prop->disjdoms);

    *(disjdom_t *) PFarray_add (prop->disjdoms)
        = (disjdom_t) { .dom1 = a, .dom2 = b };
}

/**
 * Add a new domain to the list of domains
 * (stored in property container @a prop).
 */
static void
add_dom_ (PFprop_t *prop, PFalg_col_t col, dom_t *dom)
{
    assert (prop);
    assert (prop->domains);

    *(dom_pair_t *) PFarray_add (prop->domains)
        = (dom_pair_t) { .col = col, .dom = dom };
}

/* macros that cope with type filters and empty inputs */
#define add_subdom(d,sd)                                          \
        do { if ((d) && (sd))                                     \
                 add_subdom_ (n->prop, (d), (sd));                \
        } while (0)
#define add_disjdom(a,b)                                          \
        do { if ((a) && (b))                                      \
                 add_disjdom_ (n->prop, (a), (b));                \
        } while (0)
#define add_dom(c,d)                                              \
        do { if (!type_filter ||                                  \
                 PFprop_type_of (n, (c)) == type_filter)          \
                 add_dom_ (n->prop, (c), (d));                    \
        } while (0)
#define add_new_dom(c)                                            \
        do { if (!type_filter ||                                  \
                 PFprop_type_of (n, (c)) == type_filter)          \
                 add_dom_ (n->prop, (c), new_dom ());             \
        } while (0)
#define filter_dom_(cp,n1,n2)                                     \
        do { if (!type_filter ||                                  \
                 PFprop_type_of ((cp), (n1)) == type_filter) {    \
                 dom_t *dom = new_dom();                          \
                 add_subdom (PFprop_dom ((cp)->prop, (n1)), dom); \
                 add_dom ((n2), dom);                             \
             }                                                    \
        } while (0)
#define filter_dom(cp,n)                                          \
        filter_dom_((cp), (n), (n))

/**
 * Add all domains of the node @a child to the list of domains
 * (stored in property container @a prop).
 */
static void
bulk_add_dom_ (PFla_op_t *n, PFla_op_t *child)
{
    assert (n);
    assert (n->prop);
    assert (child);
    assert (child->prop);

    for (unsigned int i = 0; i < child->schema.count; i++) {
        add_dom (child->schema.items[i].name,
                 PFprop_dom (child->prop, child->schema.items[i].name));
    }
}
#define bulk_add_dom(c)  bulk_add_dom_ (n, (c))

/**
 * Infer domain properties; worker for prop_infer().
 */
static void
infer_dom (PFla_op_t *n)
{
    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
            bulk_add_dom (R(n));
            break;

        case la_side_effects:
            break;

        case la_lit_tbl:
            /* create new domains for all columns */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_new_dom (n->schema.items[i].name);
            break;

        case la_empty_tbl:
            /* assign each column the empty domain (1) */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->schema.items[i].name, EMPTYDOM);
            break;

        case la_ref_tbl:
            /* create new domains for all columns */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_new_dom (n->schema.items[i].name);
            break;

        case la_attach:
            bulk_add_dom (L(n));
            add_new_dom (n->sem.attach.res);
            break;

        case la_cross:
            if (PFprop_card (R(n)->prop) > 0)
                bulk_add_dom (L(n));
            else
                /* we have to make sure to assign subdomains as otherwise
                   dynamic empty relations might be ignored */
                /* create new subdomains for all columns */
                for (unsigned int i = 0; i < L(n)->schema.count; i++)
                    filter_dom (L(n), L(n)->schema.items[i].name);
            
            if (PFprop_card (L(n)->prop) > 0)
                bulk_add_dom (R(n));
            else
                /* we have to make sure to assign subdomains as otherwise
                   dynamic empty relations might be ignored */
                /* create new subdomains for all columns */
                for (unsigned int i = 0; i < R(n)->schema.count; i++)
                    filter_dom (R(n), R(n)->schema.items[i].name);
            break;

        case la_thetajoin:
            /* As we do not know how multiple predicates interact
               we assign subdomains for all columns. */

            /* create new subdomains for all columns */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                filter_dom (L(n), L(n)->schema.items[i].name);
            /* create new subdomains for all columns */
            for (unsigned int i = 0; i < R(n)->schema.count; i++)
                filter_dom (R(n), R(n)->schema.items[i].name);
            break;

        case la_eqjoin:
        {   /**
             * Infering the domains of the join columns results
             * in a common domain, that is either the more general
             * domain if the are in a subdomain relationship or a
             * new subdomain. The domains of all other columns
             * (whose domain is different from the domains of the
             * join arguments) remain unchanged.
             */
            dom_t *col1_dom = PFprop_dom (L(n)->prop,
                                          n->sem.eqjoin.col1),
                  *col2_dom = PFprop_dom (R(n)->prop,
                                          n->sem.eqjoin.col2),
                  *join_dom,
                  *cur_dom;

            if (col1_dom == col2_dom)
                join_dom = col1_dom;
            else if (PFprop_subdom (n->prop, col1_dom, col2_dom))
                join_dom = col1_dom;
            else if (PFprop_subdom (n->prop, col2_dom, col1_dom))
                join_dom = col2_dom;
            else {
                join_dom = new_dom ();
                add_subdom (col1_dom, join_dom);
                add_subdom (col2_dom, join_dom);
            }

            /* copy domains and update domains of join arguments */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                if ((cur_dom = PFprop_dom (
                                   L(n)->prop,
                                   L(n)->schema.items[i].name))
                    == col1_dom)
                    add_dom (L(n)->schema.items[i].name, join_dom);
                else if (join_dom == col1_dom)
                    add_dom (L(n)->schema.items[i].name, cur_dom);
                else
                    filter_dom (L(n), L(n)->schema.items[i].name);

            for (unsigned int i = 0; i < R(n)->schema.count; i++)
                if ((cur_dom = PFprop_dom (
                                   R(n)->prop,
                                   R(n)->schema.items[i].name))
                    == col2_dom)
                    add_dom (R(n)->schema.items[i].name, join_dom);
                else if (join_dom == col2_dom)
                    add_dom (R(n)->schema.items[i].name, cur_dom);
                else
                    filter_dom (R(n), R(n)->schema.items[i].name);
        }   break;

        case la_internal_op:
            /* interpret this operator as internal join */
            if (n->sem.eqjoin_opt.kind == la_eqjoin) {
                /* do the same as for normal joins and
                   correctly update the columns names */
#define proj_at(l,i) (*(PFalg_proj_t *) PFarray_at ((l),(i)))
                /**
                 * Infering the domains of the join columns results
                 * in a common domain, that is either the more general
                 * domain if the are in a subdomain relationship or a
                 * new subdomain. The domains of all other columns
                 * (whose domain is different from the domains of the
                 * join arguments) remain unchanged.
                 */
                PFarray_t  *lproj = n->sem.eqjoin_opt.lproj,
                           *rproj = n->sem.eqjoin_opt.rproj;
                PFalg_col_t col1  = proj_at(lproj, 0).old,
                            col2  = proj_at(rproj, 0).old,
                            res   = proj_at(lproj, 0).new;
                
                dom_t *col1_dom = PFprop_dom (L(n)->prop, col1),
                      *col2_dom = PFprop_dom (R(n)->prop, col2),
                      *join_dom,
                      *cur_dom;

                if (col1_dom == col2_dom)
                    join_dom = col1_dom;
                else if (PFprop_subdom (n->prop, col1_dom, col2_dom))
                    join_dom = col1_dom;
                else if (PFprop_subdom (n->prop, col2_dom, col1_dom))
                    join_dom = col2_dom;
                else {
                    join_dom = new_dom();
                    add_subdom (col1_dom, join_dom);
                    add_subdom (col2_dom, join_dom);
                }
                add_dom (res, join_dom);

                /* copy domains and update domains of join arguments */
                for (unsigned int i = 1; i < PFarray_last (lproj); i++)
                    if ((cur_dom = PFprop_dom (
                                       L(n)->prop,
                                       proj_at (lproj, i).old))
                        == col1_dom)
                        add_dom (proj_at (lproj, i).new, join_dom);
                    else if (join_dom == col1_dom)
                        add_dom (proj_at (lproj, i).new, cur_dom);
                    else
                        filter_dom_ (L(n),
                                     proj_at (lproj, i).old,
                                     proj_at (lproj, i).new);

                for (unsigned int i = 1; i < PFarray_last (rproj); i++)
                    if ((cur_dom = PFprop_dom (
                                       R(n)->prop,
                                       proj_at (rproj, i).old))
                        == col2_dom)
                        add_dom (proj_at (rproj, i).new, join_dom);
                    else if (join_dom == col2_dom)
                        add_dom (proj_at (rproj, i).new, cur_dom);
                    else
                        filter_dom_ (R(n),
                                     proj_at (rproj, i).old,
                                     proj_at (rproj, i).new);
            }
            else
                PFoops (OOPS_FATAL,
                        "internal optimization operator is not allowed here");
            break;

        case la_semijoin:
        {   /**
             * Infering the domains of the join columns results
             * in a common domain, that is either the more general
             * domain if the are in a subdomain relationship or a
             * new subdomain. The domains of all other columns
             * (whose domain is different from the domains of the
             * join arguments) remain unchanged.
             */
            dom_t *col1_dom = PFprop_dom (L(n)->prop,
                                          n->sem.eqjoin.col1),
                  *col2_dom = PFprop_dom (R(n)->prop,
                                          n->sem.eqjoin.col2),
                  *join_dom,
                  *cur_dom;

            if (col1_dom == col2_dom)
                join_dom = col1_dom;
            else if (PFprop_subdom (n->prop, col1_dom, col2_dom))
                join_dom = col1_dom;
            else if (PFprop_subdom (n->prop, col2_dom, col1_dom))
                join_dom = col2_dom;
            else {
                join_dom = new_dom();
                add_subdom (col1_dom, join_dom);
                add_subdom (col2_dom, join_dom);
            }

            /* copy domains and update domains of join arguments */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                if ((cur_dom = PFprop_dom (
                                   L(n)->prop,
                                   L(n)->schema.items[i].name))
                    == col1_dom)
                    add_dom (L(n)->schema.items[i].name, join_dom);
                else if (join_dom == col1_dom)
                    add_dom (L(n)->schema.items[i].name, cur_dom);
                else
                    filter_dom (L(n), L(n)->schema.items[i].name);
        }   break;

        case la_project:
            /* bind all existing domains to the possibly new names */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->sem.proj.items[i].new,
                         PFprop_dom (L(n)->prop, n->sem.proj.items[i].old));
            break;

        case la_select:
        case la_pos_select:
            /* create new subdomains for all columns */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                filter_dom (L(n), L(n)->schema.items[i].name);
            break;

        case la_disjunion:
            /* create new superdomains for all existing columns */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                unsigned int j;
                dom_t *dom1, *dom2, *union_dom, *cdom;

                for (j = 0; j < R(n)->schema.count; j++)
                    if (L(n)->schema.items[i].name ==
                        R(n)->schema.items[j].name) {
                        dom1 = PFprop_dom (L(n)->prop,
                                           L(n)->schema.items[i].name);
                        dom2 = PFprop_dom (R(n)->prop,
                                           R(n)->schema.items[j].name);

                        if (dom1 == dom2)
                            union_dom = dom1;
                        else if (PFprop_subdom (n->prop, dom1, dom2) ||
                                 dom1 == EMPTYDOM)
                            union_dom = dom2;
                        else if (PFprop_subdom (n->prop, dom2, dom1) ||
                                 dom2 == EMPTYDOM)
                            union_dom = dom1;
                        else {
                            union_dom = new_dom();
                            /* add an edge to the new domain from the
                               lowest common subdomain of its input */
                            cdom = common_super_dom (n->prop, dom1, dom2);
                            if (cdom)
                                add_subdom (cdom, union_dom);

                            add_subdom (union_dom, dom1);
                            add_subdom (union_dom, dom2);
                        }
                        add_dom (L(n)->schema.items[i].name,
                                 union_dom);
                        break;
                    }
                if (j == R(n)->schema.count)
                    PFoops (OOPS_FATAL,
                            "can't find matching columns in "
                            "domain property inference.");
            }
            break;

        case la_intersect:
            /* create new subdomains for all existing columns */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                unsigned int j;
                for (j = 0; j < R(n)->schema.count; j++)
                    if (L(n)->schema.items[i].name ==
                        R(n)->schema.items[j].name) {
                        dom_t *dom = new_dom ();
                        add_subdom (PFprop_dom (L(n)->prop,
                                                L(n)->schema.items[i].name),
                                    dom);
                        add_subdom (PFprop_dom (R(n)->prop,
                                                R(n)->schema.items[j].name),
                                    dom);
                        add_dom (L(n)->schema.items[i].name, dom);
                        break;
                    }
                if (j == R(n)->schema.count)
                    PFoops (OOPS_FATAL,
                            "can't find matching columns in "
                            "domain property inference.");
            }
            break;

        case la_difference:
            /*
             * In case of the difference operator we know that
             *
             *  -- the domains for all columns must be subdomains
             *     in the left argument and
             *  -- the domains for all columns are disjoint from
             *     those in the right argument.
             */
            /* create new subdomains for all existing columns */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                dom_t *dom = new_dom ();
                add_dom (L(n)->schema.items[i].name, dom);

                add_subdom (PFprop_dom (L(n)->prop,
                                        L(n)->schema.items[i].name),
                            dom);

                add_disjdom (dom,
                             PFprop_dom (R(n)->prop,
                                         L(n)->schema.items[i].name));
            }
            break;

        case la_distinct:
            bulk_add_dom (L(n));
            break;

        case la_fun_1to1:
            bulk_add_dom (L(n));
            add_new_dom (n->sem.fun_1to1.res);
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            bulk_add_dom (L(n));
            add_new_dom (n->sem.binary.res);
            break;

        case la_bool_not:
            bulk_add_dom (L(n));
            add_new_dom (n->sem.unary.res);
            break;

        case la_aggr:
            for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                switch (n->sem.aggr.aggr[i].kind) {
                    case alg_aggr_dist:
                        add_dom (n->sem.aggr.aggr[i].res,
                                 PFprop_dom (L(n)->prop,
                                             n->sem.aggr.aggr[i].col));
                        break;
                    case alg_aggr_min:
                    case alg_aggr_max:
                    case alg_aggr_all:
                        filter_dom_ (L(n),
                                     n->sem.aggr.aggr[i].col,
                                     n->sem.aggr.aggr[i].res);
                        break;
                    case alg_aggr_count:
                    case alg_aggr_avg:
                    case alg_aggr_sum:
                    case alg_aggr_seqty1:
                    case alg_aggr_prod:
                        add_new_dom (n->sem.aggr.aggr[i].res);
                        break;
                }
            if (n->sem.aggr.part)
                add_dom (n->sem.aggr.part,
                         PFprop_dom (L(n)->prop, n->sem.aggr.part));
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            bulk_add_dom (L(n));
            add_new_dom (n->sem.sort.res);
            break;

        case la_rowid:
            bulk_add_dom (L(n));
            add_new_dom (n->sem.rowid.res);
            break;

        case la_type:
        case la_cast:
            bulk_add_dom (L(n));
            add_new_dom (n->sem.type.res);
            break;

        case la_type_assert:
            bulk_add_dom (L(n));
            break;

        case la_step:
            /* create new subdomain for column iter */
            filter_dom (R(n), n->sem.step.iter);
            /* create new domain for column item */
            add_new_dom (n->sem.step.item_res);
            break;

        case la_guide_step:
            if ((n->sem.step.spec.axis == alg_chld ||
                 n->sem.step.spec.axis == alg_attr ||
                 n->sem.step.spec.axis == alg_self) &&
                find_guide_min (n->sem.step.guide_count,
                                n->sem.step.guides) > 0)
                add_dom (n->sem.step.iter,
                         PFprop_dom (R(n)->prop, n->sem.step.iter));
            else
                /* create new subdomain for column iter */
                filter_dom (R(n), n->sem.step.iter);

            /* create new domain for column item */
            add_new_dom (n->sem.step.item_res);
            break;

        case la_step_join:
            for (unsigned int i = 0; i < R(n)->schema.count; i++)
                filter_dom (R(n), R(n)->schema.items[i].name);

            add_new_dom (n->sem.step.item_res);
            break;

        case la_guide_step_join:
            if ((n->sem.step.spec.axis == alg_chld ||
                 n->sem.step.spec.axis == alg_attr ||
                 n->sem.step.spec.axis == alg_self) &&
                find_guide_min (n->sem.step.guide_count,
                                n->sem.step.guides) > 0)
                bulk_add_dom (R(n));
            else
                for (unsigned int i = 0; i < R(n)->schema.count; i++)
                    filter_dom (R(n), R(n)->schema.items[i].name);

            add_new_dom (n->sem.step.item_res);
            break;

        case la_doc_index_join:
            for (unsigned int i = 0; i < R(n)->schema.count; i++)
                filter_dom (R(n), R(n)->schema.items[i].name);

            add_new_dom (n->sem.doc_join.item_res);
            break;

        case la_doc_tbl:
            bulk_add_dom (L(n));
            add_new_dom (n->sem.doc_tbl.res);
            break;

        case la_doc_access:
            bulk_add_dom (R(n));
            add_new_dom (n->sem.doc_access.res);
            break;

        case la_twig:
            /* retain domain for column iter */
            switch (L(n)->kind) {
                case la_docnode:
                    add_dom (n->sem.iter_item.iter,
                             PFprop_dom (L(n)->prop,
                                         L(n)->sem.docnode.iter));
                    break;

                case la_element:
                case la_comment:
                    add_dom (n->sem.iter_item.iter,
                             PFprop_dom (L(n)->prop,
                                         L(n)->sem.iter_item.iter));
                    break;

                case la_textnode:
                {   /* because of empty textnode constructors
                       create new subdomain for column iter */
                    dom_t *dom = new_dom();
                    add_subdom (PFprop_dom (L(n)->prop,
                                            L(n)->sem.iter_item.iter),
                                dom);
                    add_dom (n->sem.iter_item.iter, dom);
                }   break;
                    
                case la_attribute:
                case la_processi:
                    add_dom (n->sem.iter_item.iter,
                             PFprop_dom (L(n)->prop,
                                         L(n)->sem.iter_item1_item2.iter));
                    break;

                case la_content:
                    add_dom (n->sem.iter_item.iter,
                             PFprop_dom (L(n)->prop,
                                         L(n)->sem.iter_pos_item.iter));
                    break;

                default:
                    break;
            }
            /* create new domain for column item */
            add_new_dom (n->sem.iter_item.item);
            break;

        case la_fcns:
            break;

        case la_docnode:
            /* retain domain for column iter */
            add_dom_ (n->prop,
                      n->sem.docnode.iter,
                      PFprop_dom (L(n)->prop, n->sem.docnode.iter));
            break;

        case la_element:
        case la_comment:
        case la_textnode:
            /* retain domain for column iter */
            add_dom_ (n->prop,
                      n->sem.iter_item.iter,
                      PFprop_dom (L(n)->prop, n->sem.iter_item.iter));
            break;

        case la_attribute:
        case la_processi:
            /* retain domain for column iter */
            add_dom_ (n->prop,
                      n->sem.iter_item1_item2.iter,
                      PFprop_dom (L(n)->prop, n->sem.iter_item1_item2.iter));
            break;

        case la_content:
            /* retain domain for column iter */
            add_dom_ (n->prop,
                      n->sem.iter_pos_item.iter,
                      PFprop_dom (R(n)->prop, n->sem.iter_pos_item.iter));
            break;

        case la_merge_adjacent:
            /* retain domain for column iter */
            add_dom (n->sem.merge_adjacent.iter_res,
                     PFprop_dom (R(n)->prop, n->sem.merge_adjacent.iter_in));
            /* create new subdomain for column pos */
            filter_dom_ (R(n),
                         n->sem.merge_adjacent.pos_in,
                         n->sem.merge_adjacent.pos_res);
            /* create new domain for column item */
            add_new_dom (n->sem.merge_adjacent.item_res);
            break;

        case la_roots:
            bulk_add_dom (L(n));
            break;

        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_fun_frag_param:
            break;

        case la_error:
        case la_cache:
            bulk_add_dom (R(n));
            break;

        case la_nil:
        case la_trace:
            /* we have no properties */
            break;

        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            bulk_add_dom (L(n));
            break;

        case la_rec_fix:
            /* get the domains of the overall result */
            bulk_add_dom (R(n));
            break;

        case la_rec_param:
            /* recursion parameters do not have properties */
            break;

        case la_rec_arg:
            bulk_add_dom (R(n));
            break;

        case la_rec_base:
            /* create new domains for all columns */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_new_dom (n->schema.items[i].name);
            break;

        case la_fun_call:
        {
            unsigned int i = 0;
            if (n->sem.fun_call.occ_ind == alg_occ_exactly_one &&
                n->sem.fun_call.kind == alg_fun_call_xrpc) {
                add_dom (n->schema.items[0].name,
                         PFprop_dom (L(n)->prop, n->sem.fun_call.iter));
                i++;
            } else if (n->sem.fun_call.occ_ind == alg_occ_zero_or_one &&
                       n->sem.fun_call.kind == alg_fun_call_xrpc) {
                filter_dom_ (L(n),
                             n->sem.fun_call.iter,
                             n->schema.items[0].name);
                i++;
            }

            /* create new domains for all (remaining) columns */
            for (; i < n->schema.count; i++)
                add_new_dom (n->schema.items[i].name);
        }   break;

        case la_fun_param:
            bulk_add_dom (L(n));
            break;

        case la_proxy:
        case la_proxy_base:
            bulk_add_dom (L(n));
            break;

        case la_string_join:
            /* retain domain for column iter */
            add_dom (n->sem.string_join.iter_res,
                     PFprop_dom (R(n)->prop, n->sem.string_join.iter_sep));
            /* create new domain for column item */
            add_new_dom (n->sem.string_join.item_res);
            break;

        case la_dummy:
            bulk_add_dom (L(n));
            break;
    }
}

/* worker for PFprop_infer_dom */
static void
prop_infer (PFla_op_t *n, PFarray_t *subdoms, PFarray_t *disjdoms)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i], subdoms, disjdoms);

    n->bit_dag = true;

    /* assign all nodes the same domain relation */
    n->prop->subdoms  = subdoms;
    n->prop->disjdoms = disjdoms;

    /* reset the domain information
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (n->prop->domains)
        PFarray_last (n->prop->domains) = 0;
    else
        /* prepare the property for 10 columns */
        n->prop->domains = PFarray (sizeof (dom_pair_t), 10);

    if (L(n)) {
        if (n->prop->l_domains)
            PFarray_last (n->prop->l_domains) = 0;
        else
            /* prepare the property for 10 columns */
            n->prop->l_domains = PFarray (sizeof (dom_pair_t), 10);
    }

    if (R(n)) {
        if (n->prop->r_domains)
            PFarray_last (n->prop->r_domains) = 0;
        else
            /* prepare the property for 10 columns */
            n->prop->r_domains = PFarray (sizeof (dom_pair_t), 10);
    }

    /* copy all children domains */
    copy_child_domains (n);

    /* infer information on domain columns */
    infer_dom (n);
}

/**
 * Infer domain property for the columns of type nat in a DAG rooted in root
 */
void
PFprop_infer_nat_dom (PFla_op_t *root)
{
    PFprop_infer_card (root);
    /*
     * Initialize domain property inference with an empty domain
     * relation list,
     */
    PFarray_t *subdoms  = PFarray (sizeof (subdom_t), 50);
    PFarray_t *disjdoms = PFarray (sizeof (disjdom_t), 50);

    /* initialize the type filter */
    type_filter = aat_nat;
    /* re-initialize the domain counter */
    id = 1;
    /* create a new common domain representing the empty domain */
    EMPTYDOM = new_dom();

    prop_infer (root, subdoms, disjdoms);

    PFla_dag_reset (root);
}

/**
 * Infer domain property for a DAG rooted in root
 */
void
PFprop_infer_dom (PFla_op_t *root)
{
    PFprop_infer_card (root);
    /*
     * Initialize domain property inference with an empty domain
     * relation list,
     */
    PFarray_t *subdoms  = PFarray (sizeof (subdom_t), 50);
    PFarray_t *disjdoms = PFarray (sizeof (disjdom_t), 50);

    /* initialize the type filter */
    type_filter = 0; /* no type filter */
    /* re-initialize the domain counter */
    id = 1;
    /* create a new common domain representing the empty domain */
    EMPTYDOM = new_dom();

    prop_infer (root, subdoms, disjdoms);

    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
