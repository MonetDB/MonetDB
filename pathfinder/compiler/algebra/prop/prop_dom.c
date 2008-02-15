/**
 * @file
 *
 * Inference of domain property of logical algebra expressions.
 *
 * We use abstract domain identifiers (implemented as integers)
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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"
#include <assert.h>

#include "properties.h"
#include "alg_dag.h"
#include "oops.h"
#include "mem.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/** Identifier for the (statically known) empty domain */
#define EMPTYDOM 1

/**
 * Return domain of attribute @a attr stored
 * in property container @a prop.
 */
dom_t
PFprop_dom (const PFprop_t *prop, PFalg_att_t attr)
{
    assert (prop);
    if (!prop->domains) return 0;

    for (unsigned int i = 0; i < PFarray_last (prop->domains); i++)
        if (attr == ((dom_pair_t *) PFarray_at (prop->domains, i))->attr)
            return ((dom_pair_t *) PFarray_at (prop->domains, i))->dom;

    return 0;
}

/**
 * Return domain of attribute @a attr in the domains of the
 * left child node (stored in property container @a prop)
 */
dom_t
PFprop_dom_left (const PFprop_t *prop, PFalg_att_t attr)
{
    assert (prop);
    if (!prop->l_domains) return 0;

    for (unsigned int i = 0; i < PFarray_last (prop->l_domains); i++)
        if (attr == ((dom_pair_t *) PFarray_at (prop->l_domains, i))->attr)
            return ((dom_pair_t *) PFarray_at (prop->l_domains, i))->dom;

    return 0;
}

/**
 * Return domain of attribute @a attr in the domains of the
 * right child nod (stored in property container @a prop)
 */
dom_t
PFprop_dom_right (const PFprop_t *prop, PFalg_att_t attr)
{
    assert (prop);
    if (!prop->r_domains) return 0;

    for (unsigned int i = 0; i < PFarray_last (prop->r_domains); i++)
        if (attr == ((dom_pair_t *) PFarray_at (prop->r_domains, i))->attr)
            return ((dom_pair_t *) PFarray_at (prop->r_domains, i))->dom;

    return 0;
}

/**
 * Writes domain represented by @a domain to character array @a f.
 */
void
PFprop_write_domain (PFarray_t *f, dom_t domain)
{
    PFarray_printf (f, "%i", domain);
}

/**
 * Helper function for PFprop_write_dom_rel_dot
 *
 * Checks if domain at position @a pos is already printed.
 */
static bool
check_occurrence (PFarray_t *subdoms, unsigned int pos)
{
    dom_t dom = ((subdom_t *) PFarray_at (subdoms, pos))->dom;

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
PFprop_write_dom_rel_dot (PFarray_t *f, const PFprop_t *prop)
{
    dom_t dom, subdom;

    assert (prop);
    if (prop->subdoms) {
        PFarray_printf (f,
                        "dr_header [label=\"domain -> "
                        "subdomain\\nrelationships (# = %i)\"];\n"
                        "node1 -> dr_header [dir=forward,style=invis];\n",
                        PFarray_last (prop->subdoms));

        for (unsigned int i = 0; i < PFarray_last (prop->subdoms); i++) {
            dom = ((subdom_t *) PFarray_at (prop->subdoms, i))->dom;
            subdom = ((subdom_t *) PFarray_at (prop->subdoms, i))->subdom;

            if (!check_occurrence (prop->subdoms, i))
                PFarray_printf (
                    f,
                    "dom_rel%i [label=\"%i\"];\n"
                    "dr_header -> dom_rel%i [dir=forward];\n",
                    dom, dom, dom);

            PFarray_printf (
                f,
                "dom_rel%i [label=\"%i\"];\n"
                "dom_rel%i -> dom_rel%i [dir=forward];\n",
                subdom, subdom, dom, subdom);
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
    dom_t a, b;

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
                a, b);
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
                a, b);
        }

        PFarray_printf (f, "  </domain-disjointness>\n");
    }
}

/**
 * Test if domain @a a is guaranteed to be disjoint from @a b.
 */
bool
PFprop_disjdom (const PFprop_t *p, dom_t a, dom_t b)
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
 * Test if domain @a in_subdom is a subdomain of the domain @a in_dom
 * (using the domain relationship list in property container @a prop).
 */
bool
PFprop_subdom (const PFprop_t *prop, dom_t in_subdom, dom_t in_dom)
{
    PFarray_t *subdomains;
    bool insert, duplicate;
    dom_t subdom, dom, subitem;

    assert (prop);

    if (!in_subdom || !in_dom) return false;

    /* In case no properties are inferred and this function
       is called we are just restrictive and return no match */
    if (!prop->subdoms) return false;

    /* check trivial case of identity */
    if (in_subdom == in_dom)
        return true;

    /**
     * Start list of subdomains with the input subdomain.
     * The list will be extended whenever a relationship
     * matches one item of the subdomains list, but the
     * expected result domain differs.
     */
    subdomains = PFarray (sizeof (dom_t));
    *(dom_t *) PFarray_add (subdomains) = in_subdom;

    for (unsigned int i = PFarray_last (prop->subdoms); i > 0; i--) {
        subdom = ((subdom_t *) PFarray_at (prop->subdoms, i-1))->subdom;
        dom = ((subdom_t *) PFarray_at (prop->subdoms, i-1))->dom;
        insert = false;
        duplicate = false;

        /* check for each item in the subdomains list
           if the match occurs or the superdomain is already
           in the list of subdomains */
        for (unsigned int j = 0; j < PFarray_last (subdomains); j++) {
            subitem = *(dom_t *) PFarray_at (subdomains, j);

            if (subdom == subitem) {
                if (dom == in_dom)
                    return true;
                else
                    insert = true;
            }
            if (dom == subitem)
                duplicate = true;
        }

        /* the current relationship results in a new domain
           that is added to the list of subdomains */
        if (insert && !duplicate)
            *(dom_t *) PFarray_add (subdomains) = dom;
    }

    return false;
}

/**
 * common_super_dom finds the lowest common domain of @a dom1 and
 * @a dom2 (using the domain relationship list in property container
 * @a prop).
 */
static dom_t
common_super_dom (const PFprop_t *prop, dom_t dom1, dom_t dom2)
{
    PFarray_t *domains1, *domains2, *merge_doms;
    bool insert, duplicate;
    dom_t subdom, dom, subitem;

    assert (prop);
    assert (prop->subdoms);

    /* check trivial case of identity */
    if (dom1 == dom2)
        return dom1;

    /* collect all the super domains for each input */

    /* start with the input domains as seed */
    domains1 = PFarray (sizeof (dom_t));
    *(dom_t *) PFarray_add (domains1) = dom1;
    domains2 = PFarray (sizeof (dom_t));
    *(dom_t *) PFarray_add (domains2) = dom2;

    for (unsigned int i = PFarray_last (prop->subdoms); i > 0; i--) {
        subdom = ((subdom_t *) PFarray_at (prop->subdoms, i-1))->subdom;
        dom = ((subdom_t *) PFarray_at (prop->subdoms, i-1))->dom;

        insert = false;
        duplicate = false;
        for (unsigned int j = 0; j < PFarray_last (domains1); j++) {
            subitem = *(dom_t *) PFarray_at (domains1, j);
            if (subdom == subitem)
                insert = true;
            if (dom == subitem)
                duplicate = true;
        }
        if (insert && !duplicate)
            *(dom_t *) PFarray_add (domains1) = dom;

        insert = false;
        duplicate = false;
        for (unsigned int j = 0; j < PFarray_last (domains2); j++) {
            subitem = *(dom_t *) PFarray_at (domains2, j);
            if (subdom == subitem)
                insert = true;
            if (dom == subitem)
                duplicate = true;
        }
        if (insert && !duplicate)
            *(dom_t *) PFarray_add (domains2) = dom;
    }

    /* intersect both domain lists */
    merge_doms = PFarray (sizeof (dom_t));
    for (unsigned int i = 0; i < PFarray_last (domains1); i++) {
        dom1 = *(dom_t *) PFarray_at (domains1, i);
        for (unsigned int j = 0; j < PFarray_last (domains2); j++) {
            dom2 = *(dom_t *) PFarray_at (domains2, j);
            if (dom1 == dom2) *(dom_t *) PFarray_add (merge_doms) = dom1;
        }
    }

    /* no common super domain exists */
    if (!PFarray_last (merge_doms))
        return 0;

    dom1 = *(dom_t *) PFarray_at (merge_doms, 0);

    /* trivial case -- only one common domain */
    if (PFarray_last (merge_doms) == 1)
        return dom1;

    /* find the leaf node (of the domain tree) */
    for (unsigned int i = 1; i < PFarray_last (merge_doms); i++) {
        dom2 = *(dom_t *) PFarray_at (merge_doms, i);
        if (!dom1 /* undecided */ ||
            PFprop_subdom (prop, dom2, dom1))
            dom1 = dom2;
        else if (PFprop_subdom (prop, dom1, dom2))
            dom1 = dom1;
        else
            /* undecided */
            dom1 = 0;
    }
    /* If we still stay undecided just report no
       common super domain. The only consequence is
       that we then might detect less subdomain
       relationships. */

    return dom1;
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
                = (dom_pair_t) { .attr = L(n)->schema.items[i].name,
                                 .dom  = PFprop_dom (
                                             L(n)->prop,
                                             L(n)->schema.items[i].name)};
        }

    if (R(n))
        for (unsigned int i = 0; i < R(n)->schema.count; i++) {
            *(dom_pair_t *) PFarray_add (n->prop->r_domains)
                = (dom_pair_t) { .attr = R(n)->schema.items[i].name,
                                 .dom  = PFprop_dom (
                                             R(n)->prop,
                                             R(n)->schema.items[i].name)};
        }
}

/**
 * Add a domain-subdomain relationship.
 */
static void
add_subdom (PFprop_t *prop, dom_t dom, dom_t subdom)
{
    assert (prop);
    assert (prop->subdoms);

    *(subdom_t *) PFarray_add (prop->subdoms)
        = (subdom_t) { .dom = dom, .subdom = subdom };
}

/**
 * Add disjointness information for domains @a a and @a b.
 */
static void
add_disjdom (PFprop_t *prop, dom_t a, dom_t b)
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
add_dom (PFprop_t *prop, PFalg_att_t attr, dom_t dom)
{
    assert (prop);
    assert (prop->domains);

    *(dom_pair_t *) PFarray_add (prop->domains)
        = (dom_pair_t) { .attr = attr, .dom = dom };
}

/**
 * Add all domains of the node @a child to the list of domains
 * (stored in property container @a prop).
 */
static void
bulk_add_dom (PFprop_t *prop, PFla_op_t *child)
{
    assert (prop);
    assert (child);
    assert (child->prop);

    for (unsigned int i = 0; i < child->schema.count; i++) {
        add_dom (prop,
                 child->schema.items[i].name,
                 PFprop_dom (child->prop, child->schema.items[i].name));
    }
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

/**
 * Infer domain properties; worker for prop_infer().
 */
static unsigned int
infer_dom (PFla_op_t *n, unsigned int id)
{
    switch (n->kind) {
        case la_serialize_seq:
            bulk_add_dom (n->prop, R(n));
            break;

        case la_serialize_rel:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_lit_tbl:
            /* create new domains for all attributes */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->prop, n->schema.items[i].name, id++);
            break;

        case la_empty_tbl:
            /* assign each attribute the empty domain (1) */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->prop, n->schema.items[i].name, EMPTYDOM);
            break;

        case la_ref_tbl:
            /* create new domains for all attributes */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->prop, n->schema.items[i].name, id++);
            break;

        case la_attach:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.attach.res, id++);
            break;

        case la_cross:
            /* we have to make sure to assign subdomains as otherwise
               dynamic empty relations might be ignored */

            /* create new subdomains for all attributes */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                add_subdom (n->prop,
                            PFprop_dom (L(n)->prop,
                                        L(n)->schema.items[i].name),
                            id);
                add_dom (n->prop, L(n)->schema.items[i].name, id);
                id++;
            }
            /* create new subdomains for all attributes */
            for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                add_subdom (n->prop,
                            PFprop_dom (R(n)->prop,
                                        R(n)->schema.items[i].name),
                            id);
                add_dom (n->prop, R(n)->schema.items[i].name, id);
                id++;
            }
            break;

        case la_eqjoin:
        case la_eqjoin_unq:
        {   /**
             * Infering the domains of the join attributes results
             * in a common domain, that is either the more general
             * domain if the are in a subdomain relationship or a
             * new subdomain. The domains of all other columns
             * (whose domain is different from the domains of the
             * join arguments) remain unchanged.
             */
            dom_t att1_dom = PFprop_dom (L(n)->prop,
                                         n->sem.eqjoin.att1);
            dom_t att2_dom = PFprop_dom (R(n)->prop,
                                         n->sem.eqjoin.att2);
            dom_t join_dom;
            dom_t cur_dom;

            if (att1_dom == att2_dom)
                join_dom = att1_dom;
            else if (PFprop_subdom (n->prop, att1_dom, att2_dom))
                join_dom = att1_dom;
            else if (PFprop_subdom (n->prop, att2_dom, att1_dom))
                join_dom = att2_dom;
            else {
                join_dom = id++;
                add_subdom (n->prop, att1_dom, join_dom);
                add_subdom (n->prop, att2_dom, join_dom);
            }

            /* copy domains and update domains of join arguments */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                if ((cur_dom = PFprop_dom (
                                   L(n)->prop,
                                   L(n)->schema.items[i].name))
                    == att1_dom)
                    add_dom (n->prop, L(n)->schema.items[i].name, join_dom);
                else if (join_dom == att1_dom)
                    add_dom (n->prop, L(n)->schema.items[i].name, cur_dom);
                else {
                    add_subdom (n->prop, cur_dom, id);
                    add_dom (n->prop, L(n)->schema.items[i].name, id++);
                }

            for (unsigned int i = 0; i < R(n)->schema.count; i++)
                if ((cur_dom = PFprop_dom (
                                   R(n)->prop,
                                   R(n)->schema.items[i].name))
                    == att2_dom)
                    add_dom (n->prop, R(n)->schema.items[i].name, join_dom);
                else if (join_dom == att2_dom)
                    add_dom (n->prop, R(n)->schema.items[i].name, cur_dom);
                else {
                    add_subdom (n->prop, cur_dom, id);
                    add_dom (n->prop, R(n)->schema.items[i].name, id++);
                }
        }   break;

        case la_semijoin:
        {   /**
             * Infering the domains of the join attributes results
             * in a common domain, that is either the more general
             * domain if the are in a subdomain relationship or a
             * new subdomain. The domains of all other columns
             * (whose domain is different from the domains of the
             * join arguments) remain unchanged.
             */
            dom_t att1_dom = PFprop_dom (L(n)->prop,
                                         n->sem.eqjoin.att1);
            dom_t att2_dom = PFprop_dom (R(n)->prop,
                                         n->sem.eqjoin.att2);
            dom_t join_dom;
            dom_t cur_dom;

            if (att1_dom == att2_dom)
                join_dom = att1_dom;
            else if (PFprop_subdom (n->prop, att1_dom, att2_dom))
                join_dom = att1_dom;
            else if (PFprop_subdom (n->prop, att2_dom, att1_dom))
                join_dom = att2_dom;
            else {
                join_dom = id++;
                add_subdom (n->prop, att1_dom, join_dom);
                add_subdom (n->prop, att2_dom, join_dom);
            }

            /* copy domains and update domains of join arguments */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                if ((cur_dom = PFprop_dom (
                                   L(n)->prop,
                                   L(n)->schema.items[i].name))
                    == att1_dom)
                    add_dom (n->prop, L(n)->schema.items[i].name, join_dom);
                else if (join_dom == att1_dom)
                    add_dom (n->prop, L(n)->schema.items[i].name, cur_dom);
                else {
                    add_subdom (n->prop, cur_dom, id);
                    add_dom (n->prop, L(n)->schema.items[i].name, id++);
                }
        }   break;

        case la_thetajoin:
        {   /**
             * Infering the domains of the equi-join attributes results
             * in a common domain, that is either the more general
             * domain if the are in a subdomain relationship or a
             * new subdomain. A new subdomain is created for all other
             * columns.
             */
            dom_t att1_dom, att2_dom, join_dom;
            for (unsigned int i = 0; i < n->sem.thetajoin.count; i++)
                if (n->sem.thetajoin.pred[i].comp == alg_comp_eq) {
                    att1_dom = PFprop_dom (L(n)->prop,
                                           n->sem.thetajoin.pred[i].left);
                    att2_dom = PFprop_dom (R(n)->prop,
                                           n->sem.thetajoin.pred[i].right);

                    if (att1_dom == att2_dom)
                        join_dom = att1_dom;
                    else if (PFprop_subdom (n->prop, att1_dom, att2_dom))
                        join_dom = att1_dom;
                    else if (PFprop_subdom (n->prop, att2_dom, att1_dom))
                        join_dom = att2_dom;
                    else {
                        join_dom = id++;
                        add_subdom (n->prop, att1_dom, join_dom);
                        add_subdom (n->prop, att2_dom, join_dom);
                    }
                    add_dom (n->prop,
                             n->sem.thetajoin.pred[i].left,
                             join_dom);
                    add_dom (n->prop,
                             n->sem.thetajoin.pred[i].right,
                             join_dom);
                }

            /* copy domains and update domains of join arguments */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                unsigned int j;
                /* filter out the equi-join columns */
                for (j = 0; j < n->sem.thetajoin.count; j++)
                    if (n->sem.thetajoin.pred[i].comp == alg_comp_eq &&
                        L(n)->schema.items[i].name ==
                        n->sem.thetajoin.pred[j].left)
                        break;
                if (j == n->sem.thetajoin.count) {
                    add_subdom (n->prop,
                                PFprop_dom (
                                    L(n)->prop,
                                    L(n)->schema.items[i].name),
                                id);
                    add_dom (n->prop, L(n)->schema.items[i].name, id++);
                }
            }

            for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                unsigned int j;
                /* filter out the equi-join columns */
                for (j = 0; j < n->sem.thetajoin.count; j++)
                    if (n->sem.thetajoin.pred[i].comp == alg_comp_eq &&
                        R(n)->schema.items[i].name ==
                        n->sem.thetajoin.pred[j].right)
                        break;
                if (j == n->sem.thetajoin.count) {
                    add_subdom (n->prop,
                                PFprop_dom (
                                    R(n)->prop,
                                    R(n)->schema.items[i].name),
                                id);
                    add_dom (n->prop, R(n)->schema.items[i].name, id++);
                }
            }

        }   break;

        case la_project:
            /* bind all existing domains to the possibly new names */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->prop,
                         n->sem.proj.items[i].new,
                         PFprop_dom (L(n)->prop, n->sem.proj.items[i].old));
            break;

        case la_select:
        case la_pos_select:
            /* create new subdomains for all attributes */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                add_subdom (n->prop,
                            PFprop_dom (L(n)->prop,
                                        L(n)->schema.items[i].name),
                            id);
                add_dom (n->prop, L(n)->schema.items[i].name, id);
                id++;
            }
            break;

        case la_disjunion:
            /* create new superdomains for all existing attributes */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                unsigned int j;
                dom_t dom1, dom2, union_dom, cdom;

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
                                 dom2 == EMPTYDOM)
                            union_dom = dom2;
                        else if (PFprop_subdom (n->prop, dom2, dom1) ||
                                 dom1 == EMPTYDOM)
                            union_dom = dom1;
                        else {
                            union_dom = id++;
                            /* add an edge to the new domain from the
                               lowest common subdomain of its input */
                            cdom = common_super_dom (n->prop, dom1, dom2);
                            if (cdom)
                                add_subdom (n->prop, cdom, union_dom);

                            add_subdom (n->prop, union_dom, dom1);
                            add_subdom (n->prop, union_dom, dom2);
                        }
                        add_dom (n->prop,
                                 L(n)->schema.items[i].name,
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
            /* create new subdomains for all existing attributes */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                unsigned int j;
                for (j = 0; j < R(n)->schema.count; j++)
                    if (L(n)->schema.items[i].name ==
                        R(n)->schema.items[j].name) {
                        add_subdom (
                            n->prop,
                            PFprop_dom (L(n)->prop,
                                        L(n)->schema.items[i].name),
                            id);
                        add_subdom (
                            n->prop,
                            PFprop_dom (R(n)->prop,
                                        R(n)->schema.items[j].name),
                            id);
                        add_dom (n->prop, L(n)->schema.items[i].name, id);
                        id++;
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
             *  -- the domains for all attributes must be subdomains
             *     in the left argument and
             *  -- the domains for all attributes are disjoint from
             *     those in the right argument.
             */
            /* create new subdomains for all existing attributes */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {

                add_dom (n->prop, L(n)->schema.items[i].name, id);

                add_subdom (n->prop,
                            PFprop_dom (L(n)->prop,
                                        L(n)->schema.items[i].name),
                            id);

                add_disjdom (n->prop,
                             id,
                             PFprop_dom (R(n)->prop,
                                         L(n)->schema.items[i].name));

                id++;
            }
            break;

        case la_distinct:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_fun_1to1:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.fun_1to1.res, id++);
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_to:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.binary.res, id++);
            break;

        case la_bool_not:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.unary.res, id++);
            break;

        case la_avg:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
            add_dom (n->prop, n->sem.aggr.res, id++);
            if (n->sem.aggr.part)
                add_dom (n->prop,
                         n->sem.aggr.part,
                         PFprop_dom (L(n)->prop, n->sem.aggr.part));
            break;

        case la_max:
        case la_min:
            add_subdom (n->prop,
                        PFprop_dom (L(n)->prop, n->sem.aggr.res),
                        id);
            add_dom (n->prop, n->sem.aggr.res, id++);
            if (n->sem.aggr.part)
                add_dom (n->prop,
                         n->sem.aggr.part,
                         PFprop_dom (L(n)->prop, n->sem.aggr.part));
            break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.sort.res, id++);
            break;

        case la_rowid:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.rowid.res, id++);
            break;

        case la_type:
        case la_cast:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.type.res, id++);
            break;

        case la_type_assert:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_step:
            /* create new subdomain for attribute iter */
            add_subdom (n->prop, PFprop_dom (R(n)->prop,
                                             n->sem.step.iter), id);
            add_dom (n->prop, n->sem.step.iter, id++);
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.step.item_res, id++);
            break;

        case la_guide_step:
            if ((n->sem.step.axis == alg_chld ||
                 n->sem.step.axis == alg_attr ||
                 n->sem.step.axis == alg_self) &&
                find_guide_min (n->sem.step.guide_count,
                                n->sem.step.guides) > 0)
                add_dom (n->prop,
                         n->sem.step.iter,
                         PFprop_dom (R(n)->prop, n->sem.step.iter));
            else {
                /* create new subdomain for attribute iter */
                add_subdom (n->prop, PFprop_dom (R(n)->prop,
                                                 n->sem.step.iter), id);
                add_dom (n->prop, n->sem.step.iter, id++);
            }
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.step.item_res, id++);
            break;

        case la_step_join:
            for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                add_subdom (n->prop, PFprop_dom (R(n)->prop,
                                                 R(n)->schema.items[i].name), id);
                add_dom (n->prop, R(n)->schema.items[i].name, id++);
            }
            add_dom (n->prop, n->sem.step.item_res, id++);
            break;

        case la_guide_step_join:
            if ((n->sem.step.axis == alg_chld ||
                 n->sem.step.axis == alg_attr ||
                 n->sem.step.axis == alg_self) &&
                find_guide_min (n->sem.step.guide_count,
                                n->sem.step.guides) > 0)
                bulk_add_dom (n->prop, R(n));
            else
                for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                    add_subdom (n->prop,
                                PFprop_dom (R(n)->prop,
                                            R(n)->schema.items[i].name), id);
                    add_dom (n->prop, R(n)->schema.items[i].name, id++);
                }
            add_dom (n->prop, n->sem.step.item_res, id++);
            break;

        case la_doc_index_join:
            for (unsigned int i = 0; i < R(n)->schema.count; i++) {
                add_subdom (n->prop, PFprop_dom (R(n)->prop,
                                                 R(n)->schema.items[i].name), id);
                add_dom (n->prop, R(n)->schema.items[i].name, id++);
            }
            add_dom (n->prop, n->sem.doc_join.item_res, id++);
            break;

        case la_doc_tbl:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.doc_tbl.res, id++);
            break;

        case la_doc_access:
            bulk_add_dom (n->prop, R(n));
            add_dom (n->prop, n->sem.doc_access.res, id++);
            break;

        case la_twig:
            /* retain domain for attribute iter */
            switch (L(n)->kind) {
                case la_docnode:
                    add_dom (n->prop,
                             n->sem.iter_item.iter,
                             PFprop_dom (L(n)->prop,
                                         L(n)->sem.docnode.iter));
                    break;

                case la_element:
                case la_textnode:
                case la_comment:
                    add_dom (n->prop,
                             n->sem.iter_item.iter,
                             PFprop_dom (L(n)->prop,
                                         L(n)->sem.iter_item.iter));
                    break;

                case la_attribute:
                case la_processi:
                    add_dom (n->prop,
                             n->sem.iter_item.iter,
                             PFprop_dom (L(n)->prop,
                                         L(n)->sem.iter_item1_item2.iter));
                    break;

                case la_content:
                    add_dom (n->prop,
                             n->sem.iter_item.iter,
                             PFprop_dom (L(n)->prop,
                                         L(n)->sem.iter_pos_item.iter));
                    break;

                default:
                    break;
            }
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.iter_item.item, id++);
            break;

        case la_fcns:
            break;

        case la_docnode:
            /* retain domain for attribute iter */
            add_dom (n->prop,
                     n->sem.docnode.iter,
                     PFprop_dom (L(n)->prop, n->sem.docnode.iter));
            break;

        case la_element:
        case la_comment:
        case la_textnode:
            /* retain domain for attribute iter */
            add_dom (n->prop,
                     n->sem.iter_item.iter,
                     PFprop_dom (L(n)->prop, n->sem.iter_item.iter));
            break;

        case la_attribute:
        case la_processi:
            /* retain domain for attribute iter */
            add_dom (n->prop,
                     n->sem.iter_item1_item2.iter,
                     PFprop_dom (L(n)->prop, n->sem.iter_item1_item2.iter));
            break;

        case la_content:
            /* retain domain for attribute iter */
            add_dom (n->prop,
                     n->sem.iter_pos_item.iter,
                     PFprop_dom (R(n)->prop, n->sem.iter_pos_item.iter));
            break;

        case la_merge_adjacent:
            /* retain domain for attribute iter */
            add_dom (n->prop,
                     n->sem.merge_adjacent.iter_res,
                     PFprop_dom (R(n)->prop, n->sem.merge_adjacent.iter_in));
            /* create new subdomain for attribute pos */
            add_subdom (n->prop,
                        PFprop_dom (R(n)->prop,
                                    n->sem.merge_adjacent.pos_in),
                        id);
            add_dom (n->prop, n->sem.merge_adjacent.pos_res, id++);
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.merge_adjacent.item_res, id++);
            break;

        case la_roots:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_fun_frag_param:
            break;

        case la_error:
            /* use a new domain for the result of the error operator */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                if (L(n)->schema.items[i].name != n->sem.err.att)
                    add_dom (n->prop,
                             L(n)->schema.items[i].name,
                             PFprop_dom (L(n)->prop, L(n)->schema.items[i].name));
                else
                    add_dom (n->prop, n->sem.err.att, id++);
            break;

        case la_cond_err:
        case la_trace:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_nil:
        case la_trace_msg:
        case la_trace_map:
            /* we have no properties */
            break;

        case la_rec_fix:
            /* get the domains of the overall result */
            bulk_add_dom (n->prop, R(n));
            break;

        case la_rec_param:
            /* recursion parameters do not have properties */
            break;

        case la_rec_arg:
            bulk_add_dom (n->prop, R(n));
            break;

        case la_rec_base:
            /* create new domains for all attributes */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->prop, n->schema.items[i].name, id++);
            break;

        case la_fun_call:
        {
            unsigned int i = 0;
            if (n->sem.fun_call.occ_ind == alg_occ_exactly_one &&
                n->sem.fun_call.kind == alg_fun_call_xrpc) {
                add_dom (n->prop,
                         n->schema.items[0].name,
                         PFprop_dom (L(n)->prop, n->sem.fun_call.iter));
                i++;
            } else if (n->sem.fun_call.occ_ind == alg_occ_zero_or_one &&
                       n->sem.fun_call.kind == alg_fun_call_xrpc) {
                add_subdom (n->prop,
                            PFprop_dom (L(n)->prop, n->sem.fun_call.iter),
                            id);
                add_dom (n->prop, n->schema.items[0].name, id++);
                i++;
            }
                
            /* create new domains for all (remaining) attributes */
            for (; i < n->schema.count; i++)
                add_dom (n->prop, n->schema.items[i].name, id++);
        }   break;
                
        case la_fun_param:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_proxy:
        case la_proxy_base:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_string_join:
            /* retain domain for attribute iter */
            add_dom (n->prop,
                     n->sem.string_join.iter_res,
                     PFprop_dom (R(n)->prop, n->sem.string_join.iter_sep));
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.string_join.item_res, id++);
            break;

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");

        case la_dummy:
            bulk_add_dom (n->prop, L(n));
            break;
    }
    return id;
}

/* worker for PFprop_infer_dom */
static unsigned int
prop_infer (PFla_op_t *n, PFarray_t *subdoms, PFarray_t *disjdoms,
            unsigned int cur_dom_id)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return cur_dom_id;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        cur_dom_id = prop_infer (n->child[i], subdoms, disjdoms, cur_dom_id);

    n->bit_dag = true;

    /* assign all nodes the same domain relation */
    n->prop->subdoms   = subdoms;
    n->prop->disjdoms  = disjdoms;

    /* reset the domain information
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (n->prop->domains)
        PFarray_last (n->prop->domains) = 0;
    else
        n->prop->domains   = PFarray (sizeof (dom_pair_t));

    if (n->prop->l_domains)
        PFarray_last (n->prop->l_domains) = 0;
    else
        n->prop->l_domains = PFarray (sizeof (dom_pair_t));

    if (n->prop->r_domains)
        PFarray_last (n->prop->r_domains) = 0;
    else
        n->prop->r_domains = PFarray (sizeof (dom_pair_t));

    /* copy all children domains */
    copy_child_domains (n);

    /* infer information on domain columns */
    cur_dom_id = infer_dom (n, cur_dom_id);

    return cur_dom_id;
}

/**
 * Infer domain property for a DAG rooted in root
 */
void
PFprop_infer_dom (PFla_op_t *root)
{
    /*
     * Initialize domain property inference with an empty domain
     * relation list,
     */
    PFarray_t *subdoms  = PFarray (sizeof (subdom_t));
    PFarray_t *disjdoms = PFarray (sizeof (disjdom_t));

    prop_infer (root, subdoms, disjdoms, 2);

    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
