/**
 * @file
 *
 * Inference of domain property of logical algebra expressions.
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2006 University of Konstanz.  All Rights Reserved.
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

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** starting from p, make a step right, then a step left */
#define RL(p) L(R(p))

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
check_occurrence (PFarray_t *dom_rel, unsigned int pos)
{
    dom_t dom = ((dom_rel_t *) PFarray_at (dom_rel, pos))->dom;

    for (unsigned int i = 0; i < PFarray_last (dom_rel); i++)
        if (dom == ((dom_rel_t *) PFarray_at (dom_rel, i))->subdom)
            return true;

    for (unsigned int i = 0; i < pos; i++)
        if (dom == ((dom_rel_t *) PFarray_at (dom_rel, i))->dom)
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
    if (prop->dom_rel) {
        PFarray_printf (f,
                        "dr_header [label=\"domain -> "
                        "subdomain\\nrelationships (# = %i)\"];\n"
                        "node1 -> dr_header;\n",
                        PFarray_last (prop->dom_rel));

        for (unsigned int i = 0; i < PFarray_last (prop->dom_rel); i++) {
            dom = ((dom_rel_t *) PFarray_at (prop->dom_rel, i))->dom;
            subdom = ((dom_rel_t *) PFarray_at (prop->dom_rel, i))->subdom;

            if (!check_occurrence (prop->dom_rel, i))
                PFarray_printf (
                    f,
                    "dom_rel%i [label=\"%i\"];\n"
                    "dr_header -> dom_rel%i;\n",
                    dom, dom, dom);

            PFarray_printf (
                f,
                "dom_rel%i [label=\"%i\"];\n"
                "dom_rel%i -> dom_rel%i;\n",
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
    dom_t dom, subdom;

    assert (prop);
    if (prop->dom_rel) {
        PFarray_printf (f,
                        "  <dom_subdom_relationship count=\"%i\">\n",
                        PFarray_last (prop->dom_rel));

        for (unsigned int i = 0; i < PFarray_last (prop->dom_rel); i++) {
            dom = ((dom_rel_t *) PFarray_at (prop->dom_rel, i))->dom;
            subdom = ((dom_rel_t *) PFarray_at (prop->dom_rel, i))->subdom;

            PFarray_printf (
                f,
                "     <relate dom=\"%i\" subdom=\"%i\"/>\n",
                dom, subdom);
        }

        PFarray_printf (f, "  </dom_subdom_relationship>\n");
    }
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
    assert (prop->dom_rel);

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

    for (unsigned int i = PFarray_last (prop->dom_rel); i > 0; --i) {
        subdom = ((dom_rel_t *) PFarray_at (prop->dom_rel, i))->subdom;
        dom = ((dom_rel_t *) PFarray_at (prop->dom_rel, i))->dom;
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
add_dom_rel (PFprop_t *prop, dom_t dom, dom_t subdom)
{
    assert (prop);
    assert (prop->dom_rel);
    
    *(dom_rel_t *) PFarray_add (prop->dom_rel)
        = (dom_rel_t) { .dom = dom, .subdom = subdom };
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
 * Infer domain properties; worker for prop_infer().
 */
static unsigned int
infer_dom (PFla_op_t *n, unsigned int id)
{
    switch (n->kind) {
        case la_serialize:
            bulk_add_dom (n->prop, R(n));
            break;

        case la_lit_tbl:
            /* create new domains for all attributes */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->prop, n->schema.items[i].name, id++);
            break;
            
        case la_empty_tbl:
            /* assign each attribute the empty domain (1) */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_dom (n->prop, n->schema.items[i].name, 1);
            break;
            
        case la_attach:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.attach.attname, id++);
            break;

        case la_cross:
            bulk_add_dom (n->prop, L(n));
            bulk_add_dom (n->prop, R(n));
            break;

        case la_eqjoin:
        case la_eqjoin_unq:
        {   /**
             * Infering the domains of the join attributes results
             * in a common schema, that is either the more general
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
                add_dom_rel (n->prop, att1_dom, join_dom);
                add_dom_rel (n->prop, att2_dom, join_dom);
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
                    add_dom_rel (n->prop, cur_dom, id);
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
                    add_dom_rel (n->prop, cur_dom, id);
                    add_dom (n->prop, R(n)->schema.items[i].name, id++);
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
            /* create new subdomains for all attributes */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                add_dom_rel (n->prop, 
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
                dom_t dom1, dom2, union_dom;

                for (j = 0; j < R(n)->schema.count; j++)
                    if (L(n)->schema.items[i].name ==
                        R(n)->schema.items[j].name) {
                        dom1 = PFprop_dom (L(n)->prop,
                                           L(n)->schema.items[i].name);
                        dom2 = PFprop_dom (R(n)->prop,
                                           R(n)->schema.items[j].name);
                        
                        if (dom1 == dom2)
                            union_dom = dom1;
                        else if (PFprop_subdom (n->prop, dom1, dom2))
                            union_dom = dom1;
                        else if (PFprop_subdom (n->prop, dom2, dom1))
                            union_dom = dom2;
                        else {
                            union_dom = id++;
                            add_dom_rel (n->prop, union_dom, dom1);
                            add_dom_rel (n->prop, union_dom, dom2);
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
                        add_dom_rel (
                            n->prop,
                            PFprop_dom (L(n)->prop, 
                                        L(n)->schema.items[i].name),
                            id);
                        add_dom_rel (
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
        case la_distinct:
            /* create new subdomains for all existing attributes */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                add_dom_rel (n->prop, 
                             PFprop_dom (L(n)->prop, 
                                         L(n)->schema.items[i].name),
                             id);
                add_dom (n->prop, L(n)->schema.items[i].name, id);
                id++;
            }
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
        case la_contains:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.binary.res, id++);
            break;

        case la_num_neg:
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
            add_dom_rel (n->prop,
                         PFprop_dom (L(n)->prop, n->sem.aggr.part),
                         id);
            add_dom (n->prop, n->sem.aggr.res, id++);
            if (n->sem.aggr.part)
                add_dom (n->prop, 
                         n->sem.aggr.part,
                         PFprop_dom (L(n)->prop, n->sem.aggr.part));
            break;

        case la_rownum:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.rownum.attname, id++);
            break;

        case la_number:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.number.attname, id++);
            break;

        case la_type:
        case la_cast:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.type.res, id++);
            break;

        case la_type_assert:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_scjoin:
            /* create new subdomain for attribute iter */
            add_dom_rel (n->prop, PFprop_dom (R(n)->prop,
                                              n->sem.scjoin.iter), id);
            add_dom (n->prop, n->sem.scjoin.iter, id++);
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.scjoin.item_res, id++);
            break;
            
        case la_doc_tbl:
            /* retain domain for attribute iter */
            add_dom (n->prop,
                     n->sem.doc_tbl.iter, 
                     PFprop_dom (L(n)->prop, n->sem.doc_tbl.iter));
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.doc_tbl.item_res, id++);
            break;
            
        case la_doc_access:
            bulk_add_dom (n->prop, R(n));
            add_dom (n->prop, n->sem.doc_access.res, id++);
            break;

        case la_element:
            /* retain domain for attribute iter */
            add_dom (n->prop, 
                     n->sem.elem.iter_res, 
                     PFprop_dom (RL(n)->prop, n->sem.elem.iter_qn));
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.elem.item_res, id++);
            break;
        
        case la_element_tag:
            break;
            
        case la_attribute:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.attr.res, id++);
            break;

        case la_textnode:
            bulk_add_dom (n->prop, L(n));
            add_dom (n->prop, n->sem.textnode.res, id++);
            break;

        case la_docnode:
        case la_comment:
        case la_processi:
            break;
            
        case la_merge_adjacent:
            /* retain domain for attribute iter */
            add_dom (n->prop,
                     n->sem.merge_adjacent.iter_res,
                     PFprop_dom (L(n)->prop, n->sem.merge_adjacent.iter_in));
            /* create new subdomain for attribute pos */
            add_dom_rel (n->prop,
                         PFprop_dom (L(n)->prop,
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
        case la_frag_union:
        case la_empty_frag:
            break;
            
        case la_cond_err:
            bulk_add_dom (n->prop, L(n));
            break;

        case la_string_join:
            /* retain domain for attribute iter */
            add_dom (n->prop, 
                     n->sem.string_join.iter_res,
                     PFprop_dom (L(n)->prop, n->sem.string_join.iter));
            /* create new domain for attribute item */
            add_dom (n->prop, n->sem.string_join.item_res, id++);
            break;

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");
    }
    return id;
}

/* worker for PFprop_infer_dom */
static unsigned int
prop_infer (PFla_op_t *n, PFarray_t *dr, unsigned int cur_dom_id)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return cur_dom_id;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        cur_dom_id = prop_infer (n->child[i], dr, cur_dom_id);

    n->bit_dag = true;

    /* assign all nodes the same domain relation */
    n->prop->dom_rel = dr;
    /* reset the domain information */
    n->prop->domains   = PFarray (sizeof (dom_pair_t));
    n->prop->l_domains = PFarray (sizeof (dom_pair_t));
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
    /* initialize domain property inference 
       with an empty domain relation list */
    prop_infer (root, PFarray (sizeof (dom_rel_t)), 2);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
