/**
 * @file
 *
 * SQL tree optimization (e.g., removing multiple references to xmldoc).
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
 * $Id$
 */

#include "pathfinder.h"
#include "sql.h"
#include "sql_mnemonic.h"
#include "mem.h"
#include "oops.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>

/*
 * Easily access subtree parts.
 */
/* starting from p, make a left step */
#define L(p)      (p->child[0])
/* starting from p, make a right step */
#define R(p)      (p->child[1])
/* ... and so on */
#define LL(p)     L(L(p))
#define LR(p)     R(L(p))
#define RL(p)     L(R(p))
#define RR(p)     R(R(p))
#define RLR(p)    R(L(R(p)))
#define RLL(p)    L(L(R(p)))
#define RLRL(p)   L(R(L(R(p))))

/* encoding for the different node types in out XML scheme
   !!! Keep this aligned with the SQL code generation !!! */
#define ELEM    1    /* element node */
#define ATTR    2    /* attribute */
#define PF_TEXT 3    /* text */
#define COMM    4    /* comment */
#define PI      5    /* processing instruction */
#define DOC     6    /* document root node */

#define node_add(nl,a) *(PFsql_t **) PFarray_add (nl) = a
#define node_list_at(nl,i) (*(PFsql_t **) PFarray_at (nl, i))

/**
 * Check for a document-node kind predicate.
 */
static PFsql_t *
check_doc_alias (PFsql_t *n, void *ctx)
{
    (void) ctx;

    if (n->kind != sql_eq)
        return NULL;

    if (L(n)->kind == sql_column_name &&
        L(n)->sem.column.name->id == PF_SQL_COLUMN_SPECIAL &&
        L(n)->sem.column.name->spec == sql_col_kind &&
        R(n)->kind == sql_lit_int && 
        R(n)->sem.atom.val.i == DOC)
        return L(n);
    if (R(n)->kind == sql_column_name &&
        R(n)->sem.column.name->id == PF_SQL_COLUMN_SPECIAL &&
        R(n)->sem.column.name->spec == sql_col_kind &&
        L(n)->kind == sql_lit_int && 
        L(n)->sem.atom.val.i == DOC)
        return R(n);

    return NULL;
}

/**
 * Lookup the string of an equality predicate.
 */
static PFsql_t *
check_string_pred (PFsql_t *n, void *ctx)
{
    PFsql_aident_t alias;
    
    if (n->kind != sql_eq)
        return NULL;

    alias = *(PFsql_aident_t *) ctx;

    if (L(n)->kind == sql_column_name &&
        L(n)->sem.column.name->id == PF_SQL_COLUMN_SPECIAL &&
        L(n)->sem.column.name->spec == sql_col_value &&
        L(n)->sem.column.alias == alias &&
        R(n)->kind == sql_lit_str)
        return R(n);
    if (R(n)->kind == sql_column_name &&
        R(n)->sem.column.name->id == PF_SQL_COLUMN_SPECIAL &&
        R(n)->sem.column.name->spec == sql_col_value &&
        R(n)->sem.column.alias == alias &&
        L(n)->kind == sql_lit_str)
        return L(n);

    return NULL;
}

/**
 * Find refences that look up the document node.
 */
static bool
check_docnode (PFsql_t *n, void *ctx)
{
    PFsql_t *name = check_doc_alias (n, ctx);
    if (name &&
        name->sem.column.alias == *(PFsql_aident_t *) ctx)
        return true;
        
    return check_string_pred (n, ctx) != NULL;
}

/**
 * Traverse the WHERE list and collect all nodes
 * that fullfil a certain condition (@a fun).
 */
static void
where_traverse (PFsql_t *n, PFarray_t *node_list,
                PFsql_t * (* fun) (PFsql_t *, void *),
                void *ctx)
{
    PFsql_t *res;
    
    if (n->kind == sql_and) {
        where_traverse (L(n), node_list, fun, ctx);
        where_traverse (R(n), node_list, fun, ctx);
    }

    res = fun (n, ctx);
    if (res)
        node_add (node_list, res);
}

/**
 * Traverse the WHERE list and remove predicates
 * that fullfil a certain condition (@a fun).
 */
static void
where_pred_remove (PFsql_t *n,
                   bool (* fun) (PFsql_t *, void *),
                   void *ctx)
{
    if (n->kind == sql_and) {
        if (fun (L(n), ctx)) {
            where_pred_remove (R(n), fun, ctx);
            *n = *R(n);
        } else if (fun (R(n), ctx)) {
            where_pred_remove (L(n), fun, ctx);
            *n = *L(n);
        } else {
            where_pred_remove (L(n), fun, ctx);
            where_pred_remove (R(n), fun, ctx);
        }
    }
}

/**
 * Based on an alias @a alias remove the base relation.
 */
static void
base_rel_remove (PFsql_t *n, PFsql_aident_t alias)
{
    if (n->kind == sql_from_list) {
        if (L(n)->kind == sql_alias_bind &&
            LR(n)->kind == sql_alias &&
            LR(n)->sem.alias.name == alias)
            /* Ignore checking the rest of the list as
               aliases have to be unique anyway. */
            *n = *R(n);
        else if (R(n)->kind == sql_alias_bind &&
                 RR(n)->kind == sql_alias &&
                 RR(n)->sem.alias.name == alias)
            /* Ignore checking the rest of the list as
               aliases have to be unique anyway. */
            *n = *L(n);
        else {
            base_rel_remove (L(n), alias);
            base_rel_remove (R(n), alias);
        }
    }
}

/**
 * Traverse a SQL structure and replace all occurrences of the old alias
 * @a old_alias the new one (@a new_alias).
 */
static void
replace_alias (PFsql_t *n, PFsql_aident_t old_alias, PFsql_aident_t new_alias)
{
    if (n->kind == sql_column_name &&
        n->sem.column.alias == old_alias)
        /* Replace old alias by the new one. */
        n->sem.column.alias = new_alias;
    else {
        if (L(n)) replace_alias (L(n), old_alias, new_alias);
        if (R(n)) replace_alias (R(n), old_alias, new_alias);
        /* We need this check for the between operator, which has 3 kids. */
        if (n->child[2]) replace_alias (n->child[2], old_alias, new_alias);
    }
}

/**
 * Remove duplicate references to the document relation with the
 * same document node lookup. This is allowed as the document node
 * filter ensures a cardinality of 1.
 */
static void
remove_duplicate_documentnode_references (PFsql_t *n)
{
    unsigned int i, j;
    PFsql_aident_t new_alias, old_alias;
    PFsql_t *where;
    PFarray_t *aliases   = PFarray (sizeof (PFsql_t *)),
              *names     = PFarray (sizeof (PFsql_t *)),
              *tmp_names = PFarray (sizeof (PFsql_t *));
    
    assert (n->kind == sql_select);

    where = n->child[2]; 

    /* collect the document relations that refer to document nodes */
    where_traverse (where, aliases, check_doc_alias, NULL);
    
    for (i = 0; i < PFarray_last (aliases); i++) {
        old_alias = (node_list_at (aliases, i))->sem.column.alias;
        char *cur_name;

        /* initialize name field */
        node_list_at (names, i) = NULL;
        
        /* lookup the document names */
        where_traverse (where, tmp_names, check_string_pred, &old_alias);
        /* ignore doc relations that have multiple or no names */
        if (PFarray_last (tmp_names) != 1) {
            PFarray_last (tmp_names) = 0;
            continue;
        }

        cur_name = (node_list_at (tmp_names, 0))->sem.atom.val.s;
        assert (cur_name);
        
        /* check for duplicate references to document relations */
        for (j = 0; j < i; j++)
            if (node_list_at (names, j) &&
                !strcmp ((node_list_at (names, j))->sem.atom.val.s,
                         cur_name)) {
                /* remove duplicate reference of the doc relation */
                new_alias = (node_list_at (aliases, j))->sem.column.alias;

                /* relink the alias in the SELECT list */
                replace_alias (L(n), old_alias, new_alias);
                /* remove the document reference from the FROM list */
                base_rel_remove (R(n), old_alias);
                /* remove the duplicate doc_tbl tests */
                where_pred_remove (where, check_docnode, &old_alias);
                /* relink the alias in the WHERE list */
                replace_alias (where, old_alias, new_alias);
                /* relink the alias in the ORDER BY list */
                if (n->child[3])
                    replace_alias (n->child[3], old_alias, new_alias);
                /* relink the alias in the GROUP BY list */
                if (n->child[4])
                    replace_alias (n->child[4], old_alias, new_alias);
                break;
            }
        /* we have found a new name add it to the list of names */
        if (i == j)
            node_list_at (names, i) = node_list_at (tmp_names, 0);

        /* reset tmp_names */
        PFarray_last (tmp_names) = 0;
    }
}

/**
 * Optimize a single SELECT clause.
 */
static void
optimize_select (PFsql_t *n)
{
    assert (n->kind == sql_select);

    if (!n->child[2])
        return;

    /* delete documentnode duplication */
    remove_duplicate_documentnode_references (n);
}

/**
 * Traverse the SQL structure until we reach a SELECT operator.
 */
static void
sql_opt (PFsql_t *n)
{
    switch (n->kind) {
        case sql_root:
        case sql_bind:
            sql_opt (R(n));
            break;

        case sql_with:
        case sql_cmmn_tbl_expr:
        case sql_union:
        case sql_diff:
            sql_opt (L(n));
            sql_opt (R(n));
            break;

        case sql_alias_bind:
            sql_opt (L(n));
            break;

        case sql_select:
            optimize_select (n);
            break;

        default:
            break;
    }
}

/**
 * Optimize the generated SQL code
 */
PFsql_t *
PFsql_opt (PFsql_t *n)
{
    sql_opt (n);
    return n;
}

/* vim:set shiftwidth=4 expandtab: */
