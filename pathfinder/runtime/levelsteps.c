/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

#include <gdk.h>

/**
 * @file
 *
 * This file contains all axis step algoritms that work mainly on the
 * level table:
 *
 * - child step
 * - parent step
 * - following-sibling
 * - preceding-sibling
 * 
 * All 4 algorithms use basically the same approach:
 * 
 * - evaluation is done during a single sequential scan of the
 *   level table and the context set
 * - for every context node an exact defined region is scanned for
 *   nodes that qualify a certain level condition (to be parents, 
 *   childs or siblings) 
 * - for context nodes on descendant/ancestor axis a stack is used
 *   to avoid interruption of sequential scanning
 *
 * @note
 * All 4 algorithms need the presence of the complete level table.
 * hence, this axis steps do not represent joins on 2 independent sets,
 * as in case of the staircase join.  
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2003 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */


int
PFchild_lev (BAT** res, BAT* level, BAT* context, int* height, int* upperbound)
{
    /* --------------------------- declaration ----------------------------- */

    int ctx_bunsize, res_bunsize, stack_top;
    BUN ctx_cur, ctx_last, res_cur;
    oid ctx_next_pre, level_cur, level_last;
    chr search_level, *level_val, *stack; 

    /* result bat allocation. for result size use upperbound parameter */
    BAT *result = BATnew(TYPE_oid, TYPE_void, *upperbound);


    /* --------------------------- special cases --------------------------- */
    if (!(BAThordered (level) & 1))
    {
        GDKerror("PFchild_lev: %s must be ordered on head.\n",
                 BBP_logical(level->batCacheid));
        return GDK_FAIL;
    }
    if (BATcount(context) == 0 || BATcount (level) == 0)
    {
        *res = BATnew(TYPE_oid, TYPE_void, 0);
        return GDK_SUCCEED;
    }
    if (result == NULL) 
    { 
        GDKerror("PFchild_lev: "
                 "could not allocate a result BAT of size %d.\n", *upperbound);
        return GDK_FAIL;
    }


    /* ----------------- initialization of table cursors ------------------- */

    ctx_bunsize = BUNsize(context);
    res_bunsize = BUNsize(result);

    ctx_cur = BUNfirst(context);
    ctx_last = BUNlast(context) - ctx_bunsize;
    ctx_next_pre = *(oid*) BUNhead(context, ctx_cur);

    res_cur = BUNfirst(result);

    level_cur = ctx_next_pre;
    level_last = level->hseqbase + BATcount(level) -1;

    /* definition for "array-like" access on level values of given nodes */
    level_val = ((chr*) BUNfirst(level)) - level->hseqbase;

    /* for the child step search level is the level of the context node + 1 */
    search_level = level_val[level_cur] + 1;

    /* stack for context set nodes will never grow over height of doc. tree */
    stack = GDKmalloc(sizeof(chr) * (*height));
    stack_top = 0;

    ALGODEBUG
        THRprintf(GDKout, "PFchild_lev: "
                          "level %u buns, context %u buns, height %u\n",
                          BATcount(level), BATcount(context), *height);

    level_cur++;
    ctx_cur += ctx_bunsize;


    /* ---------------------------- main part ----------------------------- 
     * - the outer loop traverses all context nodes
     * - the inner loop traverses the defined search region of the current
     *   context node
     *
     * search region: if we select a context node v in a complete level table, 
     * the first node w following v in ascending preorder with 
     *                         level(w) <= level(v)
     * indicates the end of the descendants of v. 
     * within this block we collect all nodes y with 
     *                       level(y) = search-level(v). 
     */

outer_loop:  
    while (ctx_cur <= ctx_last)
    {
        ctx_next_pre = *(oid*) BUNhead(context, ctx_cur);

        /* check all descendants of current context node 
         */
        while (level_val[level_cur] >= search_level)
        { 
            /* copy children to result
             */
            if (level_val[level_cur] == search_level)
            {
                *(oid *)BUNhloc(result, res_cur) = level_cur;
                res_cur += res_bunsize;
            }

            if (level_cur == ctx_next_pre) 
            { 
                stack[stack_top++] = search_level;
                search_level = level_val[level_cur] + 1;
                ctx_cur += ctx_bunsize;
                level_cur++;
                goto outer_loop;
            }
            level_cur++;
        }

        if (stack_top > 0)
            search_level = stack[--stack_top];
        else 
        {
            level_cur = ctx_next_pre;
            search_level = level_val[level_cur] + 1;
            level_cur++;
            ctx_cur += ctx_bunsize;
        }
    }   

    /* now we only have to collect the children of the last context node
     * and of context nodes that still live on stack
     */

    while (1)
    {
        /* check all descendants of current context node 
         */
        while (level_cur <= level_last && level_val[level_cur] >= search_level)
        { 
            /* copy children to result
             */
            if (level_val[level_cur] == search_level)
            {
                *(oid *)BUNhloc(result, res_cur) = level_cur;
                res_cur += res_bunsize;
            }
            level_cur++;
        }  
        if (stack_top > 0)
            search_level = stack[--stack_top];
        else
            break;
    }   

    /* ---------------------------- end of main part --------------------------
     * tidy up and propagate result properties
     */

    GDKfree(stack);

    /* mark the end point of the BUNs section in the BUNheap 
     */
    result->batBuns->free = res_cur - result->batBuns->base;

    result->batDirty = TRUE;
    result->hsorted = GDK_SORTED;
    BATkey(result,TRUE);
    BATseqbase(BATmirror(result), oid_nil);
    *res = result;

    return GDK_SUCCEED;
}












int
PFparent_lev(BAT** res, BAT* level, BAT* context, int* height, int* upperbound)
{
    /* --------------------------- declaration ----------------------------- */

    int ctx_bunsize, res_bunsize, stack_top;
    BUN ctx_cur, ctx_last, res_cur;
    oid ctx_next_pre, level_cur/*, level_last*//* StM: not used in this case */;
    chr search_level, *level_val, *stack; 

    /* result bat allocation. for result size use the upperbound parameter */
    BAT *result = BATnew(TYPE_oid, TYPE_void, *upperbound);


    /* --------------------------- special cases --------------------------- */
    if (!(BAThordered (level) & 1))
    {
        GDKerror("PFparent_lev: %s must be ordered on head.\n",
                 BBP_logical(level->batCacheid));
        return GDK_FAIL;
    }
    if (BATcount (context) == 0 || BATcount (level) == 0)
    {
        *res = BATnew(TYPE_oid, TYPE_void, 0);
        return GDK_SUCCEED;
    }
    if (result == NULL) 
    { 
        GDKerror("PFparent_lev: "
                 "could not allocate a result BAT of size %d.\n", *upperbound);
        return GDK_FAIL;
    } 


    /* ----------------- initialization of table cursors ------------------- */

    ctx_bunsize = BUNsize(context);
    res_bunsize = BUNsize(result);

    /* reverse processing of context nodes and level table */
    ctx_cur = BUNlast(context) - ctx_bunsize;
    ctx_last = BUNfirst(context);
    ctx_next_pre = *(oid*) BUNhead(context, ctx_cur);

    /* for reverse insert */
    result->batBuns->free = (*upperbound) * res_bunsize;
    res_cur = BUNlast(result) - res_bunsize;

    level_cur = ctx_next_pre;
    /*level_last = level->hseqbase;*//* StM: */ /* not used in this case */

    /* definition for "array-like" access on level values of given nodes */
    level_val = ((chr*) BUNfirst(level)) - level->hseqbase;

    /* for the parent step search level is the level of the context node - 1 */
    search_level = level_val[level_cur] - 1;

    /* stack for context set nodes will never grow over height of doc. tree */
    stack = GDKmalloc(sizeof(chr) * (*height));
    stack_top = 0;

    ALGODEBUG
        THRprintf(GDKout, "PFparent_lev: "
                          "level %u buns, context %u buns, height %u\n",
                          BATcount(level), BATcount(context), *height);

    /* since root has no parent it can be left out to avoid level_cur running
     * out of bounds
     */
    if (*(oid*) BUNhead(context, ctx_last) == 0)
    {
        if (ctx_cur == ctx_last)
        {
            *res = BATnew(TYPE_oid, TYPE_void, 0);
            return GDK_SUCCEED;
        }
        ctx_last += ctx_bunsize;
    }   

    level_cur--;
    ctx_cur -= ctx_bunsize;


    /* ---------------------------- main part ----------------------------- 
     * - the outer loop traverses all context nodes
     * - the inner loop traverses the defined search region of the current
     *   context node
     *
     * search region: if we select a context node v in a complete level table, 
     * the first node w following v in descending(!) preorder with 
     *             level(w) = level(v) - 1 = search-level(v)
     * is the searched parent of v. since every node has only one parent 
     * the search ends at this point. 
     */

outer_loop:  
    while (ctx_cur >= ctx_last)
    {
        ctx_next_pre = *(oid*) BUNhead(context, ctx_cur);

        /* skip all preceding-siblings of current context node
         * and their descendants 
         */
        while (level_val[level_cur] > search_level)
        { 
            if (level_cur == ctx_next_pre) 
            { 
                if (level_val[level_cur] - 1 != search_level)
                {
                    stack[stack_top++] = search_level;
                    search_level = level_val[level_cur] - 1;
                }   
                ctx_cur -= ctx_bunsize;
                level_cur--;
                goto outer_loop;
            }
            level_cur--;
        }

        /* copy parent node to result */
        *(oid *)BUNhloc(result, res_cur) = level_cur;
        res_cur -= res_bunsize;

        if (stack_top > 0)
            search_level = stack[--stack_top];
        else 
        {
            level_cur = ctx_next_pre;
            search_level = level_val[level_cur] - 1;
            ctx_cur -= ctx_bunsize;
        }
    }   

    /* now we only have to search for the parent of the last context node
     * and of nodes that still live on stack
     */
    while (1)
    {
        while (level_val[level_cur] > search_level)
        { 
            level_cur--;
        }  
        *(oid *)BUNhloc(result, res_cur) = level_cur;
        res_cur -= res_bunsize;

        if (stack_top > 0)
            search_level = stack[--stack_top];
        else
            break;
    }   

    /* ---------------------------- end of main part --------------------------
     * tidy up and propagate result properties
     */

    GDKfree(stack);

    /* set the real starting point of the inserted BUNs;
     * here, we use a feature of the BATdescriptor that has been
     * introduced for delta- (i.e., insertion- & deletion-) management:
     * the "real" BUNs section doesn't start at b->batBuns->base,
     * but only at b->batHole
     */
    result->batDeleted = result->batHole
                       = BUNptr(result, BUNindex(result, res_cur));

    result->batDirty = TRUE;
    result->hsorted = GDK_SORTED;
    BATkey(result, TRUE);
    BATseqbase(BATmirror(result), oid_nil);
    *res = result;

    return GDK_SUCCEED;
}












int
PFfollowingsibling_lev (BAT** res, BAT* level, BAT* context,
                        int* height, int* upperbound)
{
    /* --------------------------- declaration ----------------------------- */

    int ctx_bunsize, res_bunsize, stack_top;
    BUN ctx_cur, ctx_last, res_cur;
    oid ctx_next_pre, level_cur, level_last;
    chr search_level, *level_val, *stack; 

    /* result bat allocation. for result size use the upperbound parameter */
    BAT *result = BATnew(TYPE_oid, TYPE_void, *upperbound);


    /* --------------------------- special cases --------------------------- */
    if (!(BAThordered (level) & 1))
    {
        GDKerror("PFfollowingsibling_lev: %s must be ordered on head.\n",
                 BBP_logical(level->batCacheid));
        return GDK_FAIL;
    }
    if (BATcount (context) == 0 || BATcount (level) == 0)
    {
        *res = BATnew(TYPE_oid, TYPE_void, 0);
        return GDK_SUCCEED;
    }
    if (result == NULL) 
    { 
        GDKerror("PFfollowingsibling_lev: "
                 "could not allocate a result BAT of size %d.\n", *upperbound);
        return GDK_FAIL;
    }


    /* ----------------- initialization of table cursors ------------------- */

    ctx_bunsize = BUNsize(context);
    res_bunsize = BUNsize(result);

    ctx_cur = BUNfirst(context);
    ctx_last = BUNlast(context) - ctx_bunsize;
    ctx_next_pre = *(oid*) BUNhead(context, ctx_cur);

    res_cur = BUNfirst(result);

    level_cur = ctx_next_pre;
    level_last = level->hseqbase + BATcount(level) -1;

    /* definition for "array-like" access on level values of given nodes */
    level_val = ((chr*) BUNfirst(level)) - level->hseqbase;

    /* for the siblings the search level is the level of the context node */
    search_level = level_val[level_cur];

    /* stack for context set nodes will never grow over height of doc. tree */
    stack = GDKmalloc(sizeof(chr) * (*height));
    stack_top = 0;

    ALGODEBUG
        THRprintf(GDKout, "PFfollowingsibling_lev: "
                          "level %u buns, context %u buns, height %u\n",
                          BATcount(level), BATcount(context), *height);

    level_cur++;
    ctx_cur += ctx_bunsize;


    /* ---------------------------- main part ----------------------------- 
     * - the outer loop traverses all context nodes
     * - the inner loop traverses the defined search region of the current
     *   context node
     *
     * search region: if we select a context node v in a complete level table, 
     * the first node w following v in ascending preorder with 
     *                         level(w) < level(v)
     * indicates the end of the descendants of parent(v), thus the end of the
     * region to look for siblings.
     * within this block we collect all nodes y with 
     *                       level(y) = search-level(v). 
     */

outer_loop:  
    while (ctx_cur <= ctx_last)
    {
        ctx_next_pre = *(oid*) BUNhead(context, ctx_cur);

        /* check search-region of current context node 
        */
        while (level_val[level_cur] >= search_level)
        { 
            /* copy siblings to result
            */
            if (level_val[level_cur] == search_level)
            {
                *(oid *)BUNhloc(result, res_cur) = level_cur;
                res_cur += res_bunsize;
            }

            if (level_cur == ctx_next_pre) 
            { 
                if (level_val[level_cur] != search_level)
                {
                    stack[stack_top++] = search_level;
                    search_level = level_val[level_cur];
                }   
                ctx_cur += ctx_bunsize;
                level_cur++;
                goto outer_loop;
            }
            level_cur++;
        }

        if (stack_top > 0)
            search_level = stack[--stack_top];
        else 
        {
            level_cur = ctx_next_pre;
            search_level = level_val[level_cur];
            level_cur++;
            ctx_cur += ctx_bunsize;
        }
    }   

    /* now we only have to collect the siblings of the last context node
     * and of context nodes that still live on stack
     */

    while (1)
    {
        /* check search-region of current context node 
        */
        while (level_cur <= level_last && level_val[level_cur] >= search_level)
        { 
            /* copy siblings to result
            */
            if (level_val[level_cur] == search_level)
            {
                *(oid *)BUNhloc(result, res_cur) = level_cur;
                res_cur += res_bunsize;
            }
            level_cur++;
        }  
        if (stack_top > 0)
            search_level = stack[--stack_top];
        else
            break;
    }   

    /* ---------------------------- end of main part --------------------------
     * tidy up and propagate result properties
     */

    GDKfree(stack);

    /* mark the end point of the BUNs section in the BUNheap 
    */
    result->batBuns->free = res_cur - result->batBuns->base;

    result->batDirty = TRUE;
    result->hsorted = GDK_SORTED;
    BATkey(result,TRUE);
    BATseqbase(BATmirror(result), oid_nil);
    *res = result;

    return GDK_SUCCEED;
}












int
PFprecedingsibling_lev (BAT** res, BAT* level, BAT* context,
                        int* height, int* upperbound)
{
    /* --------------------------- declaration ----------------------------- */

    int ctx_bunsize, res_bunsize, stack_top;
    BUN ctx_cur, ctx_last, res_cur;
    oid ctx_next_pre, level_cur, level_last;
    chr search_level, *level_val, *stack; 

    /* result bat allocation. for result size use the upperbound parameter */
    BAT *result = BATnew(TYPE_oid, TYPE_void, *upperbound);


    /* --------------------------- special cases --------------------------- */
    if (!(BAThordered (level) & 1))
    {
        GDKerror("PFprecedingsibling_lev: %s must be ordered on head.\n",
                 BBP_logical (level->batCacheid));
        return GDK_FAIL;
    }
    if (BATcount (context) == 0 || BATcount (level) == 0)
    {
        *res = BATnew (TYPE_oid, TYPE_void, 0);
        return GDK_SUCCEED;
    }
    if (result == NULL) 
    { 
        GDKerror("PFprecedingsibling_lev: "
                 "could not allocate a result BAT of size %d.\n", *upperbound);
        return GDK_FAIL;
    }


    /* ----------------- initialization of table cursors ------------------- */

    ctx_bunsize = BUNsize (context);
    res_bunsize = BUNsize (result);

    /* reverse processing of context nodes and level table */
    ctx_cur = BUNlast (context) - ctx_bunsize;
    ctx_last = BUNfirst (context);
    ctx_next_pre = *(oid*) BUNhead (context, ctx_cur);

    /* for reverse insert */
    result->batBuns->free = (*upperbound) * res_bunsize;
    res_cur = BUNlast(result) - res_bunsize;

    level_cur = ctx_next_pre;
    level_last = level->hseqbase;

    /* definition for "array-like" access on level values of given nodes */
    level_val = ((chr*) BUNfirst (level)) - level->hseqbase;

    /* for the siblings the search level is the level of the context node */
    search_level = level_val[level_cur];

    /* stack for context set nodes will never grow over height of doc. tree */
    stack = GDKmalloc(sizeof(chr) * (*height));
    stack_top = 0;

    ALGODEBUG
        THRprintf(GDKout, "PFprecedingsibling_lev: "
                          "level %u buns, context %u buns, height %u\n",
                          BATcount (level), BATcount (context), *height);

    level_cur--;
    ctx_cur -= ctx_bunsize;


    /* ---------------------------- main part ----------------------------- 
     * - the outer loop traverses all context nodes
     * - the inner loop traverses the defined search region of the current
     *   context node
     *
     * search region: if we select a context node v in a complete level table, 
     * the first node w following v in descending(!) preorder with 
     *                         level(w) < level(v)
     * indicates the end of the descendants of parent(v), thus the end of the
     * region to look for siblings.
     * within this block we collect all nodes y with 
     *                       level(y) = search-level(v). 
     */

outer_loop:  
    while (ctx_cur >= ctx_last)
    {
        ctx_next_pre = *(oid*) BUNhead(context, ctx_cur);

        /* check search-region of current context node 
         */
        while (level_val[level_cur] >= search_level)
        { 
            /* copy siblings to result
             */
            if (level_val[level_cur] == search_level)
            {
                *(oid *)BUNhloc(result, res_cur) = level_cur;
                res_cur -= res_bunsize;
            }

            if (level_cur == ctx_next_pre) 
            { 
                if (level_val[level_cur] != search_level)
                {
                    stack[stack_top++] = search_level;
                    search_level = level_val[level_cur];
                }   
                ctx_cur -= ctx_bunsize;
                level_cur--;
                goto outer_loop;
            }
            level_cur--;
        }

        if (stack_top > 0)
            search_level = stack[--stack_top];
        else 
        {
            level_cur = ctx_next_pre;
            search_level = level_val[level_cur];
            level_cur--;
            ctx_cur -= ctx_bunsize;
        }
    }   

    /* now we only have to collect the siblings of the last context node
     * and of context nodes that still live on stack
     */

    while (1)
    {
        /* check search-region of current context node 
         */
        while ((int)level_cur > (int)level_last
               && level_val[level_cur] >= search_level)
        { 
            /* copy siblings to result
            */
            if (level_val[level_cur] == search_level)
            {
                *(oid *)BUNhloc(result, res_cur) = level_cur;
                res_cur -= res_bunsize;
            }
            level_cur--;
        }  
        if (stack_top > 0)
            search_level = stack[--stack_top];
        else
            break;
    }   

    /* ---------------------------- end of main part --------------------------
     * tidy up and propagate result properties
     */

    GDKfree (stack);

    /* set the real starting point of the inserted BUNs;
     * here, we use a feature of the BATdescriptor that has been
     * introduced for delta- (i.e., insertion- & deletion-) management:
     * the "real" BUNs section doesn't start at b->batBuns->base,
     * but only at b->batHole
     */
    result->batDeleted = result->batHole
                       = BUNptr (result, BUNindex (result, res_cur));

    result->batDirty = TRUE;
    result->hsorted = GDK_SORTED;
    BATkey (result,TRUE);
    BATseqbase (BATmirror (result), oid_nil);
    *res = result;

    return GDK_SUCCEED;
}

/* vim:set shiftwidth=4 expandtab: */
