/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

#include <gdk.h>


/**********************************************************************
 * this file contains functions for result size estimation on XPath 
 * axes.
 * all function named *_tag exploit tag-name specific information in 
 * the case we know that all nodes within ctx have the same tag-name. 
 */ 

int
PFestimate_desc (int* res, BAT* ctx, BAT* prepost, BAT* level)
{
    /* -------------------- declarations ---------------------- */

    int ctx_size = BATcount (ctx);
    int limit1 = ctx_size, limit2 = 0;
    float fac;

    int bunsize = BUNsize (ctx);
    BUN ctx_cur = BUNfirst (ctx);
    BUN ctx_last;

    int pre, post;
    int max_post = 0;
    int result = 0;

    oid *post_val = ((oid*) BUNfirst(prepost)) - prepost->hseqbase;
    chr *lev_val  = ((chr*) BUNfirst(level)) - level->hseqbase;


    /* -------------------- special cases --------------------- */

    if (ctx_size == 0)
    {
        *res = result;
        return GDK_SUCCEED;
    }   

    /* ------------------ cutting out sample ------------------ */

    if (ctx_size > 200) 
    { 
        limit2 = limit1 = 100;
    }  

    fac = (float) ctx_size / (float) (limit1 + limit2);


    /* ---------------- descendant calculation ---------------- */

    ctx_last = ctx_cur + (bunsize * limit1);

    for (; ctx_cur < ctx_last; ctx_cur += bunsize) 
    {
        pre = *(int *) BUNhead(ctx, ctx_cur);

        /* pruning on the fly avoiding unneccessary post_val lookups*/
        if (max_post < pre && max_post < (post = (int) post_val[pre]))
        {
            max_post = post;
            result += post - pre + (int) lev_val[pre] - 1;
        } 
    }

    ctx_last = BUNlast(ctx);
    ctx_cur = ctx_last - (bunsize * limit2);

    for (; ctx_cur < ctx_last; ctx_cur += bunsize) 
    {
        pre = *(int *) BUNhead(ctx, ctx_cur);

        /* pruning on the fly avoiding unneccessary post_val lookups*/
        if (max_post < pre && max_post < (post = (int) post_val[pre]))
        {
            max_post = post;
            result += post - pre + (int) lev_val[pre] - 1;
        } 
    }


    /* ------------------ result adaptation ------------------- */

    result = (int)(result * fac);
    *res = MIN(result, BATcount(prepost) - 1);
    return GDK_SUCCEED;
}

float
parents_of_group (float g_ctx, int g_size, int g_par) 
{
    float g_fan = (float)g_size / (float)g_par;
    float i;
    float tmp, est = 1.0;

    /* special cases (in this order!) */
    if (g_ctx <= 1.0 || g_fan < 1.1) return g_ctx;
    if (g_par <= 1 || g_ctx > g_size - g_fan) return (float)g_par;

    if (g_ctx > g_fan)
    {
        tmp = g_size - g_fan + 1;
        for (i = 0.0; i < g_fan; i++){
            est *= 1.0 - (g_ctx / (tmp + i));
        }	  
        est = 1.0 - est;
        return est * g_par;
    }
    else
    {
        tmp = g_size - g_ctx + 1;
        for (i = 0.0; i < g_ctx; i++)
            est *= 1.0 - (g_fan / (tmp + i));
        est = 1.0 - est;
        return est * g_par;
    }

    /* never reached case */
    return 0.0;
}


int
PFestimate_par_tag (int* res, BAT* ctx, int* tag_size, int* tag_par)
{
    /* -------------------- declarations ---------------------- */

    int ctx_size = BATcount(ctx);
    int result;

    /* ------------------ parent calculation ------------------ */

    result = (int) parents_of_group ((float)ctx_size, *tag_size, *tag_par);

    *res = result;
    return GDK_SUCCEED;
}


int
PFestimate_par (int* res, BAT* ctx,
                BAT* group_histogram, BAT* group_parents, int* doc_size)
{
    /* -------------------- declarations ---------------------- */

    int ctx_size = BATcount (ctx);
    int g_num = BATcount(group_histogram);
    int i;

    float ctx_fac;
    float result = 0, g_res;

    int *group_histogram_val = ((int*) BUNfirst(group_histogram))
                               - group_histogram->hseqbase;
    int *group_parents_val = ((int*) BUNfirst(group_parents))
                             - group_parents->hseqbase;

    /* -------------------- special cases --------------------- */

    if (ctx_size == 0)
    {
        *res = result;
        return GDK_SUCCEED;
    }   

    /* ------------------ parent calculation ------------------ */

    ctx_fac = (float)ctx_size / (float)(*doc_size);
    for (i = 0; i < g_num; i++)
    {
        g_res = parents_of_group(ctx_fac * group_histogram_val[i], 
                group_histogram_val[i], 
                group_parents_val[i]);
        result += g_res;
    }

    *res = (int)result;
    return GDK_SUCCEED;
}

int
PFestimate_pfs(int* res, BAT* ctx, BAT* group_histogram, BAT* group_parents, 
               int* doc_size)
{
    /* -------------------- declarations ---------------------- */

    int ctx_size = BATcount (ctx);
    int g_num = BATcount (group_histogram);
    int i;

    float ctx_fac, g_fan, g_probe;
    float result = 0, g_res;

    int *group_histogram_val = ((int*) BUNfirst (group_histogram))
                               - group_histogram->hseqbase;
    int *group_parents_val = ((int*) BUNfirst (group_parents))
                             - group_parents->hseqbase;

    /* -------------------- special cases --------------------- */

    if (ctx_size == 0)
    {
        *res = result;
        return GDK_SUCCEED;
    }   

    /* ------------------ parent calculation ------------------ */

    ctx_fac = (float)ctx_size / (float)(*doc_size);
    for (i = 0; i < g_num; i++)
    {
        g_fan = (float)group_histogram_val[i] / (float)group_parents_val[i];
        if (g_fan > 1)
        {
            g_res = parents_of_group (ctx_fac * group_histogram_val[i],
                                      group_histogram_val[i],
                                      group_parents_val[i]);

            g_probe = g_res / (ctx_fac * group_histogram_val[i]);
            g_res *= (((g_fan + 1) / (g_probe + 1)) - 1);

            result += g_res;
        }
    }

    *res = (int)result;
    return GDK_SUCCEED;
}

int
PFestimate_pfs_tag(int* res, BAT* ctx, int* tag_size, int* tag_par,
                   int* tag_pfs, int* tag_pfs1)
{
    /* -------------------- declarations ---------------------- */

    int ctx_size = BATcount (ctx);
    int result = 0;
    float fac1, fac2, fac_pos, fac;
    float est_par, tag_sib, fac_prune;

    /* --------------------- calculation ---------------------- */

    if (*tag_par == 0)
    {
        *res = result;
        return GDK_SUCCEED;
    }

    est_par = parents_of_group ((float)ctx_size, *tag_size, *tag_par);
    tag_sib = (float)*tag_size / (float)*tag_par;
    fac_prune = est_par ? (float)ctx_size / est_par : 0;

    fac1 = (float)*tag_pfs1 / (float)*tag_size; 
    fac2 = (float)*tag_pfs / (float)*tag_par;
    fac_pos = (tag_sib - 1)
        ? ((tag_sib - fac_prune) * 2) / ((fac_prune + 1) * (tag_sib - 1))
        : 0;
    fac = fac2 - (fac_pos * (fac2 - fac1));

    /* in this case fan-outs differ highly and we thus probably have
     * overestimated the parent step */
    if (fac1 > fac2)
    {
        fac1 = (est_par / (float)*tag_par);
        est_par += (((fac1 * fac1) - fac1) * est_par);
    }

    result = (int)(est_par * fac);

    *res = result;
    return GDK_SUCCEED;
}

int
PFestimate_anc1(int* res, BAT* ctx, BAT* prepost, BAT* level, 
                BAT* level_histogram, BAT* level_parents, int* maxdepth)
{
    /* -------------------- declarations ---------------------- */

    int i,j;

    int ctx_size = BATcount(ctx);
    int bunsize = BUNsize(ctx);
    BUN ctx_cur = BUNlast(ctx) - bunsize;
    BUN ctx_last;

    int pre, post;
    int min_post = BATcount(level) + 1;
    int cntr = 0;
    float move = 1.0;
    int ctx_level[(*maxdepth) + 1];
    float sample_fac;

    oid *post_val = ((oid*) BUNfirst(prepost)) - prepost->hseqbase;
    chr *lev_val  = ((chr*) BUNfirst(level)) - level->hseqbase;
    int *level_histogram_val = ((int*) BUNfirst(level_histogram))
                               - level_histogram->hseqbase;
    int *level_parents_val   = ((int*) BUNfirst(level_parents))
                               - level_parents->hseqbase;

    float lev_ctx_fac;
    float result = 0.0;
    float lev_res = 0.0;

    for (j = 0; j <= (*maxdepth); j++) ctx_level[j] = 0;

    /* -------------------- special cases --------------------- */

    if (ctx_size == 0)
    {
        *res = result;
        return GDK_SUCCEED;
    }   


    /* -------------------- sampling part --------------------- 
     * pruning and counting nodes on each level
     */

    ctx_last = ctx_cur - ((bunsize / 2) * ctx_size);

    for (; ctx_cur > ctx_last; move *= 1.1, ctx_cur -= (int)move * bunsize) 
    {
        cntr++;
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    move = 1.0;
    ctx_last = BUNfirst(ctx);

    for (; ctx_cur > ctx_last; move *= 1.1, ctx_cur -= (int)move * bunsize) 
    {
        cntr++;
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    /* sampling rate */
    sample_fac = (float)ctx_size / (float)(cntr);


    /* ------------------ ancestor calculation ---------------- 
     * level by level calculation
     * within a level all histogram groups belonging to it are evaluated 
     */
    j = (*maxdepth);
    for (i = BATcount(level_histogram) -2; i >= 0; i--)
    {
        lev_ctx_fac = MIN((lev_res + (ctx_level[j] * sample_fac))
                      / level_histogram_val[i], 1.0);
        i--;
        lev_res = 0.0;
        for (; level_histogram_val[i] != -1; i--)
        {
            lev_res += parents_of_group(level_histogram_val[i] * lev_ctx_fac,
                                        level_histogram_val[i], 
                                        level_parents_val[i]); 
        }
        result += lev_res;
        j--;
    }

    *res = (int)result;
    return GDK_SUCCEED;
}

int
PFestimate_anc2 (int* res, BAT* ctx, BAT* level_histogram,
                 BAT* level_parents, BAT* level_pruned,
                 int* maxdepth, int* doc_size)
{
    /* -------------------- declarations ---------------------- */

    int i, j;
    int ctx_size = BATcount(ctx);

    int *level_histogram_val = ((int*) BUNfirst(level_histogram))
                               - level_histogram->hseqbase;
    int *level_parents_val   = ((int*) BUNfirst(level_parents))
                               - level_parents->hseqbase;
    int *level_pruned_val    = ((int*) BUNfirst(level_pruned))
                               - level_pruned->hseqbase;

    float ctx_fac;
    float lev_ctx_fac;
    float result = 0.0;
    float lev_res = 0.0;

    (void)maxdepth;
    /* -------------------- special cases --------------------- */

    if (ctx_size == 0)
    {
        *res = result;
        return GDK_SUCCEED;
    }   


    /* ------------------ ancestor calculation ---------------- 
     * level by level calculation
     * within a level all histogram groups belonging to it are evaluated 
     */

    /* printf("start\n"); */
    ctx_fac = (float)ctx_size / (float)(*doc_size);
    j = BATcount (level_pruned) - 1;
    for (i = BATcount (level_histogram) -2; i >= 0; i--)
    {
        lev_ctx_fac = (lev_res + (ctx_fac * level_pruned_val[j]))
                      / level_histogram_val[i]; 
        /* printf("%f\n", lev_ctx_fac); */
        i--; j--;
        lev_res = 0.0;
        for (; level_histogram_val[i] != -1; i--)
        {
            lev_res += parents_of_group(level_histogram_val[i] * lev_ctx_fac,
                                        level_histogram_val[i], 
                                        level_parents_val[i]); 
        }
        result += lev_res;
    }

    *res = (int)result;
    return GDK_SUCCEED;
}

int
PFdistr_anc (BAT** res, BAT* ctx, BAT* prepost, BAT* level, int* maxdepth)
{
    int j;

    int bunsize = BUNsize(ctx);
    BUN ctx_cur = BUNlast(ctx) - bunsize;
    BUN ctx_last = BUNfirst(ctx);

    int pre, post;
    int min_post = BATcount(prepost) + 1;
    int ctx_level[(*maxdepth) + 1];

    oid *post_val = ((oid*) BUNfirst(prepost)) - prepost->hseqbase;
    chr *lev_val  = ((chr*) BUNfirst(level)) - level->hseqbase;

    BAT *result = BATnew(TYPE_int, TYPE_void, *maxdepth);
    BUN res_cur = BUNfirst(result);
    int res_bunsize = BUNsize(result);

    for (j = 0; j <= (*maxdepth); j++) ctx_level[j] = 0;


    /* -------------------- sampling part --------------------- 
     * pruning and counting nodes on each level
     */

    for (; ctx_cur > ctx_last; ctx_cur -= bunsize) 
    {
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    for (j = 0; j <= (*maxdepth); j++)
    {
        *(int *)BUNhloc(result, res_cur) = ctx_level[j];
        res_cur += res_bunsize;
    }

    result->batBuns->free = res_cur - result->batBuns->base;
    result->batDirty = TRUE;
    BATkey(result,TRUE);
    BATseqbase(BATmirror(result), oid_nil);
    *res = result;

    return GDK_SUCCEED;
}

int
PFhisto_anc(BAT** res, BAT* ctx, BAT* anc_histo, int* doc_size, int* maxdepth)
{
    int j;

    int ctx_size = BATcount(ctx);
    int *anc_histo_val = ((int*) BUNfirst(anc_histo)) - anc_histo->hseqbase;
    float fac = (float)ctx_size / (float)(*doc_size);

    BAT *result = BATnew(TYPE_int, TYPE_void, *maxdepth);
    BUN res_cur = BUNfirst(result);
    int res_bunsize = BUNsize(result);

    for (j = 0; j <= (*maxdepth); j++)
    {
        *(int *)BUNhloc(result, res_cur) = anc_histo_val[j] * fac;
        res_cur += res_bunsize;
    }

    result->batBuns->free = res_cur - result->batBuns->base;
    result->batDirty = TRUE;
    BATkey(result,TRUE);
    BATseqbase(BATmirror(result), oid_nil);
    *res = result;

    return GDK_SUCCEED;
}

int
PFsample_anc1(BAT** res, BAT* ctx, BAT* prepost, BAT* level, int* maxdepth)
{
    int j;

    int ctx_size = BATcount(ctx);
    int bunsize = BUNsize(ctx);
    BUN ctx_cur = BUNlast(ctx) - bunsize;
    BUN ctx_last;

    int pre, post;
    int min_post = BATcount(prepost) + 1;
    int cntr = 0;
    float move = 1.0;
    int ctx_level[(*maxdepth) + 1];
    float sample_fac;

    oid *post_val = ((oid*) BUNfirst(prepost)) - prepost->hseqbase;
    chr *lev_val  = ((chr*) BUNfirst(level)) - level->hseqbase;

    BAT *result = BATnew(TYPE_int, TYPE_void, *maxdepth);
    BUN res_cur = BUNfirst(result);
    int res_bunsize = BUNsize(result);

    for (j = 0; j <= (*maxdepth); j++) ctx_level[j] = 0;


    /* -------------------- sampling part --------------------- 
     * pruning and counting nodes on each level
     */

    ctx_last = ctx_cur - ((bunsize / 2) * ctx_size);

    for (; ctx_cur > ctx_last; move *= 1.1, ctx_cur -= (int)move * bunsize) 
    {
        cntr++;
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    move = 1.0;
    ctx_last = BUNfirst(ctx);

    for (; ctx_cur > ctx_last; move *= 1.1, ctx_cur -= (int)move * bunsize) 
    {
        cntr++;
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    /* sampling rate */
    sample_fac = (float)ctx_size / (float)(cntr);

    for (j = 0; j <= (*maxdepth); j++)
    {
        *(int *)BUNhloc(result, res_cur) = (int)(ctx_level[j] * sample_fac);
        res_cur += res_bunsize;
    }

    result->batBuns->free = res_cur - result->batBuns->base;
    result->batDirty = TRUE;
    BATkey(result,TRUE);
    BATseqbase(BATmirror(result), oid_nil);
    *res = result;

    return GDK_SUCCEED;
}

int
PFsample_anc2 (BAT** res, BAT* ctx, BAT* prepost, BAT* level, int* maxdepth)
{
    int j;

    int ctx_size = BATcount(ctx);
    int bunsize = BUNsize(ctx);
    BUN ctx_cur = BUNlast(ctx) - bunsize;
    BUN ctx_last;

    int limit1 = ctx_size, limit2 = 0;
    int pre, post;
    int min_post = BATcount(prepost) + 1;
    int ctx_level[(*maxdepth) + 1];
    float sample_fac;

    oid *post_val = ((oid*) BUNfirst(prepost)) - prepost->hseqbase;
    chr *lev_val  = ((chr*) BUNfirst(level)) - level->hseqbase;

    BAT *result = BATnew(TYPE_int, TYPE_void, *maxdepth);
    BUN res_cur = BUNfirst(result);
    int res_bunsize = BUNsize(result);

    for (j = 0; j <= (*maxdepth); j++) ctx_level[j] = 0;


    /* -------------------- sampling part --------------------- 
     * pruning and counting nodes on each level
     */
    if (ctx_size > 200) 
    { 
        limit2 = limit1 = 100;
    }  

    ctx_last = ctx_cur - (bunsize * limit1);

    for (; ctx_cur > ctx_last; ctx_cur -= bunsize) 
    {
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    ctx_last = BUNfirst(ctx);
    ctx_cur = ctx_last + (bunsize * limit2);

    for (; ctx_cur > ctx_last; ctx_cur -= bunsize) 
    {
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    /* sampling rate */
    sample_fac = (float)ctx_size / (float)(limit1 + limit2);

    for (j = 0; j <= (*maxdepth); j++)
    {
        *(int *)BUNhloc(result, res_cur) = (int)(ctx_level[j] * sample_fac);
        res_cur += res_bunsize;
    }

    result->batBuns->free = res_cur - result->batBuns->base;
    result->batDirty = TRUE;
    BATkey(result,TRUE);
    BATseqbase(BATmirror(result), oid_nil);
    *res = result;

    return GDK_SUCCEED;
}


int
PFestimate_anc_tag(int* res, BAT* ctx, BAT* prepost, BAT* level, 
                   BAT* level_histogram, BAT* level_parents,
                   int* maxdepth, int* tag_size, int* tag_par)
{
    /* -------------------- declarations ---------------------- */

    int i,j;

    int ctx_size = BATcount(ctx);
    int bunsize = BUNsize(ctx);
    BUN ctx_cur = BUNlast(ctx) - bunsize;
    BUN ctx_last;

    int pre, post;
    int min_post = BATcount(level) + 1;
    int cntr = 0;
    float move = 1.0;
    int ctx_level[(*maxdepth) + 1];
    float sample_fac;

    oid *post_val = ((oid*) BUNfirst(prepost)) - prepost->hseqbase;
    chr *lev_val  = ((chr*) BUNfirst(level)) - level->hseqbase;
    int *level_histogram_val = ((int*) BUNfirst(level_histogram)) 
                               - level_histogram->hseqbase;
    int *level_parents_val   = ((int*) BUNfirst(level_parents))
                               - level_parents->hseqbase;

    float lev_ctx_fac;
    float result = 0.0;
    float lev_res = 0.0;
    float tag_res;
    int lev_par;
    int lev_size;

    for (j = 0; j <= (*maxdepth); j++) ctx_level[j] = 0;

    /* -------------------- special cases --------------------- */

    if (ctx_size == 0)
    {
        *res = result;
        return GDK_SUCCEED;
    }   


    /* -------------------- sampling part --------------------- 
     * pruning and counting nodes on each level
     */

    ctx_last = ctx_cur - ((bunsize / 2) * ctx_size);

    for (; ctx_cur > ctx_last; move *= 1.1, ctx_cur -= (int)move * bunsize) 
    {
        cntr++;
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    move = 1.0;
    ctx_last = BUNfirst(ctx);

    for (; ctx_cur > ctx_last; move *= 1.1, ctx_cur -= (int)move * bunsize) 
    {
        cntr++;
        pre = *(int *)BUNhead(ctx, ctx_cur);
        post = (int)post_val[pre];
        if (min_post > post)
        {
            min_post = post;
            ctx_level[(int)lev_val[pre]]++;
        } 
    }

    /* sampling rate */
    sample_fac = (float)ctx_size / (float)(cntr);


    /* ------------------ ancestor calculation ---------------- 
     * level by level calculation
     * within a level all histogram groups belonging to it are evaluated 
     */
    j = (*maxdepth);
    for (i = BATcount(level_histogram) -2; i >= 0; i--)
    {
        lev_size = level_histogram_val[i];
        lev_par = level_parents_val[i];
        tag_res = parents_of_group(MIN(ctx_level[j] * sample_fac,
                                       (float)lev_size), 
                                   MIN(*tag_size, lev_size),
                                   MIN(*tag_par, lev_par));
        lev_ctx_fac = lev_res / lev_size;
        lev_res = 0.0;
        i--;
        for (; level_histogram_val[i] != -1; i--)
            lev_res += parents_of_group(level_histogram_val[i] * lev_ctx_fac,
                                        level_histogram_val[i], 
                                        level_parents_val[i]);
        lev_res = MAX(lev_res, tag_res);
        result += lev_res;
        j--;
    }

    *res = (int)result;
    return GDK_SUCCEED;
}

/* vim:set shiftwidth=4 expandtab: */
