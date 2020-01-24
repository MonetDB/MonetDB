static inline void CONCAT6(scan_loop_raw_, TPE, _, CAND_ITER, _, TEST)
(const bool has_nil, const bool anti, MOStask* task, BUN first, BUN last, TPE tl, TPE th, bool li, bool hi)
{
    (void) has_nil;
    (void) anti;
    (void) task;
    (void) tl;
    (void) th;
    (void) li;
    (void) hi;

    oid* o = task->lb;
    TPE *val= &GET_INIT_raw(task, TPE);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
        BUN i = (BUN) (c - first);
        TPE v = val[i];
        (void) v;
        /*TODO: change from control to data dependency.*/
        if (CONCAT2(_, TEST))
            *o++ = c;
    }
    task->lb = o;
}
