static inline void CONCAT6(scan_loop_runlength_, TPE, _, CAND_ITER, _, TEST)
(const bool has_nil, const bool anti, MOStask* task, BUN first, BUN last, TPE tl, TPE th, bool li, bool hi)
{
    (void) has_nil;
    (void) anti;
    (void) task;
    (void) first;
    (void) last;
    (void) tl;
    (void) th;
    (void) li;
    (void) hi;

    oid* o = task->lb;
    TPE v = GET_VAL_runlength(task, TPE);
    (void) v;
    if (CONCAT2(_, TEST)) {
        for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
		    *o++ = c;
        }
    }
    task->lb = o;
}
