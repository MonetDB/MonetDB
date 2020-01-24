static inline void CONCAT2(CONCAT4(scan_loop_, NAME, _, TPE), CONCAT4(_, CAND_ITER, _, TEST))
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
    TPE* dict = GET_FINAL_DICT(task, NAME, TPE);
	BitVector base = MOScodevectorDict(task, NAME, TPE);
    bte bits = GET_FINAL_BITS(task, NAME);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
        BUN i = (BUN) (c - first);
        BitVectorChunk j = getBitVector(base,i,bits); 
        TPE v = dict[j];
        (void) v;
        /*TODO: change from control to data dependency.*/
        if (CONCAT2(_, TEST))
            *o++ = c;
    }
    task->lb = o;
}
