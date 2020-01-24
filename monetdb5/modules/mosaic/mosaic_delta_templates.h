static inline void CONCAT6(scan_loop_delta_, TPE, _, CAND_ITER, _, TEST)
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
    MOSBlockHeaderTpe(delta, TPE)* parameters = (MOSBlockHeaderTpe(delta, TPE)*) task->blk;
	BitVector base = (BitVector) MOScodevectorDelta(task, TPE);
	DeltaTpe(TPE) acc = (DeltaTpe(TPE)) parameters->init; /*previous value*/
	const bte bits = parameters->bits;
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);
    TPE v = (TPE) acc;
    (void) v;
    BUN j = 0;
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
        BUN i = (BUN) (c - first);
        for (;j <= i; j++) {
            TPE delta = getBitVector(base, j, bits);
			v = ACCUMULATE(acc, delta, sign_mask, TPE);
        }
        /*TODO: change from control to data dependency.*/
        if (CONCAT2(_, TEST))
            *o++ = c;
    }
    task->lb = o;
}
