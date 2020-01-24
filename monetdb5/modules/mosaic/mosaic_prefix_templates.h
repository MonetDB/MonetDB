static inline void CONCAT6(scan_loop_prefix_, TPE, _, CAND_ITER, _, TEST)
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
	MOSBlockHeaderTpe(prefix, TPE)* parameters = (MOSBlockHeaderTpe(prefix, TPE)*) task->blk;
	BitVector base = (BitVector) MOScodevectorPrefix(task, TPE);
	PrefixTpe(TPE) prefix = parameters->prefix;
	bte suffix_bits = parameters->suffix_bits;
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
        BUN i = (BUN) (c - first);
        TPE v = (TPE) (prefix | getBitVector(base,i,suffix_bits));
        (void) v;
        /*TODO: change from control to data dependency.*/
        if (CONCAT2(_, TEST))
            *o++ = c;
    }
    task->lb = o;
}
