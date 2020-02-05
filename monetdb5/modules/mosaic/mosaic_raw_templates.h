#ifdef COMPRESSION_DEFINITION
MOSadvance_SIGNATURE(raw, TPE)
{
	BUN cnt = MOSgetCnt(task->blk);
	task->start += MOSgetCnt(task->blk);

	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(raw, TPE));
	blk += cnt * sizeof(TPE);
	blk += GET_PADDING(task->blk, raw, TPE);

	task->blk = (MosaicBlk) blk;
}

MOSestimate_SIGNATURE(raw, TPE)
{
	/*The raw compression technique is always applicable and only adds one item at a time.*/
	(void) task;
	current->compression_strategy.tag = MOSAIC_RAW;
	current->is_applicable = true;
	current->uncompressed_size += (BUN) sizeof(TPE);
	unsigned int cnt = previous->compression_strategy.cnt;
	if (previous->is_applicable && previous->compression_strategy.tag == MOSAIC_RAW && (cnt + 1 < MOSAICMAXCNT)) {
		current->must_be_merged_with_previous = true;
		cnt++;
		current->previous_compressed_size = previous->previous_compressed_size;
		current->compressed_size += sizeof(TPE);
	}
	else {
		current->must_be_merged_with_previous = false;
		cnt = 1;
		current->compressed_size += sizeof(TPE);
	}
	current->compression_strategy.cnt = cnt;

	return MAL_SUCCEED;
}

MOSpostEstimate_SIGNATURE(raw, TPE)
{
	(void) task;
}

// rather expensive simple value non-compressed store
MOScompress_SIGNATURE(raw, TPE)
{
	ALIGN_BLOCK_HEADER(task, raw, TPE);

	MosaicBlk blk = (MosaicBlk) task->blk;
	MOSsetTag(blk, MOSAIC_RAW);
	TPE *v = ((TPE*)task->src) + task->start;
	BUN cnt = estimate->cnt;
	TPE *d = &GET_INIT_raw(task, TPE);
	for(BUN i = 0; i < cnt; i++,v++){
		*d++ = (TPE) *v;
	}
	task->dst += sizeof(TPE);
	MOSsetCnt(blk,cnt);
}

MOSdecompress_SIGNATURE(raw, TPE)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	TPE* val = &GET_INIT_raw(task, TPE);
	TPE* dst = (TPE*) task->src;
	BUN lim = MOSgetCnt(blk);
	for(i = 0; i < lim; i++) {
	dst[i] = val[i]; 
	}
	task->src += i * sizeof(TPE);
}
#endif

#ifdef SCAN_LOOP_DEFINITION
MOSscanloop_SIGNATURE(raw, TPE, CAND_ITER, TEST)
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
#endif

#ifdef PROJECTION_LOOP_DEFINITION
MOSprojectionloop_SIGNATURE(raw, TPE, CAND_ITER)
{
    (void) first;
    (void) last;

	TPE* bt= (TPE*) task->src;

	TPE *rt;
	rt = &GET_INIT_raw(task, TPE);
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CAND_ITER(task->ci)) {
		BUN i = (BUN) (o - first);
		*bt++ = rt[i];
		task->cnt++;
	}

	task->src = (char*) bt;
}
#endif

#ifdef LAYOUT_DEFINITION

#define LAYOUT_BUFFER_SIZE 10000

MOSlayout_SIGNATURE(METHOD, TPE)
{
	lng _count = (lng) MOSgetCnt(task->blk);
	size_t compressed_size = 0;
	compressed_size += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	compressed_size += (lng) (_count - 1) * sizeof(TPE);
	compressed_size += GET_PADDING(task->blk, METHOD, TPE);

	LAYOUT_INSERT(
		bsn = current_bsn;
		tech = STRINGIFY(METHOD);
		count = _count;
		input = (lng) (_count * sizeof(TPE));
		output = (lng) compressed_size;
		);

	return MAL_SUCCEED;
}

#undef LAYOUT_BUFFER_SIZE
#endif
