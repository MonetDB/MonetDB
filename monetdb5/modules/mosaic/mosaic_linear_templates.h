#ifdef COMPRESSION_DEFINITION
MOSadvance_SIGNATURE(linear, TPE)
{
	task->start += MOSgetCnt(task->blk);
	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(linear, TPE));
	blk += GET_PADDING(task->blk, linear, TPE);

	task->blk = (MosaicBlk) blk;
}

MOSestimate_SIGNATURE(linear, TPE)
{
	(void) previous;
	current->compression_strategy.tag = MOSAIC_LINEAR;
	TPE *c = ((TPE*) task->src)+task->start; /*(c)urrent value*/
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;
	BUN i = 1;
	if (limit > 1 ){
		TPE *p = c++; /*(p)revious value*/
		DeltaTpe(TPE) step = GET_DELTA(TPE, *c, *p);
		for( ; i < limit; i++, p++, c++) {
			DeltaTpe(TPE) current_step = GET_DELTA(TPE, *c, *p);
			if (  current_step != step)
				break;
		}
	}
	assert(i > 0);/*Should always compress.*/
	current->is_applicable = true;
	current->uncompressed_size += (BUN) (i * sizeof(TPE));
	current->compressed_size += 2 * sizeof(MOSBlockHeaderTpe(linear, TPE));
	current->compression_strategy.cnt = (unsigned int) i;

	if (i > *current->max_compression_length ) {
		*current->max_compression_length = i;
	}

	return MAL_SUCCEED;
}

MOSpostEstimate_SIGNATURE(linear, TPE)
{
	(void) task;
}

MOScompress_SIGNATURE(linear, TPE)
{
	ALIGN_BLOCK_HEADER(task,  linear, TPE);

	MOSsetTag(task->blk,MOSAIC_LINEAR);
	TPE *c = ((TPE*) task->src)+task->start; /*(c)urrent value*/
	TPE step = 0;
	BUN limit = estimate->cnt;
	linear_offset(TPE, task) = *(DeltaTpe(TPE)*) c;
	if (limit > 1 ){
		TPE *p = c++; /*(p)revious value*/
		step = (TPE) GET_DELTA(TPE, *c, *p);
	}
	MOSsetCnt(task->blk, limit);
	linear_step(TPE, task) = (DeltaTpe(TPE)) step;
	task->dst = ((char*) task->blk)+ wordaligned(MosaicBlkSize +  2 * sizeof(TPE),MosaicBlkRec);
}

MOSdecompress_SIGNATURE(linear, TPE)
{
	MosaicBlk blk =  task->blk;
	BUN i;
	DeltaTpe(TPE) val	= linear_offset(TPE, task);
	DeltaTpe(TPE) step	= linear_step(TPE, task);
	BUN lim = MOSgetCnt(blk);
	for(i = 0; i < lim; i++, val += step) {
		((TPE*)task->src)[i] = (TPE) val;
	}
	task->src += i * sizeof(TPE);
}
#endif

#ifdef SCAN_LOOP_DEFINITION
MOSscanloop_SIGNATURE(linear, TPE, CAND_ITER, TEST)
{
    (void) has_nil;
    (void) anti;
    (void) task;
    (void) tl;
    (void) th;
    (void) li;
    (void) hi;

    oid* o = task->lb;
	DeltaTpe(TPE) offset	= linear_offset(TPE, task);
	DeltaTpe(TPE) step		= linear_step(TPE, task);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
        BUN i = (BUN) (c - first);
        TPE v = (TPE) (offset + (i * step));
        (void) v;
        /*TODO: change from control to data dependency.*/
        if (CONCAT2(_, TEST))
            *o++ = c;
    }
    task->lb = o;
}
#endif

#ifdef PROJECTION_LOOP_DEFINITION
MOSprojectionloop_SIGNATURE(linear, TPE, CAND_ITER)
{
    (void) first;
    (void) last;

	TPE* bt= (TPE*) task->src;

	DeltaTpe(TPE) offset	= linear_offset(TPE, task) ;
	DeltaTpe(TPE) step		= linear_step(TPE, task);
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CAND_ITER(task->ci)) {
        BUN i = (BUN) (o - first);
		TPE value =  (TPE) (offset + (i * step));
		*bt++ = value;
		task->cnt++;
	}

	task->src = (char*) bt;
}
#endif
