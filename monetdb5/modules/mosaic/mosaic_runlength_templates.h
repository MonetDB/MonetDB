#ifdef COMPRESSION_DEFINITION
MOSadvance_SIGNATURE(runlength, TPE)
{
	task->start += MOSgetCnt(task->blk);

	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(runlength, TPE));
	blk += GET_PADDING(task->blk, runlength, TPE);

	task->blk = (MosaicBlk) blk;
}

MOSestimate_SIGNATURE(runlength, TPE)
{	unsigned int i = 0;
	(void) previous;
	current->compression_strategy.tag = MOSAIC_RLE;
	bool nil = !task->bsrc->tnonil;
	TPE *v = ((TPE*) task->src) + task->start, val = *v;
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;
	for(v++,i = 1; i < limit; i++,v++) if ( !ARE_EQUAL(*v, val, nil, TPE) ) break;
	assert(i > 0);/*Should always compress.*/
	current->is_applicable = true;
	current->uncompressed_size += (BUN) (i * sizeof(TPE));
	current->compressed_size += 2 * sizeof(MOSBlockHeaderTpe(runlength, TPE));
	current->compression_strategy.cnt = i;

	if (i > *current->max_compression_length ) *current->max_compression_length = i;

	return MAL_SUCCEED;
}

MOSpostEstimate_SIGNATURE(runlength, TPE)
{
	(void) task;
}

// rather expensive simple value non-compressed store
MOScompress_SIGNATURE(runlength, TPE)
{
	ALIGN_BLOCK_HEADER(task,  runlength, TPE);

	(void) estimate;
	BUN i ;
	bool nil = !task->bsrc->tnonil;

	MosaicBlk blk = task->blk;
	MOSsetTag(blk, MOSAIC_RLE);
	TPE *v = ((TPE*) task->src)+task->start, val = *v;
	TPE *dst = &GET_VAL_runlength(task, TPE);
	BUN limit = task->stop - task->start > MOSAICMAXCNT ? MOSAICMAXCNT: task->stop - task->start;
	*dst = val;
	for(v++, i =1; i<limit; i++,v++)
	if ( !ARE_EQUAL(*v, val, nil, TPE))
		break;
	MOSsetCnt(blk, i);
	task->dst +=  sizeof(TPE);
}

MOSdecompress_SIGNATURE(runlength, TPE)
{
	TPE val = GET_VAL_runlength(task, TPE);
	BUN lim = MOSgetCnt(task->blk);

	BUN i;
	for(i = 0; i < lim; i++)
		((TPE*)task->src)[i] = val;
	task->src += i * sizeof(TPE);
}
#endif

#ifdef SCAN_LOOP_DEFINITION
MOSscanloop_SIGNATURE(runlength, TPE, CAND_ITER, TEST)
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
#endif

#ifdef PROJECTION_LOOP_DEFINITION
MOSprojectionloop_SIGNATURE(runlength, TPE, CAND_ITER)
{
    (void) first;
    (void) last;

	TPE* bt= (TPE*) task->src;

	TPE rt = GET_VAL_runlength(task, TPE);
	for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
		*bt++ = rt;
		task->cnt++;
	}

	task->src = (char*) bt;
}
#endif
