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
