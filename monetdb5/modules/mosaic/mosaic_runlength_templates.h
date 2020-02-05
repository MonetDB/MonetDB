#ifdef COMPRESSION_DEFINITION
MOSadvance_SIGNATURE(METHOD, TPE)
{
	task->start += MOSgetCnt(task->blk);

	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	blk += GET_PADDING(task->blk, METHOD, TPE);

	task->blk = (MosaicBlk) blk;
}

MOSestimate_SIGNATURE(METHOD, TPE)
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
	current->compressed_size += 2 * sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	current->compression_strategy.cnt = i;

	if (i > *current->max_compression_length ) *current->max_compression_length = i;

	return MAL_SUCCEED;
}

MOSpostEstimate_SIGNATURE(METHOD, TPE)
{
	(void) task;
}

// rather expensive simple value non-compressed store
MOScompress_SIGNATURE(METHOD, TPE)
{
	ALIGN_BLOCK_HEADER(task,  METHOD, TPE);

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

MOSdecompress_SIGNATURE(METHOD, TPE)
{
	TPE val = GET_VAL_runlength(task, TPE);
	BUN lim = MOSgetCnt(task->blk);

	BUN i;
	for(i = 0; i < lim; i++)
		((TPE*)task->src)[i] = val;
	task->src += i * sizeof(TPE);
}

#define LAYOUT_BUFFER_SIZE 10000

MOSlayout_SIGNATURE(METHOD, TPE)
{
	size_t compressed_size = 0;
	compressed_size += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	compressed_size += GET_PADDING(task->blk, METHOD, TPE);

	char rle_value[LAYOUT_BUFFER_SIZE] = {0};

	char* prle_value = &rle_value[0];

	size_t buffer_size = LAYOUT_BUFFER_SIZE;

	MOSBlockHeaderTpe(METHOD, TPE)* hdr = (MOSBlockHeaderTpe(METHOD, TPE)*) task->blk;

	CONCAT2(TPE, ToStr)(&prle_value, &buffer_size, &hdr->val, true);

	char final_properties[1000] = {0};

	strcat(final_properties, "{\"value\" : ");
	strcat(final_properties, rle_value);
	strcat(final_properties, "}");

	LAYOUT_INSERT(
		bsn = current_bsn;
		tech = STRINGIFY(METHOD);
		count = (lng) MOSgetCnt(task->blk);
		input = (lng) count * sizeof(TPE);
		output = (lng) compressed_size;
		properties = final_properties;
		);

	return MAL_SUCCEED;
}

#undef LAYOUT_BUFFER_SIZE
#endif

#ifdef SCAN_LOOP_DEFINITION
MOSscanloop_SIGNATURE(METHOD, TPE, CAND_ITER, TEST)
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
MOSprojectionloop_SIGNATURE(METHOD, TPE, CAND_ITER)
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
