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
	current->compressed_size += 2 * sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	current->compression_strategy.cnt = (unsigned int) i;

	if (i > *current->max_compression_length ) {
		*current->max_compression_length = i;
	}

	return MAL_SUCCEED;
}

MOSpostEstimate_SIGNATURE(METHOD, TPE)
{
	(void) task;
}

MOScompress_SIGNATURE(METHOD, TPE)
{
	ALIGN_BLOCK_HEADER(task,  METHOD, TPE);

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

MOSdecompress_SIGNATURE(METHOD, TPE)
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


MOSlayout_SIGNATURE(METHOD, TPE)
{
	size_t compressed_size = 0;
	compressed_size += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	compressed_size += GET_PADDING(task->blk, METHOD, TPE);

	MOSBlockHeaderTpe(METHOD, TPE)* hdr = (MOSBlockHeaderTpe(METHOD, TPE)*) task->blk;

	char buffer[LAYOUT_BUFFER_SIZE] = {0};

	char* pbuffer = &buffer[0];

	size_t buffer_size = LAYOUT_BUFFER_SIZE;

	char final_properties[1000] = {0};

	strcat(final_properties, "{");
		strcat(final_properties, "\"offset\" : ");
			CONCAT2(TPE, ToStr)(&pbuffer, &buffer_size, (TPE*) &hdr->offset, true);
			strcat(final_properties, buffer);
			memset(buffer, 0, buffer_size);
	strcat(final_properties, ",");
		strcat(final_properties, "\"step\" : ");
			bteToStr(&pbuffer, &buffer_size, (const bte*) (TPE*) &hdr->step, true);
			strcat(final_properties, buffer);
			memset(buffer, 0, buffer_size);
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
#endif /*def COMPRESSION_DEFINITION*/

#ifdef SCAN_LOOP_DEFINITION
MOSscanloop_SIGNATURE(METHOD, TPE, CAND_ITER, TEST)
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
MOSprojectionloop_SIGNATURE(METHOD, TPE, CAND_ITER)
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
