#ifdef COMPRESSION_DEFINITION
MOSadvance_SIGNATURE(METHOD, TPE)
{
	MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) (task)->blk;
	BUN cnt		= MOSgetCnt(task->blk);

	assert(cnt > 0);
	assert(MOSgetTag(task->blk) == MOSAIC_FRAME);

	task->start += (oid) cnt;

	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	blk += BitVectorSize(cnt, parameters->bits);
	blk += GET_PADDING(task->blk, METHOD, TPE);

	task->blk = (MosaicBlk) blk;
}

static inline void CONCAT2(determineDeltaParameters, TPE)
(MOSBlockHeaderTpe(METHOD, TPE)* parameters, TPE* src, BUN limit) {
	TPE *val = src, max, min;
	bte bits = 1;
	unsigned int i;
	max = *val;
	min = *val;
	/*TODO: add additional loop to find best bit wise upper bound*/
	for(i = 0; i < limit; i++, val++){
		TPE current_max = max;
		TPE current_min = min;
		bool evaluate_bits = false;
		if (*val > current_max) {
			current_max = *val;
			evaluate_bits = true;
		}
		if (*val < current_min) {
			current_min = *val;
			evaluate_bits = true;
		}
		if (evaluate_bits) {
		 	DeltaTpe(TPE) width = GET_DELTA(TPE, current_max, current_min);
			bte current_bits = bits;
			while (width > ((DeltaTpe(TPE))(-1)) >> (sizeof(DeltaTpe(TPE)) * CHAR_BIT - current_bits) ) {/*keep track of number of BITS necessary to store difference*/
				current_bits++;
			}
			if ( (current_bits >= (int) ((sizeof(TPE) * CHAR_BIT) / 2))
				/*TODO: this extra condition should be removed once bitvector is extended to int64's*/
				|| (current_bits > (int) sizeof(BitVectorChunk) * CHAR_BIT) ) {
				/*If we can from here on not compress better then the half of the original data type, we give up. */
				break;
			}
			max = current_max;
			min = current_min;
			bits = current_bits;
		}
	}
	parameters->min = min;
	parameters->bits = bits;
	parameters->rec.cnt = i;
}

MOSestimate_SIGNATURE(METHOD, TPE)
{
	(void) previous;
	current->is_applicable = true;
	current->compression_strategy.tag = MOSAIC_FRAME;
	TPE *src = getSrc(TPE, task);
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;
	MOSBlockHeaderTpe(METHOD, TPE) parameters;
	CONCAT2(determineDeltaParameters, TPE)(&parameters, src, limit);
	assert(parameters.rec.cnt > 0);/*Should always compress.*/
	current->uncompressed_size += (BUN) (parameters.rec.cnt * sizeof(TPE));
	current->compressed_size += 2 * sizeof(MOSBlockHeaderTpe(METHOD, TPE)) + wordaligned((parameters.rec.cnt * parameters.bits) / CHAR_BIT, lng);
	current->compression_strategy.cnt = (unsigned int) parameters.rec.cnt;

	if (parameters.rec.cnt > *current->max_compression_length ) {
		*current->max_compression_length = parameters.rec.cnt;
	}

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

	MosaicBlk blk = task->blk;
	MOSsetTag(blk,MOSAIC_FRAME);
	MOSsetCnt(blk, 0);
	TPE *src = getSrc(TPE, task);
	TPE delta;
	BUN i = 0;
	BUN limit = estimate->cnt;
	MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) (task)->blk;
    CONCAT2(determineDeltaParameters, TPE)(parameters, src, limit);
	BitVector base = MOScodevectorFrame(task, TPE);
	task->dst = (char*) base;
	for(i = 0; i < MOSgetCnt(task->blk); i++, src++) {
		/*TODO: assert that delta's actually does not cause an overflow. */
		delta = *src - parameters->min;
		setBitVector(base, i, parameters->bits, (BitVectorChunk) /*TODO: fix this once we have increased capacity of bitvector*/ delta);
	}
	task->dst += BitVectorSize(i, parameters->bits);
}

MOSdecompress_SIGNATURE(METHOD, TPE)
{
	MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) (task)->blk;
	BUN lim = MOSgetCnt(task->blk);
    TPE min = parameters->min;
	BitVector base = (BitVector) MOScodevectorFrame(task, TPE);
	BUN i;
	for(i = 0; i < lim; i++){
		TPE delta = getBitVector(base, i, parameters->bits);
		/*TODO: assert that delta's actually does not cause an overflow. */
		TPE val = min + delta;
		((TPE*)task->src)[i] = val;
	}
	task->src += i * sizeof(TPE);
}

#define LAYOUT_BUFFER_SIZE 10000

MOSlayout_SIGNATURE(METHOD, TPE)
{
	size_t compressed_size = 0;
	compressed_size += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	lng cnt = (lng) MOSgetCnt(task->blk);

	MOSBlockHeaderTpe(METHOD, TPE)* hdr = (MOSBlockHeaderTpe(METHOD, TPE)*) task->blk;

	compressed_size += BitVectorSize(cnt, hdr->bits);
	compressed_size += GET_PADDING(task->blk, METHOD, TPE);

	char buffer[LAYOUT_BUFFER_SIZE] = {0};

	char* pbuffer = &buffer[0];

	size_t buffer_size = LAYOUT_BUFFER_SIZE;

	char final_properties[1000] = {0};

	strcat(final_properties, "{");
		strcat(final_properties, "\"min\" : ");
			CONCAT2(TPE, ToStr)(&pbuffer, &buffer_size, &hdr->min, true);
			strcat(final_properties, buffer);
			memset(buffer, 0, buffer_size);
	strcat(final_properties, ",");
		strcat(final_properties, "\"offset size in bits\" : ");
			bteToStr(&pbuffer, &buffer_size, (const bte*) &hdr->bits, true);
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

#undef LAYOUT_BUFFER_SIZE
#endif /*def TPE*/

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
	MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) task->blk;
    TPE min = parameters->min;
	BitVector base = (BitVector) MOScodevectorFrame(task, TPE);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
        BUN i = (BUN) (c - first);
        TPE delta = getBitVector(base, i, parameters->bits);
        TPE v = ADD_DELTA(TPE, min, delta);
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

    MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) ((task))->blk;
	TPE METHOD =  parameters->min;
	BitVector base = (BitVector) MOScodevectorFrame(task, TPE);
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CAND_ITER(task->ci)) {
		BUN i = (BUN) (o - first);
		TPE w = ADD_DELTA(TPE, METHOD, getBitVector(base, i, parameters->bits));
		*bt++ = w;
		task->cnt++;
	}

	task->src = (char*) bt;
}
#endif
