#ifdef COMPRESSION_DEFINITION
MOSadvance_SIGNATURE(METHOD, TPE)
{
	MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) (task)->blk;
	BUN cnt = MOSgetCnt(task->blk);

	assert(cnt > 0);
	assert(MOSgetTag(task->blk) == MOSAIC_DELTA);

	task->start += (oid) cnt;

	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	blk += BitVectorSize(cnt, parameters->bits);
	blk += GET_PADDING(task->blk, METHOD, TPE);

	task->blk = (MosaicBlk) blk;

}

static inline void CONCAT2(determineDeltaParameters, TPE)
(MOSBlockHeaderTpe(METHOD, TPE)* parameters, TPE* src, BUN limit) {
	TPE *val = src;
	bte bits = 1;
	unsigned int i;
	DeltaTpe(TPE) unsigned_delta = 0;
	TPE prev_val;
	parameters->init = *val;

	for(i = 1; i < limit; i++){
		prev_val = *val++;
		DeltaTpe(TPE) current_unsigned_delta;
		if (*val > prev_val) {
			current_unsigned_delta = GET_DELTA(TPE, *val, prev_val);
		}
		else {
			current_unsigned_delta = GET_DELTA(TPE, prev_val, *val);
		}

		if (current_unsigned_delta > unsigned_delta) {
			bte current_bits = bits;
			while (current_unsigned_delta > ((DeltaTpe(TPE))(-1)) >> (sizeof(DeltaTpe(TPE)) * CHAR_BIT - current_bits) ) {
				/*keep track of number of BITS necessary to store the difference*/
				current_bits++;
			}
			int current_bits_with_sign_bit = current_bits + 1;
			if ( (current_bits_with_sign_bit >= (int) ((sizeof(TPE) * CHAR_BIT) / 2))
				/*If we can from here on not compress better then the half of the original data type, we give up. */
				|| (current_bits_with_sign_bit > (int) sizeof(BitVectorChunk) * CHAR_BIT) ) {
				/*TODO: this extra condition should be removed once bitvector is extended to int64's*/
				break;
			}
			bits = current_bits;
			unsigned_delta = current_unsigned_delta;
		}
	}

	/*Add the additional sign bit to the bit count.*/
	bits++;
	parameters->rec.cnt = i;
	parameters->bits = bits;
}

MOSestimate_SIGNATURE(METHOD, TPE)
{
	(void) previous;
	current->is_applicable = true;
	current->compression_strategy.tag = MOSAIC_DELTA;
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
	MOSsetTag(blk,MOSAIC_DELTA);
	MOSsetCnt(blk, 0);
	TPE *src = getSrc(TPE, task);
	BUN i = 0;
	BUN limit = estimate->cnt;
	MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) (task)->blk;
	CONCAT2(determineDeltaParameters, TPE)(parameters, src, limit);
	BitVector base = MOScodevectorDelta(task, TPE);
	task->dst = (char*) base;
	TPE pv = parameters->init; /*previous value*/
	/*Initial METHOD is zero.*/
	setBitVector(base, 0, parameters->bits, (BitVectorChunk) 0);
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (parameters->bits - 1);

	for(i = 1; i < MOSgetCnt(task->blk); i++) {
		/*TODO: assert that METHOD's actually does not cause an overflow. */
		TPE cv = *++src; /*current value*/
		DeltaTpe(TPE) METHOD = (DeltaTpe(TPE)) (cv > pv ? (IPTpe(TPE)) (cv - pv) : (IPTpe(TPE)) ((sign_mask) | (IPTpe(TPE)) (pv - cv)));
		setBitVector(base, i, parameters->bits, (BitVectorChunk) /*TODO: fix this once we have increased capacity of bitvector*/ METHOD);
		pv = cv;
	}
	task->dst += BitVectorSize(i, parameters->bits);
}

MOSdecompress_SIGNATURE(METHOD, TPE)
{
	MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) (task)->blk;
	BUN lim = MOSgetCnt(task->blk);
	((TPE*)task->src)[0] = parameters->init; /*previous value*/
	BitVector base = (BitVector) MOScodevectorDelta(task, TPE);
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (parameters->bits - 1);
	DeltaTpe(TPE) acc = (DeltaTpe(TPE)) parameters->init /*unsigned accumulating value*/;
	BUN i;
	for(i = 0; i < lim; i++) {
		DeltaTpe(TPE) METHOD = getBitVector(base, i, parameters->bits);
		((TPE*)task->src)[i] = ACCUMULATE(acc, METHOD, sign_mask, TPE);
	}
	task->src += i * sizeof(TPE);
}
#endif

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
            TPE METHOD = getBitVector(base, j, bits);
			v = ACCUMULATE(acc, METHOD, sign_mask, TPE);
        }
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

	MOSBlockHeaderTpe(METHOD, TPE)* parameters = (MOSBlockHeaderTpe(METHOD, TPE)*) task->blk;
	BitVector base = (BitVector) MOScodevectorDelta(task, TPE);
	DeltaTpe(TPE) acc = (DeltaTpe(TPE)) parameters->init; /*previous value*/
	const bte bits = parameters->bits;
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (bits - 1);
    TPE v = (TPE) acc;
    BUN j = 0;
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CAND_ITER(task->ci)) {
        BUN i = (BUN) (o - first);
        for (;j <= i; j++) {
            TPE METHOD = getBitVector(base, j, bits);
			v = ACCUMULATE(acc, METHOD, sign_mask, TPE);
        }
		*bt++ = v;
		task->cnt++;
	}

	task->src = (char*) bt;
}
#endif

#ifdef LAYOUT_DEFINITION

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
		strcat(final_properties, "{\"init\" : ");
			CONCAT2(TPE, ToStr)(&pbuffer, &buffer_size, &hdr->init, true);
			strcat(final_properties, buffer);
			memset(buffer, 0, buffer_size);
		strcat(final_properties, "\"}");
	strcat(final_properties, ",");
		strcat(final_properties, "{\"bits per delta\" : ");
			bteToStr(&pbuffer, &buffer_size, (const bte*) &hdr->bits, true);
			strcat(final_properties, buffer);
			memset(buffer, 0, buffer_size);
		strcat(final_properties, "\"}");
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
