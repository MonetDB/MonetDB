MOSadvance_SIGNATURE(delta, TPE)
{
	MOSBlockHeaderTpe(delta, TPE)* parameters = (MOSBlockHeaderTpe(delta, TPE)*) (task)->blk;
	BUN cnt = MOSgetCnt(task->blk);

	assert(cnt > 0);
	assert(MOSgetTag(task->blk) == MOSAIC_DELTA);

	task->start += (oid) cnt;

	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(delta, TPE));
	blk += BitVectorSize(cnt, parameters->bits);
	blk += GET_PADDING(task->blk, delta, TPE);

	task->blk = (MosaicBlk) blk;

}

static inline void CONCAT2(determineDeltaParameters, TPE)
(MOSBlockHeaderTpe(delta, TPE)* parameters, TPE* src, BUN limit) {
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

MOSestimate_SIGNATURE(delta, TPE)
{
	(void) previous;
	current->is_applicable = true;
	current->compression_strategy.tag = MOSAIC_DELTA;
	TPE *src = getSrc(TPE, task);
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;
	MOSBlockHeaderTpe(delta, TPE) parameters;
	CONCAT2(determineDeltaParameters, TPE)(&parameters, src, limit);
	assert(parameters.rec.cnt > 0);/*Should always compress.*/
	current->uncompressed_size += (BUN) (parameters.rec.cnt * sizeof(TPE));
	current->compressed_size += 2 * sizeof(MOSBlockHeaderTpe(delta, TPE)) + wordaligned((parameters.rec.cnt * parameters.bits) / CHAR_BIT, lng);
	current->compression_strategy.cnt = (unsigned int) parameters.rec.cnt;

	if (parameters.rec.cnt > *current->max_compression_length ) {
		*current->max_compression_length = parameters.rec.cnt;
	}

	return MAL_SUCCEED;
}

MOSpostEstimate_SIGNATURE(delta, TPE)
{
	(void) task;
}

// rather expensive simple value non-compressed store
MOScompress_SIGNATURE(delta, TPE)
{
	ALIGN_BLOCK_HEADER(task,  delta, TPE);

	MosaicBlk blk = task->blk;
	MOSsetTag(blk,MOSAIC_DELTA);
	MOSsetCnt(blk, 0);
	TPE *src = getSrc(TPE, task);
	BUN i = 0;
	BUN limit = estimate->cnt;
	MOSBlockHeaderTpe(delta, TPE)* parameters = (MOSBlockHeaderTpe(delta, TPE)*) (task)->blk;
	CONCAT2(determineDeltaParameters, TPE)(parameters, src, limit);
	BitVector base = MOScodevectorDelta(task, TPE);
	task->dst = (char*) base;
	TPE pv = parameters->init; /*previous value*/
	/*Initial delta is zero.*/
	setBitVector(base, 0, parameters->bits, (BitVectorChunk) 0);
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (parameters->bits - 1);

	for(i = 1; i < MOSgetCnt(task->blk); i++) {
		/*TODO: assert that delta's actually does not cause an overflow. */
		TPE cv = *++src; /*current value*/
		DeltaTpe(TPE) delta = (DeltaTpe(TPE)) (cv > pv ? (IPTpe(TPE)) (cv - pv) : (IPTpe(TPE)) ((sign_mask) | (IPTpe(TPE)) (pv - cv)));
		setBitVector(base, i, parameters->bits, (BitVectorChunk) /*TODO: fix this once we have increased capacity of bitvector*/ delta);
		pv = cv;
	}
	task->dst += BitVectorSize(i, parameters->bits);
}

MOSdecompress_SIGNATURE(delta, TPE)
{
	MOSBlockHeaderTpe(delta, TPE)* parameters = (MOSBlockHeaderTpe(delta, TPE)*) (task)->blk;
	BUN lim = MOSgetCnt(task->blk);
	((TPE*)task->src)[0] = parameters->init; /*previous value*/
	BitVector base = (BitVector) MOScodevectorDelta(task, TPE);
	DeltaTpe(TPE) sign_mask = (DeltaTpe(TPE)) ((IPTpe(TPE)) 1) << (parameters->bits - 1);
	DeltaTpe(TPE) acc = (DeltaTpe(TPE)) parameters->init /*unsigned accumulating value*/;
	BUN i;
	for(i = 0; i < lim; i++) {
		DeltaTpe(TPE) delta = getBitVector(base, i, parameters->bits);
		((TPE*)task->src)[i] = ACCUMULATE(acc, delta, sign_mask, TPE);
	}
	task->src += i * sizeof(TPE);
}
