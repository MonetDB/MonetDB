MOSestimate_SIGNATURE(NAME, TPE) {
	(void) previous;
	GlobalDictionaryInfo* info = task->CONCAT2(NAME, _info);
	BUN limit = (BUN) (task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start);

	BUN nr_compressed;

	BUN old_keys_size		= (CONCAT3(current->nr_, NAME, _encoded_elements) * GET_BITS(info)) / CHAR_BIT;
	BUN old_dict_size		= GET_COUNT(info) * sizeof(TPE);
	BUN old_headers_size	= CONCAT3(current->nr_, NAME, _encoded_blocks) * 2 * sizeof(MOSBlockHeaderTpe(NAME, TPE));
	BUN old_bytes			= old_keys_size + old_dict_size + old_headers_size;

	bool full_build = true;

	BUN neg_start;
	BUN neg_limit;
	BUN pos_start;
	BUN pos_limit;
	if (task->start < info->previous_start + info->previous_limit) {
		neg_start = info->previous_start;
		neg_limit = task->start - info->previous_start;
		pos_limit = (task->start + limit) - (info->previous_start + info->previous_limit);
		if (neg_limit + pos_limit < limit) {
			pos_start = info->previous_start + info->previous_limit;
			full_build = false;
		}
	}

	if (full_build) {
		/*FULL DICTIONARY BUILD*/
		nr_compressed = 0;

		neg_start = task->start;
		neg_limit = 0;
		pos_start = task->start;
		pos_limit = limit;
		BATcount(info->selection_vector)	= 0;
	}
	else {
		nr_compressed = info->previous_limit;
		/* TODO: I think there is a bug here when using dict256. What if dictionary estimates exits prematurely*/
	}

	BAT* pos_slice = BATslice(task->bsrc, pos_start, pos_start + pos_limit); /*TODO CHECK ERROR*/

	BAT* selection_vector = info->selection_vector;
	BATcount(selection_vector) = 0;
	selection_vector->tsorted = false;
	selection_vector->trevsorted = false;

	oid* selection_vector_val = Tloc(selection_vector, 0);
	TPE* slice_val = Tloc(pos_slice, 0);
	BUN nr_pos_candidates = 0;
	for (BUN o = 0; o < pos_limit; o++) /*TODO: just use a proper cl iterator*/{
		TPE val = slice_val[o];
		bte matches = 0;
		for (
			BUN hb = HASHget(info->dict->thash, HASHprobe((info->dict->thash), &val));
			hb != HASHnil(info->dict->thash);
			hb = HASHgetlink(info->dict->thash,hb)) {
			TPE hashed_val = *(TPE*) Tloc(info->dict, hb);
			if ( ARE_EQUAL(val, hashed_val, !info->dict->tnonil, TPE) ) {
				*(selection_vector_val++) = hb;
				BATcount(selection_vector)++;
				nr_compressed++;
				matches++;
				/*TODO: break after first match.*/
			}
		}
		if (matches == 0) {
			break;
		}
		nr_pos_candidates++;
	}
	BBPreclaim(pos_slice);

	memset(Tloc(info->increments, 0), 1, BATcount(selection_vector));

	if (full_build) {
		/* sort positive selection vector on oid and keep order*/

		GDKqsort(
			Tloc(selection_vector, 0),
			NULL,
			NULL,
			BATcount(selection_vector),
			Tsize(selection_vector), 0, selection_vector->ttype, false, false);
	}
	else /*incremental build*/ {
		BAT* neg_slice = BATslice(task->bsrc, neg_start, neg_start + neg_limit); /*TODO CHECK ERROR*/
		TPE* slice_val = Tloc(neg_slice, 0);

		BUN nr_neg_candidates = 0;

		oid* selection_vector_val = Tloc(selection_vector, nr_pos_candidates);
		for (BUN o = 0; o < neg_limit; o++) /*TODO: just use a proper cl iterator*/{
			TPE val = slice_val[o];
			bte matches = 0;
			for (
				BUN hb = HASHget(info->dict->thash, HASHprobe((info->dict->thash), &val));
				hb != HASHnil(info->dict->thash);
				hb = HASHgetlink(info->dict->thash,hb)) {
				TPE hashed_val = *(TPE*) Tloc(info->dict, hb);
				if ( ARE_EQUAL(val, hashed_val, !info->dict->tnonil, TPE) ) {
					*(selection_vector_val++) = hb;
					BATcount(selection_vector)++;
					nr_compressed--;
					matches++;
					/*TODO: break after first match.*/
				}
			}
			assert (matches == 1);
			nr_neg_candidates++;
		}
		BBPreclaim(neg_slice);

		memset(Tloc(info->increments, nr_pos_candidates), -1, nr_neg_candidates);

		GDKqsort(
			Tloc(selection_vector, 0),
			Tloc(info->increments, 0),
			NULL,
			BATcount(selection_vector),
			Tsize(selection_vector), sizeof(bte), selection_vector->ttype, false, false);
	}
	
	BATcount(info->increments) = BATcount(selection_vector);

	bte* increment = Tloc(info->increments, 0);
	MosaicBlkRec* bv = Tloc(info->admin, 0);
	oid* ssv_val = Tloc(selection_vector, 0);
	BUN	 delta_count = 0;
	for (BUN o = 0; o < BATcount(selection_vector); o++ ) {
		MosaicBlkRec* val = &bv[ssv_val[o]];
		if (!val->tag) {
			/* A frequency value of -1 indicates that this value is already in the dictionary */
			int freq = increment[o];
			while (o + 1 < BATcount(selection_vector) && ssv_val[o] == ssv_val[o+1]) {
				freq += increment[++o];
			}
			assert(val->cnt + freq < MOSAICMAXCNT);
			val->cnt += freq;
			delta_count += val->cnt ? 1 :0;
		}
		else {
			while (o + 1 < BATcount(selection_vector) && ssv_val[o] == ssv_val[o+1]) {
				o++;
			}
		}
	}

	info->previous_start = neg_start + neg_limit;
	info->previous_limit = nr_compressed;

	current->is_applicable = nr_compressed > 0;
	CONCAT3(current->nr_, NAME, _encoded_elements) += nr_compressed;
	CONCAT3(current->nr_, NAME, _encoded_blocks)++;

	BUN new_count = delta_count + info->count;
	calculateBits(GET_BITS_EXTENDED(info), new_count);

	BUN new_keys_size		= (CONCAT3(current->nr_, NAME, _encoded_elements) * GET_BITS_EXTENDED(info)) / CHAR_BIT;
	BUN new_dict_size		= new_count * sizeof(TPE);
	BUN new_headers_size	= CONCAT3(current->nr_, NAME, _encoded_blocks) * 2 * sizeof(MOSBlockHeaderTpe(NAME, TPE));
	BUN new_bytes			= new_keys_size + new_dict_size + new_headers_size;

	current->compression_strategy.tag = NAME;
	current->compression_strategy.cnt = (unsigned int) nr_compressed;

	current->uncompressed_size	+= (BUN) ( nr_compressed * sizeof(TPE));
	current->compressed_size 	+= (BUN) (wordaligned( MosaicBlkSize, BitVectorChunk) + new_bytes - old_bytes);

	return MAL_SUCCEED;
}

MOSpostEstimate_SIGNATURE(NAME, TPE) {
	GlobalDictionaryInfo* info	= task->CONCAT2(NAME, _info);
	MosaicBlkRec* bytevector	= Tloc(info->admin, 0);

	BUN delta_count = 0;

	BUN i;
	for (i = 0; i < BATcount(info->admin); i++) {
		if (!bytevector[i].tag && bytevector[i].cnt) {
			 bytevector[i].tag = 1;
			 delta_count++;
			 bytevector[i].cnt = 0;
		}
	}
	info->count += delta_count;
	GET_BITS(info) = GET_BITS_EXTENDED(info);
}

MOSfinalizeDictionary_SIGNATURE(NAME, TPE) {
	BAT* b = task->bsrc;
	GlobalDictionaryInfo* info = task->CONCAT2(NAME, _info);
	BUN* pos_dict = &task->hdr->CONCAT2(pos_, NAME);
	BUN* length_dict = &task->hdr->CONCAT2(length_, NAME);
	bte* bits_dict = &GET_FINAL_BITS(task, NAME);

	Heap* vmh = b->tvmosaic;

	assert(vmh->free % GetTypeWidth(info) == 0);
	*pos_dict = vmh->free / GetTypeWidth(info);

	BUN size_in_bytes = vmh->free + GetSizeInBytes(info);

	if (HEAPextend(vmh, size_in_bytes, true) != GDK_SUCCEED) {
		BBPreclaim(info->dict);
		BBPreclaim(info->admin);
		throw(MAL, "mosaic.mergeDictionary_" STRINGIFY(NAME) ".HEAPextend", GDK_EXCEPTION);
	}

	TPE* dst = (TPE*) (vmh->base + vmh->free);
	MosaicBlkRec* admin_val = Tloc(info->admin, 0);
	TPE* dict_val = Tloc(info->dict, 0);

	for (BUN i = 0; i < BATcount(info->admin); i++) {
		if (admin_val[i].tag) {
			*(dst++) = dict_val[i];
		}
	}

	vmh->free += (size_t) GetSizeInBytes(info);
	vmh->dirty = true;

	*length_dict = GET_COUNT(info);
	calculateBits(*bits_dict, *length_dict);

	GET_FINAL_DICT_COUNT(task, NAME) = *length_dict;

	BBPreclaim(info->dict);
	BBPreclaim(info->admin);
	BBPreclaim(info->selection_vector);
	BBPreclaim(info->increments);

	GDKfree(info);

	return MAL_SUCCEED;
}

static inline 
BUN CONCAT2(find_value_, TPE)(TPE* dict, BUN dict_count, TPE val) {
	BUN m, f= 0, l = dict_count, offset = 0;
	/* This function assumes that the implementation of a dictionary*/
	/* is that of a sorted array with nils starting first.*/
	if (IS_NIL(TPE, val)) return 0;
	if (dict_count > 0 && IS_NIL(TPE, dict[0])) {
		/*If the dictionary starts with a nil,*/
		/*the actual sorted dictionary starts an array entry later.*/
		dict++;
		offset++;
		l--;
	}
	while( l-f > 0 ) {
		m = f + (l-f)/2;
		if ( val < dict[m]) l=m-1; else f= m;
		if ( val > dict[m]) f=m+1; else l= m;
	}
	return f + offset;
}

MOScompress_SIGNATURE(NAME, TPE) {
	MOSsetTag(task->blk, NAME);
	ALIGN_BLOCK_HEADER(task,  NAME, TPE);

	TPE *val = getSrc(TPE, (task));
	BUN cnt = estimate->cnt;
	BitVector base = MOScodevectorDict(task, NAME, TPE);
	BUN i;
	TPE* dict = GET_FINAL_DICT(task, NAME, TPE);
	BUN dict_size = GET_FINAL_DICT_COUNT(task, NAME);
	bte bits = GET_FINAL_BITS(task, NAME);
	for(i = 0; i < cnt; i++, val++) {
		BUN key = CONCAT2(find_value_, TPE)(dict, dict_size, *val);
		assert(key < dict_size);
		setBitVector(base, i, bits, (BitVectorChunk) key);
	}
	MOSsetCnt(task->blk, i);
}

MOSadvance_SIGNATURE(NAME, TPE) {
	BUN cnt = MOSgetCnt(task->blk);

	assert(cnt > 0);
	task->start += (oid) cnt;

	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(NAME, TPE));
	blk += BitVectorSize(cnt, GET_FINAL_BITS(task, NAME));
	blk += GET_PADDING(task->blk, NAME, TPE);

	task->blk = (MosaicBlk) blk;
}

MOSdecompress_SIGNATURE(NAME, TPE) {
	BUN cnt = MOSgetCnt((task)->blk);
	BitVector base = MOScodevectorDict(task, NAME, TPE);
	bte bits = GET_FINAL_BITS(task, NAME);
	TPE* dict = GET_FINAL_DICT(task, NAME, TPE);
	TPE* dest = (TPE*) (task)->src;
	for(BUN i = 0; i < cnt; i++) {
		BUN key = getBitVector(base,i,(int) bits);
		(dest)[i] = dict[key];
	}
	dest += cnt;
	(task)->src = (char*) dest;
}
