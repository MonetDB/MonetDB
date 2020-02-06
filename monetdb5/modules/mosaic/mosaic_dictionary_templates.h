#ifdef COMPRESSION_DEFINITION

#ifndef TPE
#define clean_up_info_ID(METHOD) CONCAT2(clean_up_info_, METHOD)

static inline void clean_up_info_ID(METHOD)(MOStask* task) {
	GlobalDictionaryInfo* info = task->CONCAT2(METHOD, _info);

	BBPreclaim(info->dict);
	BBPreclaim(info->admin);
	BBPreclaim(info->selection_vector);
	BBPreclaim(info->increments);

	GDKfree(info);

	task->CONCAT2(METHOD, _info) = NULL;
}

MOSprepareDictionaryContext_SIGNATURE(METHOD)
{
	const unsigned int zero = 0;
	str msg = MAL_SUCCEED;

	GlobalDictionaryInfo** info = &task->CONCAT2(METHOD, _info);
	BAT* source = task->bsrc;

	if ( (*info = GDKmalloc(sizeof(GlobalDictionaryInfo))) == NULL ) {
		throw(MAL,"mosaic." STRINGIFY(METHOD) ,MAL_MALLOC_FAIL);
	}
	(*info)->previous_start = task->start;

	BAT *ngid = NULL, *next = NULL, *freq = NULL;
	BAT* source_copy = NULL;
	BAT* unsorted_dict = NULL;
	BAT *cand_capped_dict = NULL;
	BAT* dict = NULL;
	BAT* admin = NULL;
	BAT* selection_vector = NULL;
	BAT* increments = NULL;

	BAT* projection_chain[4] = {NULL};
	(void) projection_chain;
	/* Work around to prevent re-entering batIdxLock in BATsort when it decides to create an ordered/hash indices.
	 * This is some what expensive since it requires a full copy of the original bat.
	 * TODO: build vmosaic heap in a occ way.
	 * TODO: there can be a single function that builds both the capped and the global dictionary
	 */
	if ( (source_copy = COLcopy(source, source->ttype, true /*writable = true*/, TRANSIENT)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".COLcopy", GDK_EXCEPTION);
		goto finalize;
	}

	if (BATgroup(&ngid, &next, &freq, source_copy, NULL, NULL, NULL, NULL) != GDK_SUCCEED) {
		msg = createException(MAL, "mosaic.createGlobalDictInfo.BATgroup", GDK_EXCEPTION);
		goto finalize;
	}

#ifdef MOS_CUT_OFF_SIZE
	if (BATfirstn(&cand_capped_dict, NULL, freq, NULL, NULL, MOS_CUT_OFF_SIZE, false, true, false) != GDK_SUCCEED) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".BATfirstn_unique", GDK_EXCEPTION);
		goto finalize;
	}

	projection_chain[0] = cand_capped_dict;
	projection_chain[1] = next;
	projection_chain[2] = source_copy;
	if ((unsorted_dict = BATprojectchain(projection_chain)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".BATprojectchain", GDK_EXCEPTION);
		goto finalize;
	}
#else
	if ((unsorted_dict = BATproject(next, source_copy)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".BATproject", GDK_EXCEPTION);
		goto finalize;
	}
#endif

	if (BATsort(&dict, NULL, NULL, unsorted_dict, NULL, NULL, false, false, false) != GDK_SUCCEED) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".BATfirstn_unique", GDK_EXCEPTION);
		goto finalize;
	}

	if (BAThash(dict) != GDK_SUCCEED) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".BAThash", GDK_EXCEPTION);
		goto finalize;
	}

	if ((admin = BATconstant(0, TYPE_int, &zero, BATcount(dict), TRANSIENT)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".COLnew", GDK_EXCEPTION);
		goto finalize;
	}

	if ((selection_vector = COLnew(0, TYPE_oid, MOSAICMAXCNT, TRANSIENT)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".COLnew", GDK_EXCEPTION);
		goto finalize;
	}

	if ((increments = COLnew(0, TYPE_bte, MOSAICMAXCNT, TRANSIENT)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(METHOD) ".COLnew", GDK_EXCEPTION);
		goto finalize;
	}

	(*info)->dict						= dict;
	(*info)->admin						= admin;
	(*info)->selection_vector			= selection_vector;
	(*info)->increments					= increments;

	(*info)->count						= 0;

finalize:
		BBPreclaim(ngid);
		BBPreclaim(next);
		BBPreclaim(freq);
		BBPreclaim(source_copy);
		BBPreclaim(unsorted_dict);
		BBPreclaim(cand_capped_dict);

		if (msg != MAL_SUCCEED) clean_up_info_ID(METHOD)(task);

		return msg;
}

MOSlayoutDictionary_SIGNATURE(METHOD)
{
	(void) task;
	(void) layout;
	(void) current_bsn;

	size_t buffer_size = LAYOUT_BUFFER_SIZE;

	char key_size[LAYOUT_BUFFER_SIZE] = {0};

	char* pkey_size = &key_size[0];

	bteToStr(&pkey_size, &buffer_size, &GET_FINAL_BITS(task, METHOD), true);

	char final_properties[1000] = {0};

	strcat(final_properties, "{");
		strcat(final_properties, "\"bits per key\" : ");
		strcat(final_properties, key_size);
	strcat(final_properties, "}");

	LAYOUT_INSERT(
		bsn = current_bsn;
		tech = STRINGIFY(METHOD);
		count = GET_FINAL_DICT_COUNT(task, METHOD);
		input = count * task->bsrc->twidth;
		properties = final_properties;
		);

	return MAL_SUCCEED;
}
#else /*TPE is defined*/

MOSestimate_SIGNATURE(METHOD, TPE) {
	str msg = MAL_SUCCEED;
	(void) previous;
	GlobalDictionaryInfo* info = task->CONCAT2(METHOD, _info);
	BUN limit = (BUN) (task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start);

	BUN nr_compressed;

	BUN old_keys_size		= (CONCAT3(current->nr_, METHOD, _encoded_elements) * GET_BITS(info)) / CHAR_BIT;
	BUN old_dict_size		= GET_COUNT(info) * sizeof(TPE);
	BUN old_headers_size	= CONCAT3(current->nr_, METHOD, _encoded_blocks) * 2 * sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	BUN old_bytes			= old_keys_size + old_dict_size + old_headers_size;

	bool full_build = true;

	BUN neg_start = 0;
	BUN neg_limit = 0;
	BUN pos_start = 0;
	BUN pos_limit = 0;
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
		/* TODO: I think there is a bug here when using METHOD. What if dictionary estimates exits prematurely*/
	}

	BAT* pos_slice;
	if ((pos_slice = BATslice(task->bsrc, pos_start, pos_start + pos_limit)) == NULL) {
		clean_up_info_ID(METHOD)(task);
		throw(MAL, "BATslice.pos_slice", MAL_MALLOC_FAIL);
	}

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
		BAT* neg_slice;
		if ((neg_slice = BATslice(task->bsrc, neg_start, neg_start + neg_limit)) == NULL){
			clean_up_info_ID(METHOD)(task);
			throw(MAL, "BATslice.neg_slice", MAL_MALLOC_FAIL);
		}

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
	CONCAT3(current->nr_, METHOD, _encoded_elements) += nr_compressed;
	CONCAT3(current->nr_, METHOD, _encoded_blocks)++;

	BUN new_count = delta_count + info->count;
	calculateBits(GET_BITS_EXTENDED(info), new_count);

	BUN new_keys_size		= (CONCAT3(current->nr_, METHOD, _encoded_elements) * GET_BITS_EXTENDED(info)) / CHAR_BIT;
	BUN new_dict_size		= new_count * sizeof(TPE);
	BUN new_headers_size	= CONCAT3(current->nr_, METHOD, _encoded_blocks) * 2 * sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	BUN new_bytes			= new_keys_size + new_dict_size + new_headers_size;

	current->compression_strategy.tag = METHOD;
	current->compression_strategy.cnt = (unsigned int) nr_compressed;

	current->uncompressed_size	+= (BUN) ( nr_compressed * sizeof(TPE));
	current->compressed_size 	+= (BUN) (wordaligned( MosaicBlkSize, BitVectorChunk) + new_bytes - old_bytes);

	return msg;
}

MOSpostEstimate_SIGNATURE(METHOD, TPE) {
	GlobalDictionaryInfo* info	= task->CONCAT2(METHOD, _info);
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

MOSfinalizeDictionary_SIGNATURE(METHOD, TPE) {
	BAT* b = task->bsrc;
	GlobalDictionaryInfo* info = task->CONCAT2(METHOD, _info);
	BUN* pos_dict = &task->hdr->CONCAT2(pos_, METHOD);
	BUN* length_dict = &task->hdr->CONCAT2(length_, METHOD);
	bte* bits_dict = &GET_FINAL_BITS(task, METHOD);

	Heap* vmh = b->tvmosaic;

	assert(vmh->free % GetTypeWidth(info) == 0);
	*pos_dict = vmh->free / GetTypeWidth(info);

	BUN size_in_bytes = vmh->free + GetSizeInBytes(info);

	if (HEAPextend(vmh, size_in_bytes, true) != GDK_SUCCEED) {
		clean_up_info_ID(METHOD)(task);
		throw(MAL, "HEAPextend", GDK_EXCEPTION);
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

	GET_FINAL_DICT_COUNT(task, METHOD) = *length_dict;

	clean_up_info_ID(METHOD)(task);

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

MOScompress_SIGNATURE(METHOD, TPE) {
	MOSsetTag(task->blk, METHOD);
	ALIGN_BLOCK_HEADER(task,  METHOD, TPE);

	TPE *val = getSrc(TPE, (task));
	BUN cnt = estimate->cnt;
	BitVector base = MOScodevectorDict(task, METHOD, TPE);
	BUN i;
	TPE* dict = GET_FINAL_DICT(task, METHOD, TPE);
	BUN dict_size = GET_FINAL_DICT_COUNT(task, METHOD);
	bte bits = GET_FINAL_BITS(task, METHOD);
	for(i = 0; i < cnt; i++, val++) {
		BUN key = CONCAT2(find_value_, TPE)(dict, dict_size, *val);
		assert(key < dict_size);
		setBitVector(base, i, bits, (BitVectorChunk) key);
	}
	MOSsetCnt(task->blk, i);
}

MOSadvance_SIGNATURE(METHOD, TPE) {
	BUN cnt = MOSgetCnt(task->blk);

	assert(cnt > 0);
	task->start += (oid) cnt;

	char* blk = (char*)task->blk;
	blk += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	blk += BitVectorSize(cnt, GET_FINAL_BITS(task, METHOD));
	blk += GET_PADDING(task->blk, METHOD, TPE);

	task->blk = (MosaicBlk) blk;
}

MOSdecompress_SIGNATURE(METHOD, TPE) {
	BUN cnt = MOSgetCnt((task)->blk);
	BitVector base = MOScodevectorDict(task, METHOD, TPE);
	bte bits = GET_FINAL_BITS(task, METHOD);
	TPE* dict = GET_FINAL_DICT(task, METHOD, TPE);
	TPE* dest = (TPE*) (task)->src;
	for(BUN i = 0; i < cnt; i++) {
		BUN key = getBitVector(base,i,(int) bits);
		(dest)[i] = dict[key];
	}
	dest += cnt;
	(task)->src = (char*) dest;
}

MOSlayout_SIGNATURE(METHOD, TPE)
{
	size_t compressed_size = 0;
	compressed_size += sizeof(MOSBlockHeaderTpe(METHOD, TPE));
	lng cnt = (lng) MOSgetCnt(task->blk);
	compressed_size += BitVectorSize(cnt, GET_FINAL_BITS(task, METHOD));	
	compressed_size += GET_PADDING(task->blk, METHOD, TPE);

	LAYOUT_INSERT(
		bsn = current_bsn;
		tech = STRINGIFY(METHOD);
		count = cnt;
		input = (lng) (cnt * sizeof(TPE));
		output = (lng) compressed_size;
		);

	return MAL_SUCCEED;
}

#endif /*ndef TPE*/


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
    TPE* dict = GET_FINAL_DICT(task, METHOD, TPE);
	BitVector base = MOScodevectorDict(task, METHOD, TPE);
    bte bits = GET_FINAL_BITS(task, METHOD);
    for (oid c = canditer_peekprev(task->ci); !is_oid_nil(c) && c < last; c = CAND_ITER(task->ci)) {
        BUN i = (BUN) (c - first);
        BitVectorChunk j = getBitVector(base,i,bits); 
        TPE v = dict[j];
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

	TPE* dict = GET_FINAL_DICT(task, METHOD, TPE);
	BitVector base = MOScodevectorDict(task, METHOD, TPE);
    bte bits = GET_FINAL_BITS(task, METHOD);
	for (oid o = canditer_peekprev(task->ci); !is_oid_nil(o) && o < last; o = CAND_ITER(task->ci)) {
        BUN i = (BUN) (o - first);
        BitVectorChunk j = getBitVector(base,i,bits); 
		*bt++ = dict[j];
		task->cnt++;
	}

	task->src = (char*) bt;
}
#endif
