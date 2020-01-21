MOSprepareDictionaryContext_SIGNATURE(NAME)
{
	const unsigned int zero = 0;
	str msg = MAL_SUCCEED;

	GlobalDictionaryInfo** info = &task->CONCAT2(NAME, _info);
	BAT* source = task->bsrc;

	if ( (*info = GDKmalloc(sizeof(GlobalDictionaryInfo))) == NULL ) {
		throw(MAL,"mosaic." STRINGIFY(NAME) ,MAL_MALLOC_FAIL);	
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
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".COLcopy", GDK_EXCEPTION);
		goto finalize;
	}

	if (BATgroup(&ngid, &next, &freq, source_copy, NULL, NULL, NULL, NULL) != GDK_SUCCEED) {
		msg = createException(MAL, "mosaic.createGlobalDictInfo.BATgroup", GDK_EXCEPTION);
		goto finalize;
	}

#ifdef MOS_CUT_OFF_SIZE
	if (BATfirstn(&cand_capped_dict, NULL, freq, NULL, NULL, MOS_CUT_OFF_SIZE, false, true, false) != GDK_SUCCEED) {
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".BATfirstn_unique", GDK_EXCEPTION);
		goto finalize;
	}

	projection_chain[0] = cand_capped_dict;
	projection_chain[1] = next;
	projection_chain[2] = source_copy;
	if ((unsorted_dict = BATprojectchain(projection_chain)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".BATprojectchain", GDK_EXCEPTION);
		goto finalize;
	}
#else
	if ((unsorted_dict = BATproject(next, source_copy)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".BATproject", GDK_EXCEPTION);
		goto finalize;
	}
#endif

	if (BATsort(&dict, NULL, NULL, unsorted_dict, NULL, NULL, false, false, false) != GDK_SUCCEED) {
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".BATfirstn_unique", GDK_EXCEPTION);
		goto finalize;
	}

	if (BAThash(dict) != GDK_SUCCEED) {
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".BAThash", GDK_EXCEPTION);
		goto finalize;
	}

	if ((admin = BATconstant(0, TYPE_int, &zero, BATcount(dict), TRANSIENT)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".COLnew", GDK_EXCEPTION);
		goto finalize;
	}

	if ((selection_vector = COLnew(0, TYPE_oid, MOSAICMAXCNT, TRANSIENT)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".COLnew", GDK_EXCEPTION);
		goto finalize;
	}

	if ((increments = COLnew(0, TYPE_bte, MOSAICMAXCNT, TRANSIENT)) == NULL) {
		msg = createException(MAL, "mosaic." STRINGIFY(NAME) ".COLnew", GDK_EXCEPTION);
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

		if (msg != MAL_SUCCEED) {
			BBPreclaim(dict);
			BBPreclaim(admin);
			BBPreclaim(selection_vector);
			BBPreclaim(increments);
		}

		return msg;
}
