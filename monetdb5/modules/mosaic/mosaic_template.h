
// TODO: NAME and NAME_TAG can be combined.
#define estimate(NAME, TPE, NAME_TAG)\
{\
	str msg = MOSestimate_##NAME##_##TPE(task, &estimations[NAME_TAG], previous);\
	if (msg != MAL_SUCCEED) return msg;\
}

#define postEstimate(NAME, TPE, DUMMY_ARGUMENT) if (current->is_applicable) { MOSpostEstimate_##NAME##_##TPE(task); }

static str CONCAT2(MOSestimate_inner_, TPE)(MOStask* task, MosaicEstimation* current, const MosaicEstimation* previous) {

	BUN max_compression_length = 0;
	MosaicEstimation estimations[MOSAIC_METHODS];
	const int size = sizeof(estimations) / sizeof(MosaicEstimation);
	for (int i = 0; i < size; i++) {
		estimations[i].previous_compressed_size = previous->compressed_size;
		estimations[i].uncompressed_size = previous->uncompressed_size;
		estimations[i].compressed_size = previous->compressed_size;
		estimations[i].compression_strategy = previous->compression_strategy;
		estimations[i].nr_dict_encoded_blocks = previous->nr_dict_encoded_blocks;
		estimations[i].nr_dict_encoded_elements = previous->nr_dict_encoded_elements;
		estimations[i].nr_dict256_encoded_elements = previous->nr_dict256_encoded_elements;
		estimations[i].nr_dict256_encoded_blocks = previous->nr_dict256_encoded_blocks;
		estimations[i].dict_limit = previous->dict_limit;
		estimations[i].dict256_limit = previous->dict256_limit;
		estimations[i].must_be_merged_with_previous = false;
		estimations[i].is_applicable = false;
		estimations[i].max_compression_length = &max_compression_length;
	}

	/* select candidate amongst those*/
	if (METHOD_IS_SET(task->mask, MOSAIC_RLE))		{
		DO_OPERATION_IF_ALLOWED_VARIADIC(estimate, runlength, TPE, MOSAIC_RLE);
	}
	if (METHOD_IS_SET(task->mask, MOSAIC_DELTA))	{
		DO_OPERATION_IF_ALLOWED_VARIADIC(estimate, delta, TPE, MOSAIC_DELTA);
	}
	if (METHOD_IS_SET(task->mask, MOSAIC_LINEAR))	{
		DO_OPERATION_IF_ALLOWED_VARIADIC(estimate, linear, TPE, MOSAIC_LINEAR);
	}
	if (METHOD_IS_SET(task->mask, MOSAIC_FRAME))	{
		DO_OPERATION_IF_ALLOWED_VARIADIC(estimate, frame, TPE, MOSAIC_FRAME);
	}
	if (METHOD_IS_SET(task->mask, MOSAIC_PREFIX))	{
		DO_OPERATION_IF_ALLOWED_VARIADIC(estimate, prefix, TPE, MOSAIC_PREFIX);
	}
	if (METHOD_IS_SET(task->mask, MOSAIC_DICT256))	{
		DO_OPERATION_IF_ALLOWED_VARIADIC(estimate, dict256, TPE, MOSAIC_DICT256);
	}
	if (METHOD_IS_SET(task->mask, MOSAIC_DICT))		{
		DO_OPERATION_IF_ALLOWED_VARIADIC(estimate, dict,	TPE, MOSAIC_DICT);
	}
	if (METHOD_IS_SET(task->mask, MOSAIC_RAW))		{
		DO_OPERATION_IF_ALLOWED_VARIADIC(estimate, raw,	TPE, MOSAIC_RAW);
	}

	BUN best_normalized_compression = (BUN) (-1);
	current->is_applicable = false;

	for (int i = 0; i < size; i++) {
		if (estimations[i].is_applicable) {
			BUN normalized_compression = get_normalized_compression(&(estimations[i]), previous);

			if (estimations[i].is_applicable && normalized_compression < best_normalized_compression ) {
				*current = estimations[i];
				best_normalized_compression = normalized_compression;
			}
		}
	}

	if (current->compression_strategy.tag == MOSAIC_RAW)		DO_OPERATION_IF_ALLOWED(postEstimate, raw, TPE);
	if (current->compression_strategy.tag == MOSAIC_RLE)		DO_OPERATION_IF_ALLOWED(postEstimate, runlength, TPE);
	if (current->compression_strategy.tag == MOSAIC_DICT256)	DO_OPERATION_IF_ALLOWED(postEstimate, dict256, TPE);
	if (current->compression_strategy.tag == MOSAIC_DICT)		DO_OPERATION_IF_ALLOWED(postEstimate, dict, TPE);
	if (current->compression_strategy.tag == MOSAIC_DELTA)		DO_OPERATION_IF_ALLOWED(postEstimate, delta, TPE);
	if (current->compression_strategy.tag == MOSAIC_LINEAR)		DO_OPERATION_IF_ALLOWED(postEstimate, linear, TPE);
	if (current->compression_strategy.tag == MOSAIC_FRAME)		DO_OPERATION_IF_ALLOWED(postEstimate, frame, TPE);
	if (current->compression_strategy.tag == MOSAIC_PREFIX)		DO_OPERATION_IF_ALLOWED(postEstimate, prefix, TPE);

	return MAL_SUCCEED;
}\
static str CONCAT2(MOSestimate_, TPE)(MOStask* task, BAT* estimates, size_t* compressed_size) {
	str result = MAL_SUCCEED;

	*compressed_size = 0;


	BUN dict_limit = 0;
	BUN dict256_limit = 0;
	MosaicEstimation previous = {
		.is_applicable = false,
		.uncompressed_size = 0,
		.previous_compressed_size = 0,
		.compressed_size = 0,
		.nr_dict_encoded_elements = 0,
		.nr_dict_encoded_blocks = 0,
		.dict256_limit = &dict256_limit,
		.dict_limit = &dict_limit,
		.nr_dict256_encoded_elements = 0,
		.nr_dict256_encoded_blocks = 0,
		.compression_strategy = {.cnt = 0},
		.must_be_merged_with_previous = false
	};

	MosaicEstimation current;
	MosaicBlkRec* cursor = Tloc(estimates,0);

	while(task->start < task->stop ){

		/* default is to extend the non-compressed block with a single element*/
		if ( (result = CONCAT2(MOSestimate_inner_, TPE)(task, &current, &previous)) ) {
			return result;
		}

		if (!current.is_applicable) {
			throw(MAL,"mosaic.compress", "Cannot compress BAT with given compression MOSmethods.");
		}

		if (current.must_be_merged_with_previous) {
			--cursor;
			assert(cursor->tag == previous.compression_strategy.tag && cursor->cnt == previous.compression_strategy.cnt);
			task->start -= previous.compression_strategy.cnt;
		}
		else BATcount(estimates)++;

		*cursor = current.compression_strategy;
		++cursor;
		previous = current;
		task->start += current.compression_strategy.cnt;
	}

	(*compressed_size) = current.compressed_size;

	return MAL_SUCCEED;
}

#undef estimate
#undef postEstimate


#define compress(NAME, TPE, DUMMY_ARGUMENT)\
{\
	ALGODEBUG mnstr_printf(GDKstdout, "#MOScompress_" #NAME "\n");\
	MOScompress_##NAME##_##TPE(task, estimate);\
	MOSupdateHeader(task);\
	MOSadvance_##NAME##_##TPE(task);\
	MOSnewBlk(task);\
}

static void
CONCAT2(MOScompressInternal_, TPE)(MOStask* task, BAT* estimates) {
	/* second pass: compression phase*/
	for(BUN i = 0; i < BATcount(estimates); i++) {
		assert (task->dst < task->bsrc->tmosaic->base + task->bsrc->tmosaic->size );

		MosaicBlkRec* estimate = Tloc(estimates, i);

		switch(estimate->tag) {
		case MOSAIC_RLE:
			DO_OPERATION_IF_ALLOWED(compress, runlength, TPE);
			break;
		case MOSAIC_DICT256:
			DO_OPERATION_IF_ALLOWED(compress, dict256, TPE);
			break;
		case MOSAIC_DICT:
			DO_OPERATION_IF_ALLOWED(compress, dict, TPE);
			break;
		case MOSAIC_DELTA:
			DO_OPERATION_IF_ALLOWED(compress, delta, TPE);
			break;
		case MOSAIC_LINEAR:
			DO_OPERATION_IF_ALLOWED(compress, linear, TPE);
			break;
		case MOSAIC_FRAME:
			DO_OPERATION_IF_ALLOWED(compress, frame, TPE);
			break;
		case MOSAIC_PREFIX:
			DO_OPERATION_IF_ALLOWED(compress, prefix, TPE);
			break;
		case MOSAIC_RAW:
			DO_OPERATION_IF_ALLOWED(compress, raw, TPE);
			break;
		default : /* Unknown block type. Should not happen.*/
			assert(0);
		}
	}
}

#undef compress

#define decompress(NAME, TPE, DUMMY_ARGUMENT)\
{\
	ALGODEBUG mnstr_printf(GDKstdout, "#MOSdecompress_" #NAME "\n");\
	MOSdecompress_##NAME##_##TPE(task);\
	MOSadvance_##NAME##_##TPE(task);\
}

static void CONCAT2(MOSdecompressInternal_, TPE)(MOStask* task)
{
	while(task->start != task->stop){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RAW:
			DO_OPERATION_IF_ALLOWED(decompress, raw, TPE);
			break;
		case MOSAIC_RLE:
			DO_OPERATION_IF_ALLOWED(decompress, runlength, TPE);
			break;
		case MOSAIC_DICT256:
			DO_OPERATION_IF_ALLOWED(decompress, dict256, TPE);
			break;
		case MOSAIC_DICT:
			DO_OPERATION_IF_ALLOWED(decompress, dict, TPE);
			break;
		case MOSAIC_DELTA:
			DO_OPERATION_IF_ALLOWED(decompress, delta, TPE);
			break;
		case MOSAIC_LINEAR:
			DO_OPERATION_IF_ALLOWED(decompress, linear, TPE);
			break;
		case MOSAIC_FRAME:
			DO_OPERATION_IF_ALLOWED(decompress, frame, TPE);
			break;
		case MOSAIC_PREFIX:
			DO_OPERATION_IF_ALLOWED(decompress, prefix, TPE);
			break;
		default: assert(0);
		}
	}
}

#undef decompress

#define select(NAME, TPE, DUMMY_ARGUMENT)\
{\
    MOSselect_##NAME##_##TPE(\
        task,\
        *(TPE*) low,\
        *(TPE*) hgh,\
        *li,\
        *hi,\
        *anti);\
	MOSadvance_##NAME##_##TPE(task);\
}

static str CONCAT2(MOSselect_, TPE) (MOStask* task, const void* low, const void* hgh, const bit* li, const bit* hi, const bit* anti)
{
	while(task->start < task->stop ){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSselect_runlength\n");
			DO_OPERATION_IF_ALLOWED(select, runlength, TPE);
			break;
		case MOSAIC_DICT256:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSselect_dict256\n");
			DO_OPERATION_IF_ALLOWED(select, dict256, TPE);
			break;
		case MOSAIC_DICT:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSselect_var\n");
			DO_OPERATION_IF_ALLOWED(select, dict, TPE);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSselect_frame\n");
			DO_OPERATION_IF_ALLOWED(select, frame, TPE);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSselect_delta\n");
			DO_OPERATION_IF_ALLOWED(select, delta, TPE);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSselect_prefix\n");
			DO_OPERATION_IF_ALLOWED(select, prefix, TPE);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSselect_linear\n");
			DO_OPERATION_IF_ALLOWED(select, linear, TPE);
			break;
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSselect_raw\n");
			DO_OPERATION_IF_ALLOWED(select, raw, TPE);
			break;
		}

		if (task->ci->next == task->ci->ncand) {
			/* We are at the end of the candidate list.
			 * So we can stop now.
			 */
			return MAL_SUCCEED;
		}
	}

	return MAL_SUCCEED;
}

#undef select

#define projection(NAME, TPE, DUMMY_ARGUMENT) \
{\
	MOSprojection_##NAME##_##TPE(task);\
	MOSadvance_##NAME##_##TPE(task);\
}

static str CONCAT2(MOSprojection_, TPE)(MOStask* task)
{
	while(task->start < task->stop ){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_runlength\n");
			DO_OPERATION_IF_ALLOWED(projection, runlength, TPE);
			break;
		case MOSAIC_DICT256:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_dict256\n");
			DO_OPERATION_IF_ALLOWED(projection, dict256, TPE);
			break;
		case MOSAIC_DICT:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_var\n");
			DO_OPERATION_IF_ALLOWED(projection, dict, TPE);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_frame\n");
			DO_OPERATION_IF_ALLOWED(projection, frame, TPE);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_delta\n");
			DO_OPERATION_IF_ALLOWED(projection, delta, TPE);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_prefix\n");
			DO_OPERATION_IF_ALLOWED(projection, prefix, TPE);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_linear\n");
			DO_OPERATION_IF_ALLOWED(projection, linear, TPE);
			break;
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSprojection_raw\n");
			DO_OPERATION_IF_ALLOWED(projection, raw, TPE);
			break;
		}

		if (task->ci->next == task->ci->ncand) {
			/* We are at the end of the candidate list.
			 * So we can stop now.
			 */
			return MAL_SUCCEED;
		}
	}

	return MAL_SUCCEED;
}

#undef projection
