/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (c) 2014 author Martin Kersten
 * Adaptive compression scheme to reduce the storage footprint for stable persistent data.
 * The permissible compression techniques can be controlled thru an argument list
*/

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_hdr.h"
#include "mosaic_raw.h"
#include "mosaic_runlength.h"
#include "mosaic_capped.h"
#include "mosaic_var.h"
#include "mosaic_delta.h"
#include "mosaic_linear.h"
#include "mosaic_frame.h"
#include "mosaic_prefix.h"

char *MOSfiltername[]={"raw","runlength","capped","var","delta","linear","frame","prefix","EOL"};

bool MOSisTypeAllowed(int compression, BAT* b) {
	switch (compression) {
	case MOSAIC_RAW:		return MOStypes_raw(b);
	case MOSAIC_RLE:		return MOStypes_runlength(b);
	case MOSAIC_CAPPED:		return MOStypes_capped(b);
	case MOSAIC_VAR:		return MOStypes_var(b);
	case MOSAIC_DELTA:		return MOStypes_delta(b);
	case MOSAIC_LINEAR:		return MOStypes_linear(b);
	case MOSAIC_FRAME:		return MOStypes_frame(b);
	case MOSAIC_PREFIX:		return MOStypes_prefix(b);
	default: /* should not happen*/ assert(0);
	}

	return false;
}

static bool
MOSinitializeFilter(MOStask task, const char* compressions) {

	bool is_not_compressible = true;

	if (!GDK_STRNIL(compressions)) {
		for(int i = 0; i< MOSAIC_METHODS-1; i++) {
				if ( (task->filter[i] = strstr(compressions, MOSfiltername[i]) != 0 && MOSisTypeAllowed(i, task->bsrc)) ) {
					task->hdr->elms[i] = task->hdr->blks[i] = 0;
					is_not_compressible = false;
				}
		}
	}
	else {
		for(int i = 0; i< MOSAIC_METHODS-1; i++) {
				if ( (task->filter[i] = MOSisTypeAllowed(i, task->bsrc)) ) {
					task->hdr->elms[i] = task->hdr->blks[i] = 0;
					is_not_compressible = false;
				}
		}
	}

	return is_not_compressible;
}

static void
MOSinit(MOStask task, BAT *b) {
	char *base;
	if( VIEWmosaictparent(b) != 0)
		b= BATdescriptor(VIEWmosaictparent(b));
	assert(b);
	assert( b->tmosaic);
	base = b->tmosaic->base;
	assert(base);
	task->type = b->ttype;
	task->bsrc = b;
	task->hdr = (MosaicHdr) base;
	base += MosaicHdrSize;
	task->blk = (MosaicBlk)  base;
	task->dst = MOScodevector(task);
	task->capped_info = NULL;
	task->var_info = NULL;
}

void MOSblk(MosaicBlk blk)
{
	printf("Block tag %d cnt "BUNFMT"\n", MOSgetTag(blk),MOSgetCnt(blk));
}

str
MOSlayout(BAT *b, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MOStask task=0;
	int i;
	char buf[BUFSIZ];
	lng zero=0;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		throw(SQL,"mosaic",MAL_MALLOC_FAIL);

	if( b->tmosaic == NULL) {
			GDKfree(task);
			throw(MAL,"mosaic.layout","Compression heap missing");
	}

	MOSinit(task,b);
	MOSinitializeScan(task, b);
	// safe the general properties

		snprintf(buf,BUFSIZ,"%g", task->hdr->ratio);
		if( BUNappend(btech, "ratio", false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			BUNappend(bproperties, buf, false) != GDK_SUCCEED ||
			BUNappend(boutput, &zero , false) != GDK_SUCCEED)
				throw(MAL,"mosaic.layout", MAL_MALLOC_FAIL);
	for(i=0; i < MOSAIC_METHODS-1; i++){
		lng zero = 0;
		snprintf(buf,BUFSIZ,"%s blocks", MOSfiltername[i]);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &task->hdr->blks[i], false) != GDK_SUCCEED ||
			BUNappend(binput, &task->hdr->elms[i], false) != GDK_SUCCEED ||
			BUNappend(boutput, &zero , false) != GDK_SUCCEED ||
			BUNappend(bproperties, "", false) != GDK_SUCCEED)
				throw(MAL,"mosaic.layout", MAL_MALLOC_FAIL);
	}
	if( task->hdr->blks[MOSAIC_CAPPED])
		MOSlayout_capped_hdr(task,btech,bcount,binput,boutput,bproperties);
	if( task->hdr->blks[MOSAIC_VAR])
		MOSlayout_var_hdr(task,btech,bcount,binput,boutput,bproperties);

	if( BUNappend(btech, "========", false) != GDK_SUCCEED ||
		BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
		BUNappend(binput, &zero, false) != GDK_SUCCEED ||
		BUNappend(boutput, &zero , false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
			throw(MAL,"mosaic.layout", MAL_MALLOC_FAIL);

/*
	while(task->start< task->stop){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSlayout_raw\n");
			MOSlayout_raw(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_raw(task);
			break;
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSlayout_runlength\n");
			MOSlayout_runlength(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_runlength(task);
			break;
		case MOSAIC_CAPPED:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSlayout_capped\n");
			MOSlayout_capped(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_capped(task);
			break;
		case MOSAIC_VAR:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSlayout_var\n");
			MOSlayout_var(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_var(task);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSlayout_delta\n");
			MOSlayout_delta(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_delta(task);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSlayout_linear\n");
			MOSlayout_linear(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_linear(task);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSlayout_frame\n");
			MOSlayout_frame(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_frame(task);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKstdout, "#MOSlayout_prefix\n");
			MOSlayout_prefix(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_prefix(task);
			break;
		default:
			assert(0);
		}
	}
*/
	return MAL_SUCCEED;
}

/*
 * Compression is focussed on a single column.
 * Multiple compression techniques are applied at the same time.
 */

#define MOSnewBlk(TASK)\
			MOSsetTag(TASK->blk,MOSAIC_EOL);\
			MOSsetCnt(TASK->blk,0);\
			TASK->dst = MOScodevector(TASK);

#define getFactor(ESTIMATION) ((flt) (ESTIMATION).uncompressed_size / (ESTIMATION).compressed_size)


static str
MOSprepareEstimate(MOStask task) {

	str error;
	if (task->filter[MOSAIC_CAPPED]){
		if ( (error = MOSprepareEstimate_capped(task))) {
			return error;
		}
	}

	if (task->filter[MOSAIC_VAR]){
		if ( (error = MOSprepareEstimate_var(task))) {
			return error;
		}
	}

	return MAL_SUCCEED;
}


#define do_estimate(NAME, TPE, DUMMY_ARGUMENT)\
{\
	str msg = MOSestimate_##NAME##_##TPE(task, &estimations[MOSAIC_LINEAR], previous);\
	if (msg != MAL_SUCCEED) return msg;\
}


#define do_postEstimate(NAME, TPE, DUMMY_ARGUMENT) MOSpostEstimate_##NAME##_##TPE(task);

#define MOSestimate_AND_MOSoptimizerCost_DEF(TPE) \
static str MOSestimate_inner_##TPE(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous) {\
\
	MosaicEstimation estimations[MOSAICINDEX];\
	const int size = sizeof(estimations) / sizeof(MosaicEstimation);\
	for (int i = 0; i < size; i++) {\
		estimations[i].uncompressed_size = previous->uncompressed_size;\
		estimations[i].compressed_size = previous->compressed_size;\
		estimations[i].compression_strategy = previous->compression_strategy;\
		estimations[i].nr_var_encoded_blocks = previous->nr_var_encoded_blocks;\
		estimations[i].nr_var_encoded_elements = previous->nr_var_encoded_elements;\
		estimations[i].nr_capped_encoded_elements = previous->nr_capped_encoded_elements;\
		estimations[i].nr_capped_encoded_blocks = previous->nr_capped_encoded_blocks;\
		estimations[i].must_be_merged_with_previous = false;\
		estimations[i].is_applicable = false;\
	}\
\
	/* select candidate amongst those*/\
	if (task->filter[MOSAIC_RAW]){\
		DO_OPERATION_IF_ALLOWED(estimate, raw, TPE);\
	}\
	if (task->filter[MOSAIC_RLE]){\
		DO_OPERATION_IF_ALLOWED(estimate, runlength, TPE);\
	}\
	if (task->filter[MOSAIC_CAPPED]){\
		DO_OPERATION_IF_ALLOWED(estimate, capped, TPE);\
	}\
	if (task->filter[MOSAIC_VAR]){\
		DO_OPERATION_IF_ALLOWED(estimate, var, TPE);\
	}\
	if (task->filter[MOSAIC_DELTA]){\
		DO_OPERATION_IF_ALLOWED(estimate, delta, TPE);\
	}\
	if (task->filter[MOSAIC_LINEAR]){\
		DO_OPERATION_IF_ALLOWED(estimate, linear, TPE);\
	}\
	if (task->filter[MOSAIC_FRAME]){\
		DO_OPERATION_IF_ALLOWED(estimate, frame, TPE);\
	}\
	if (task->filter[MOSAIC_PREFIX]){\
		DO_OPERATION_IF_ALLOWED(estimate, prefix, TPE);\
	}\
\
	flt best_factor = 0.0;\
	current->is_applicable = false;\
\
	for (int i = 0; i < size; i++) {\
		flt factor = getFactor(estimations[i]);\
\
		if (estimations[i].is_applicable && best_factor < factor) {\
			*current = estimations[i];\
			best_factor = factor;\
		}\
	}\
\
	if (current->compression_strategy.tag == MOSAIC_RAW)	DO_OPERATION_IF_ALLOWED(postEstimate, raw, TPE);\
	if (current->compression_strategy.tag == MOSAIC_RLE)	DO_OPERATION_IF_ALLOWED(postEstimate, runlength, TPE);\
	if (current->compression_strategy.tag == MOSAIC_CAPPED)	DO_OPERATION_IF_ALLOWED(postEstimate, capped, TPE);\
	if (current->compression_strategy.tag == MOSAIC_VAR)	DO_OPERATION_IF_ALLOWED(postEstimate, var, TPE);\
	if (current->compression_strategy.tag == MOSAIC_DELTA)	DO_OPERATION_IF_ALLOWED(postEstimate, delta, TPE);\
	if (current->compression_strategy.tag == MOSAIC_LINEAR)	DO_OPERATION_IF_ALLOWED(postEstimate, linear, TPE);\
	if (current->compression_strategy.tag == MOSAIC_FRAME)	DO_OPERATION_IF_ALLOWED(postEstimate, frame, TPE);\
	if (current->compression_strategy.tag == MOSAIC_PREFIX)	DO_OPERATION_IF_ALLOWED(postEstimate, prefix, TPE);\
\
	return MAL_SUCCEED;\
}\
static str MOSestimate_##TPE(MOStask task, BAT* estimates, size_t* compressed_size) {\
	str result = MAL_SUCCEED;\
\
	*compressed_size = 0;\
\
	MosaicEstimation previous = {\
		.is_applicable = false,\
		.uncompressed_size = 0,\
		.compressed_size = 0,\
		.nr_var_encoded_elements = 0,\
		.nr_var_encoded_blocks = 0,\
		.nr_capped_encoded_elements = 0,\
		.nr_capped_encoded_blocks = 0,\
		.compression_strategy = {.tag = MOSAIC_EOL, .cnt = 0},\
		.must_be_merged_with_previous = false\
	};\
\
	MosaicEstimation current;\
	MosaicBlkRec* cursor = Tloc(estimates,0);\
\
	while(task->start < task->stop ){\
		/* default is to extend the non-compressed block with a single element*/\
		if ( (result = MOSestimate_inner_##TPE(task, &current, &previous)) ) {\
			return result;\
		}\
\
		if (!current.is_applicable) {\
			throw(MAL,"mosaic.compress", "Cannot compress BAT with given compression techniques.");\
		}\
\
		if (current.must_be_merged_with_previous) {\
			--cursor;\
			assert(cursor->tag == previous.compression_strategy.tag && cursor->cnt == previous.compression_strategy.cnt);\
			task->start -= previous.compression_strategy.cnt;\
		}\
		else BATcount(estimates)++;\
\
		*cursor = current.compression_strategy;\
		++cursor;\
		previous = current;\
		task->start += current.compression_strategy.cnt;\
	}\
\
	(*compressed_size) = current.compressed_size;\
\
	return MAL_SUCCEED;\
}

MOSestimate_AND_MOSoptimizerCost_DEF(bte)
MOSestimate_AND_MOSoptimizerCost_DEF(sht)
MOSestimate_AND_MOSoptimizerCost_DEF(int)
MOSestimate_AND_MOSoptimizerCost_DEF(lng)
MOSestimate_AND_MOSoptimizerCost_DEF(flt)
MOSestimate_AND_MOSoptimizerCost_DEF(dbl)
#ifdef HAVE_HGE
MOSestimate_AND_MOSoptimizerCost_DEF(hge)
#endif


static
str MOSestimate(MOStask task, BAT* estimates, size_t* compressed_size) {
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: return MOSestimate_bte(task, estimates, compressed_size);
	case TYPE_sht: return MOSestimate_sht(task, estimates, compressed_size);
	case TYPE_int: return MOSestimate_int(task, estimates, compressed_size);
	case TYPE_lng: return MOSestimate_lng(task, estimates, compressed_size);
	case TYPE_flt: return MOSestimate_flt(task, estimates, compressed_size);
	case TYPE_dbl: return MOSestimate_dbl(task, estimates, compressed_size);
#ifdef HAVE_HGE
	case TYPE_hge: return MOSestimate_hge(task, estimates, compressed_size);
#endif
	default:
		// Unknown block type. Should not happen.
		assert(0);
	}

	throw(MAL, "mosaic.estimate", TYPE_NOT_SUPPORTED);
}

static str
MOSfinalizeDictionary(MOStask task) {

	str error;

	if (task->filter[MOSAIC_VAR]) {
		if ((error = finalizeDictionary_var(task))) {
			return error;
		}
	}
	if (task->filter[MOSAIC_CAPPED]) {
		if ((error = finalizeDictionary_capped(task))) {
			return error;
		}
	}

	return MAL_SUCCEED;
}


#define do_compress(NAME, TPE, DUMMY_ARGUMENT)\
{\
	ALGODEBUG mnstr_printf(GDKstdout, "#MOScompress_" #NAME "\n");\
	MOScompress_##NAME##_##TPE(task, estimate);\
	MOSupdateHeader(task);\
	MOSadvance_##NAME##_##TPE(task);\
	MOSnewBlk(task);\
}

#define MOScompressInternal_DEF(TPE)\
static void \
MOScompressInternal_##TPE(MOStask task, BAT* estimates)\
{\
	/* second pass: compression phase*/\
	for(BUN i = 0; i < BATcount(estimates); i++) {\
		assert (task->dst < task->bsrc->tmosaic->base + task->bsrc->tmosaic->size );\
\
		MosaicBlkRec* estimate = Tloc(estimates, i);\
\
		switch(estimate->tag) {\
		case MOSAIC_RLE:\
			DO_OPERATION_IF_ALLOWED(compress, runlength, TPE);\
			break;\
		case MOSAIC_CAPPED:\
			DO_OPERATION_IF_ALLOWED(compress, capped, TPE);\
			break;\
		case MOSAIC_VAR:\
			DO_OPERATION_IF_ALLOWED(compress, var, TPE);\
			break;\
		case MOSAIC_DELTA:\
			DO_OPERATION_IF_ALLOWED(compress, delta, TPE);\
			break;\
		case MOSAIC_LINEAR:\
			DO_OPERATION_IF_ALLOWED(compress, linear, TPE);\
			break;\
		case MOSAIC_FRAME:\
			DO_OPERATION_IF_ALLOWED(compress, frame, TPE);\
			break;\
		case MOSAIC_PREFIX:\
			DO_OPERATION_IF_ALLOWED(compress, prefix, TPE);\
			break;\
		case MOSAIC_RAW:\
			DO_OPERATION_IF_ALLOWED(compress, raw, TPE);\
			break;\
		default : /* Unknown block type. Should not happen.*/\
			assert(0);\
		}\
	}\
}

MOScompressInternal_DEF(bte)
MOScompressInternal_DEF(sht)
MOScompressInternal_DEF(int)
MOScompressInternal_DEF(lng)
MOScompressInternal_DEF(flt)
MOScompressInternal_DEF(dbl)
#ifdef HAVE_HGE
MOScompressInternal_DEF(hge)
#endif

/* the source is extended with a BAT mosaic heap */
str
MOScompressInternal(BAT* bsrc, const char* compressions)
{
	MOStask task;
	str msg = MAL_SUCCEED;
	lng t0,t1;

  if (BATcheckmosaic(bsrc)){
		/* already compressed */

		return MAL_SUCCEED;
	}
    assert(bsrc->tmosaic == NULL);

	if ( BATcount(bsrc) < MOSAIC_THRESHOLD ){
		/* no need to compress */
		return MAL_SUCCEED;
	}

  t0 = GDKusec();

	if(BATmosaic(bsrc,  BATcapacity(bsrc) + (MosaicHdrSize + 2 * MosaicBlkSize)/Tsize(bsrc)+ BATTINY) == GDK_FAIL){
		// create the mosaic heap if not available.
		// The final size should be smaller then the original
		// It may, however, be the case that we mix a lot of RAW and, say, DELTA small blocks
		// Then we total size may go beyond the original size and we should terminate the process.
		// This should be detected before we compress a block, in the estimate functions
		// or when we extend the non-compressed collector block
		throw(MAL,"mosaic.compress", "heap construction failes");
	}

	assert(bsrc->tmosaic->parentid == bsrc->batCacheid);

	if((task = (MOStask) GDKzalloc(sizeof(*task))) == NULL) {
		MOSdestroy(bsrc);
		throw(MAL, "mosaic.compress", MAL_MALLOC_FAIL);
	}
	
	// initialize the non-compressed read pointer
	task->src = Tloc(bsrc, 0);
	task->start = 0;
	task->stop = BATcount(bsrc);
	task->timer = GDKusec();

	MOSinit(task,bsrc);
	task->blk->cnt= 0;
	MOSinitHeader(task);
	if (MOSinitializeFilter(task, compressions)) {
		msg = createException(MAL, "mosaic.compress", "No valid compression technique given or available for type: %s", ATOMname(task->type));
		MOSdestroy(bsrc);
		goto finalize;
	}

	// always start with an EOL block
	MOSsetTag(task->blk,MOSAIC_EOL);

	// Zero pass: estimation preparation phase
	if ((msg = MOSprepareEstimate(task))) {
		MOSdestroy(bsrc);
		goto finalize;
	}

	BAT* estimates;
	if (!(estimates = COLnew(0, TYPE_int, BATcount(bsrc), TRANSIENT)) ) {
		msg = createException(MAL, "mosaic.compress", "Could not allocate temporary estimates BAT.\n");
		MOSdestroy(bsrc);
		goto finalize;
	}

	size_t compressed_size_bytes;
	// First pass: estimation phase
	if ( ( msg = MOSestimate(task, estimates, &compressed_size_bytes) )) {
		BBPreclaim(estimates);
		MOSdestroy(bsrc);
		goto finalize;
	}

	if ((msg = MOSfinalizeDictionary(task))) {
		BBPreclaim(estimates);
		MOSdestroy(bsrc);
		goto finalize;
	}

	// set the exact necessary capacity
	if (HEAPextend(bsrc->tmosaic, compressed_size_bytes, true) != GDK_SUCCEED) {
		BBPreclaim(estimates);
		MOSdestroy(bsrc);
		goto finalize;
	}

	MOSinit(task, bsrc);

	task->start = 0;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: MOScompressInternal_bte(task, estimates); break;
	case TYPE_sht: MOScompressInternal_sht(task, estimates); break;
	case TYPE_int: MOScompressInternal_int(task, estimates); break;
	case TYPE_lng: MOScompressInternal_lng(task, estimates); break;
	case TYPE_flt: MOScompressInternal_flt(task, estimates); break;
	case TYPE_dbl: MOScompressInternal_dbl(task, estimates); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOScompressInternal_hge(task, estimates); break;
#endif
	default: // Unknown block type. Should not happen.
		assert(0);
	}

	task->bsrc->tmosaic->free = (task->dst - (char*)task->hdr);
	task->timer = GDKusec() - task->timer;

	// TODO: if we couldnt compress well enough, ignore the result

	bsrc->batDirtydesc = true;
	task->hdr->ratio = (flt)task->bsrc->theap.free/ task->bsrc->tmosaic->free;
finalize:
	GDKfree(task);

	t1 = GDKusec();
	ALGODEBUG mnstr_printf(GDKstdout, "##BATmosaic: mosaic construction " LLFMT " usec\n", t1 - t0);

	return msg;
}

str
MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	str msg = MAL_SUCCEED;
	BAT *b;
	bat *bid =getArgReference_bat(stk,pci,1);
	bat *ret =getArgReference_bat(stk,pci,0);

	(void) cntxt;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mosaic.compress", INTERNAL_BAT_ACCESS);

	/* views are never compressed */
    if (VIEWtparent(b)) {
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.compress", "Mosaic does not allow views as input.");
    }

	(void) mb;

	const char* compressions;
	if( pci->argc == 3) {
		compressions = *getArgReference_str(stk,pci,2);
	}
	else {
		compressions = str_nil;
	}

	MOSsetLock(b);
	msg= MOScompressInternal(b, compressions);
	MOSunsetLock(b);

	BBPkeepref(*ret = b->batCacheid);
	return msg;
}


#define do_decompress(NAME, TPE, DUMMY_ARGUMENT)\
{\
	ALGODEBUG mnstr_printf(GDKstdout, "#MOSdecompress_" #NAME "\n");\
	MOSdecompress_##NAME##_##TPE(task);\
	MOSadvance_##NAME##_##TPE(task);\
}

#define MOSdecompressInternal_DEF(TPE) \
static void MOSdecompressInternal_##TPE(MOStask task)\
{\
	while(MOSgetTag(task->blk) != MOSAIC_EOL){\
		switch(MOSgetTag(task->blk)){\
		case MOSAIC_RAW:\
			DO_OPERATION_IF_ALLOWED(decompress, raw, TPE);\
			break;\
		case MOSAIC_RLE:\
			DO_OPERATION_IF_ALLOWED(decompress, runlength, TPE);\
			break;\
		case MOSAIC_CAPPED:\
			DO_OPERATION_IF_ALLOWED(decompress, capped, TPE);\
			break;\
		case MOSAIC_VAR:\
			DO_OPERATION_IF_ALLOWED(decompress, var, TPE);\
			break;\
		case MOSAIC_DELTA:\
			DO_OPERATION_IF_ALLOWED(decompress, delta, TPE);\
			break;\
		case MOSAIC_LINEAR:\
			DO_OPERATION_IF_ALLOWED(decompress, linear, TPE);\
			break;\
		case MOSAIC_FRAME:\
			DO_OPERATION_IF_ALLOWED(decompress, frame, TPE);\
			break;\
		case MOSAIC_PREFIX:\
			DO_OPERATION_IF_ALLOWED(decompress, prefix, TPE);\
			break;\
		default: assert(0);\
		}\
	}\
}

MOSdecompressInternal_DEF(bte)
MOSdecompressInternal_DEF(sht)
MOSdecompressInternal_DEF(int)
MOSdecompressInternal_DEF(lng)
MOSdecompressInternal_DEF(flt)
MOSdecompressInternal_DEF(dbl)
#ifdef HAVE_HGE
MOSdecompressInternal_DEF(hge)
#endif

// recreate the uncompressed heap from its mosaic version
static str
MOSdecompressInternal(BAT** res, BAT* bsrc)
{	
	MOStask task;

	if (BATcheckmosaic(bsrc) == 0 ){
		*res = bsrc;
		BBPfix(bsrc->batCacheid); // We are just returning a reference to bsrc.
		return MAL_SUCCEED;
	}

	if (VIEWtparent(bsrc)) {
		throw(MAL, "mosaic.decompress", "cannot decompress tail-VIEW");
	}

	// use the original heap for reconstruction
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		throw(MAL, "mosaic.decompress", MAL_MALLOC_FAIL);
	}

	BBPshare(bsrc->tmosaic->parentid);

	*res = COLnew(0, bsrc->ttype, bsrc->batCapacity, TRANSIENT);
	BATsetcount(*res, bsrc->batCount);
	(*res)->tmosaic = bsrc->tmosaic;
	(*res)->tvmosaic = bsrc->tvmosaic;

 	// TODO: We should also compress the string heap itself somehow.
	// For now we just share the string heap of the original compressed bat.
	if (bsrc->tvheap) {
		BBPshare(bsrc->tvheap->parentid);
		(*res)->tvheap = bsrc->tvheap;
	}

	(*res)->tkey = bsrc->tkey;
	(*res)->tsorted = bsrc->tsorted;
	(*res)->trevsorted = bsrc->trevsorted;
	(*res)->tseqbase = oid_nil;
	(*res)->tnonil = bsrc->tnonil;
	(*res)->tnil = bsrc->tnil;

	MOSinit(task,bsrc);

	task->bsrc = *res;
	task->src = Tloc(*res, 0);

	task->timer = GDKusec();

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: MOSdecompressInternal_bte(task); break;
	case TYPE_sht: MOSdecompressInternal_sht(task); break;
	case TYPE_int: MOSdecompressInternal_int(task); break;
	case TYPE_lng: MOSdecompressInternal_lng(task); break;
	case TYPE_flt: MOSdecompressInternal_flt(task); break;
	case TYPE_dbl: MOSdecompressInternal_dbl(task); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOSdecompressInternal_hge(task); break;
#endif
	default: // Unknown block type. Should not happen.
		assert(0);
	}

	task->timer = GDKusec() - task->timer;

	// remove the compressed mirror
	GDKfree(task); // TODO should there not be a more thorough clean up function for tasks
	// continue with all work

	BATsettrivprop(bsrc); // TODO: What's the purpose of this statement?

	return MAL_SUCCEED;
}

// decompression does not change the BAT id
str
MOSdecompress(bat* ret, const bat* bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mosaic.decompress", INTERNAL_BAT_ACCESS);

	BAT* res;

	MOSsetLock(b);

	str result = MOSdecompressInternal( &res, b);

	MOSunsetLock(b);

	BBPunfix(b->batCacheid);

	BBPkeepref(res->batCacheid);

	*ret = res->batCacheid;

	// TODO: handle errors

	return result;
}

// The remainders is cloned from the generator code base
// overload the algebra functions to check for compressed heaps.
static int // TODO make boolean
isCompressed(bat bid)
{
	BAT *b;
	int r=0;
	if( bid == 0)
		return 0;
	b = BATdescriptor(bid);

	MOSsetLock(b);
	r = BATcheckmosaic(b);
	MOSunsetLock(b);
	BBPunfix(bid);
	return r;
}

MOSselect_generic_DEF(bte)
MOSselect_generic_DEF(sht)
MOSselect_generic_DEF(int)
MOSselect_generic_DEF(lng)
MOSselect_generic_DEF(flt)
MOSselect_generic_DEF(dbl)
#ifdef HAVE_HGE
MOSselect_generic_DEF(hge)
#endif

/* The algebra operators should fall back to their default
 * when we know that the heap is not compressed
 * The actual decompression should wait until we know that
 * the administration thru SQL layers works properly.
 *
 * The oid-range can be reduced due to partitioning.
 */

static str
MOSselect2(bat *ret, const bat *bid, const bat *cid, void *low, void *hgh, bit *li, bit *hi, bit *anti) {
	BAT *b, *bn, *cand = NULL;
	str msg = MAL_SUCCEED;
	BUN cnt = 0;
	MOStask task;
	//
	// use default implementation if possible
	if( !isCompressed(*bid)){
		if(cid)
			return ALGselect2(ret,bid,cid,low,hgh,li,hi,anti);
		else
			return ALGselect1(ret,bid,low,hgh,li,hi,anti);
	}

	b= BATdescriptor(*bid);
	if( b == NULL)
			throw(MAL, "mosaic.select",RUNTIME_OBJECT_MISSING);

	// TODO: why is this task on the heap?
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.select", RUNTIME_OBJECT_MISSING);
	}

	// accumulator for the oids
	bn = COLnew((oid)0, TYPE_oid, BATcount(b), TRANSIENT);
	if( bn == NULL){
		GDKfree(task);
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.select", RUNTIME_OBJECT_MISSING);
	}
	task->lb = (oid*) Tloc(bn,0);

	MOSinit(task,b);
	MOSinitializeScan(task, b);
	// drag along the candidate list into the task descriptor
	if (cid) {
		cand = BATdescriptor(*cid);
		if (cand == NULL){
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			GDKfree(task);
			throw(MAL, "mosaic.select", RUNTIME_OBJECT_MISSING);
		}
		task->cl = (oid*) Tloc(cand, 0);
		task->n = BATcount(cand);
	}

	struct canditer ci;
	task->ci = &ci;
	canditer_init(task->ci, b, cand);

	// determine block range to scan for partitioned columns
	/*
	** TODO: Figure out how do partitions relate to mosaic chunks.
	** Is it a good idea to set the capacity to the total size of the select operand b?
	*/

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: MOSselect_bte(task, low, hgh, li, hi, anti); break;
	case TYPE_sht: MOSselect_sht(task, low, hgh, li, hi, anti); break;
	case TYPE_int: MOSselect_int(task, low, hgh, li, hi, anti); break;
	case TYPE_lng: MOSselect_lng(task, low, hgh, li, hi, anti); break;
	case TYPE_flt: MOSselect_flt(task, low, hgh, li, hi, anti); break;
	case TYPE_dbl: MOSselect_dbl(task, low, hgh, li, hi, anti); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOSselect_hge(task, low, hgh, li, hi, anti); break;
#endif
	}
	// derive the filling
	cnt = (BUN) (task->lb - (oid*) Tloc(bn,0));
	assert(bn->batCapacity >= cnt);
	BATsetcount(bn,cnt);
	bn->tnil = false;
	bn->tnonil = true;
	bn->tsorted = true;
	bn->trevsorted = cnt <=1;
	bn->tkey = true;
	MOSvirtualize(bn);

	*ret = bn->batCacheid;
	GDKfree(task);

	BBPunfix(b->batCacheid);
	if (cand != NULL) BBPunfix(cand->batCacheid);
	BBPkeepref(bn->batCacheid);
	return msg;
}

str
MOSselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *li, *hi, *anti;
	void *low, *hgh;
	bat *ret, *bid, *cid = NULL;
	int i;
	(void) cntxt;
	(void) mb;
	ret = getArgReference_bat(stk, pci, 0);
	bid = getArgReference_bat(stk, pci, 1);

	if (pci->argc == 8) {	/* candidate list included */
		cid = getArgReference_bat(stk, pci, 2);
		i = 3;
	} else
		i = 2;
	low = (void *) getArgReference(stk, pci, i);
	hgh = (void *) getArgReference(stk, pci, i + 1);
	li = getArgReference_bit(stk, pci, i + 2);
	hi = getArgReference_bit(stk, pci, i + 3);
	anti = getArgReference_bit(stk, pci, i + 4);

	return MOSselect2(ret, bid, cid, low, hgh, li, hi, anti);
}

str MOSthetaselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx;
	bat *cid =0,  *ret, *bid;
	char **oper;
	void *val;

	(void) cntxt;
	(void) mb;
	ret= getArgReference_bat(stk,pci,0);
	bid= getArgReference_bat(stk,pci,1);
	if( pci->argc == 5){ // candidate list included
		cid = getArgReference_bat(stk,pci, 2);
		idx = 3;
	} else idx = 2;
	val= (void*) getArgReference(stk,pci,idx);
	oper= getArgReference_str(stk,pci,idx+1);

	if( !isCompressed(*bid)){
		if( cid)
			return ALGthetaselect2(ret,bid,cid,val, (const char **)oper);
		else
			return ALGthetaselect1(ret,bid,val, (const char **)oper);
	}

	BAT* b;
	if ((b = BBPquickdesc(*bid, false)) == NULL) {
		throw(MAL, "mosaic.MOSthetaselect", RUNTIME_OBJECT_MISSING);
	}

	const char* op = *oper;
	const void *nil = ATOMnilptr(b->ttype);
	if (ATOMcmp(b->ttype, val, (void *) nil) == 0) {
		BAT* bn =  BATdense(0, 0, 0);
		BBPkeepref(*ret = bn->batCacheid);
		return MAL_SUCCEED;
	}
	if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[1] == 0)) {
		/* "=" or "==" */
		bit li = true;
		bit hi = true;
		bit anti = false;
		return MOSselect2(ret, bid, cid, val, val, &li, &hi, &anti);
	}
	if (op[0] == '!' && op[1] == '=' && op[2] == 0) {
		/* "!=" (equivalent to "<>") */
		bit li = true;
		bit hi = true;
		bit anti = true;
		return MOSselect2(ret, bid, cid, val, val, &li, &hi, &anti);
	}
	if (op[0] == '<') {
		if (op[1] == 0) {
			/* "<" */
		bit li = false;
		bit hi = false;
		bit anti = false;
		return MOSselect2(ret, bid, cid, (void *) nil, val, &li, &hi, &anti);
		}
		if (op[1] == '=' && op[2] == 0) {
			/* "<=" */
			bit li = false;
			bit hi = true;
			bit anti = false;
			return MOSselect2(ret, bid, cid, (void *) nil, val, &li, &hi, &anti);
		}
		if (op[1] == '>' && op[2] == 0) {
			/* "<>" (equivalent to "!=") */
			bit li = true;
			bit hi = true;
			bit anti = true;
			return MOSselect2(ret, bid, cid, val, (void *) nil, &li, &hi, &anti);
		}
	}
	if (op[0] == '>') {
		if (op[1] == 0) {
			/* ">" */
			bit li = false;
			bit hi = false;
			bit anti = false;
			return MOSselect2(ret, bid, cid, val, (void *) nil, &li, &hi, &anti);
		}
		if (op[1] == '=' && op[2] == 0) {
			/* ">=" */
			bit li = true;
			bit hi = false;
			bit anti = false;
			return MOSselect2(ret, bid, cid, val, (void *) nil, &li, &hi, &anti);
		}
	}

	throw(MAL, "mosaic.MOSthetaselect", "unknown operator.");
}

MOSprojection_generic_DEF(bte)
MOSprojection_generic_DEF(sht)
MOSprojection_generic_DEF(int)
MOSprojection_generic_DEF(lng)
MOSprojection_generic_DEF(flt)
MOSprojection_generic_DEF(dbl)
#ifdef HAVE_HGE
MOSprojection_generic_DEF(hge)
#endif

str MOSprojection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret, *lid =0, *rid=0;
	BAT *bl = NULL, *br = NULL, *bn;
	BUN cnt;
	oid *ol =0, o = 0;
	str msg= MAL_SUCCEED;
	MOStask task;

	(void) cntxt;
	(void) mb;

	ret = getArgReference_bat(stk,pci,0);
	lid = getArgReference_bat(stk,pci,1);
	rid = getArgReference_bat(stk,pci,2);

	if( !isCompressed(*rid))
		return ALGprojection(ret,lid,rid);

	br = BATdescriptor(*rid);
	if( br == NULL){
		throw(MAL,"mosaic.projection",RUNTIME_OBJECT_MISSING);
	}

	bl = BATdescriptor(*lid);
	if( bl == NULL){
		BBPunfix(*rid);
		throw(MAL,"mosaic.projection",RUNTIME_OBJECT_MISSING);
	}

	if (/*not a candidate list*/ !(bl->tkey && bl->tsorted && bl->tnonil)) {

		msg = ALGprojection(ret, lid, rid);
		BBPunfix(*lid);
		BBPunfix(*rid);

		return msg;
	}

	if (bl->ttype == TYPE_void && BATcount(bl) == BATcount(br)) {

		if (bl->tseqbase == br->hseqbase) {
			/* The left side is a dense candidate list covering the entire right side.
			* So we can just return the right side as is.
			*/
			BBPkeepref(*ret = br->batCacheid);
			BBPunfix(*lid);
		}
		else {
			/* Something is probably wrong if the left tseqbase and right hseqbase aren't equal.*/
			msg = ALGprojection(ret, lid, rid);
			BBPunfix(*lid);
			BBPunfix(*rid);
		}

		return msg;
	}

	cnt = BATcount(bl);
	bn = COLnew((oid)0,br->ttype, cnt, TRANSIENT);
	if ( bn == NULL){
		BBPunfix(*lid);
		BBPunfix(*rid);
		throw(MAL,"mosaic.projection",MAL_MALLOC_FAIL);
	}

	if ( bl->ttype == TYPE_void)
		o = bl->tseqbase;
	else
		ol = (oid*) Tloc(bl,0);

	(void) o;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPunfix(*lid);
		BBPunfix(*rid);
		GDKfree(task);
		throw(MAL, "mosaic.projection", RUNTIME_OBJECT_MISSING);
	}
	MOSinit(task,br);
	MOSinitializeScan(task, br);
	task->src = (char*) Tloc(bn,0);

	task->cl = ol;
	task->n = cnt;

	struct canditer ci;
	task->ci = &ci;
	canditer_init(task->ci, NULL, bl);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: MOSprojection_bte(task); break;
	case TYPE_sht: MOSprojection_sht(task); break;
	case TYPE_int: MOSprojection_int(task); break;
	case TYPE_lng: MOSprojection_lng(task); break;
	case TYPE_flt: MOSprojection_flt(task); break;
	case TYPE_dbl: MOSprojection_dbl(task); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOSprojection_hge(task); break;
#endif
	}

	/* adminstrative wrapup of the projection */
	BBPunfix(*lid);
	BBPunfix(*rid);

	BATsetcount(bn,task->cnt);
	bn->tnil = 0;
	bn->tnonil = br->tnonil;
	bn->tsorted = bn->trevsorted = cnt <= 1;
	BBPkeepref(*ret = bn->batCacheid);
	GDKfree(task);
	return msg;
}

MOSjoin_generic_COUI_DEF(bte)
MOSjoin_generic_COUI_DEF(sht)
MOSjoin_generic_COUI_DEF(int)
MOSjoin_generic_COUI_DEF(lng)
MOSjoin_generic_COUI_DEF(flt)
MOSjoin_generic_COUI_DEF(dbl)
#ifdef HAVE_HGE
MOSjoin_generic_COUI_DEF(hge)
#endif

MOSjoin_generic_DEF(bte)
MOSjoin_generic_DEF(sht)
MOSjoin_generic_DEF(int)
MOSjoin_generic_DEF(lng)
MOSjoin_generic_DEF(flt)
MOSjoin_generic_DEF(dbl)
#ifdef HAVE_HGE
MOSjoin_generic_DEF(hge)
#endif

/* A mosaic join operator that works when either the left or the right side is compressed
 * and there are no additional candidate lists.
 * Furthermore if both sides are in possesion of a mosaic index,
 * the operator implementation currently only uses the mosaic index of the left side.
 */

#define PREPARE_JOIN_CONTEXT(COMPRESSED_BAT, COMPRESSED_BAT_CL, UNCOMPRESSED_BAT, UNCOMPRESSED_BAT_CL) \
{\
	MOSinit(task,COMPRESSED_BAT);\
	MOSinitializeScan(task, COMPRESSED_BAT);\
	task->stop = BATcount(COMPRESSED_BAT);\
	task->src= Tloc(UNCOMPRESSED_BAT,0);\
	if (*COMPRESSED_BAT_CL != bat_nil && ((cand_c = BATdescriptor(*COMPRESSED_BAT_CL)) == NULL))\
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);\
	if (*UNCOMPRESSED_BAT_CL != bat_nil && ((cand_u = BATdescriptor(*UNCOMPRESSED_BAT_CL)) == NULL))\
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);\
	canditer_init(&ci_c, COMPRESSED_BAT, cand_c);\
	canditer_init(&ci_u, UNCOMPRESSED_BAT, cand_u);\
	u = UNCOMPRESSED_BAT;\
}

str
MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret, *ret2,*lid,*rid, *sl, *sr;
	bit *nil_matches= 0;
	lng *estimate;
	BAT  *bl = NULL, *br = NULL, *bln = NULL, *brn= NULL;
	BUN cnt = 0;
	int swapped = 0;
	str msg = MAL_SUCCEED;
	MOStask task;

	(void) cntxt;
	(void) mb;
	ret = getArgReference_bat(stk,pci,0);
	ret2 = getArgReference_bat(stk,pci,1);
	lid = getArgReference_bat(stk,pci,2);
	rid = getArgReference_bat(stk,pci,3);
	nil_matches = getArgReference_bit(stk,pci,6);
	estimate = getArgReference_lng(stk,pci,7);

	sl = (bat*) getArgReference(stk,pci,4) ;
	sr = (bat*) getArgReference(stk,pci,5);
	if( !isCompressed(*lid) && !isCompressed(*rid) )
		return ALGjoin(ret,ret2,lid,rid,sl,sr,nil_matches,estimate);

	bl = BATdescriptor(*lid);
	if( bl == NULL)
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
	br = BATdescriptor(*rid);
	if( br == NULL){
		BBPunfix(bl->batCacheid);
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
	}

	// result set preparation
	bln = COLnew((oid)0,TYPE_oid, cnt, TRANSIENT);
	brn = COLnew((oid)0,TYPE_oid, cnt, TRANSIENT);
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( bln == NULL || brn == NULL || task == NULL){
		if( bln) BBPunfix(bln->batCacheid);
		if( brn) BBPunfix(brn->batCacheid);
		BBPunfix(bl->batCacheid);
		BBPunfix(br->batCacheid);
		throw(MAL,"mosaic.join",MAL_MALLOC_FAIL);
	}

	// if the left side is not compressed, then assume the right side is compressed.

	BAT* cand_c = NULL;
	BAT* cand_u = NULL;
	BAT* u;

	struct canditer ci_c;
	struct canditer ci_u;

	task->ci = &ci_c;

	if ( bl->tmosaic){
		PREPARE_JOIN_CONTEXT(bl, sl, br, sr);
	} else {
		PREPARE_JOIN_CONTEXT(br, sr, bl, sl);
		swapped=1;
	}
	task->lbat = bln;
	task->rbat = brn;

	bit* COUI = NULL;

	if ((pci->argc != 9) || ((*(COUI = getArgReference_bit(stk,pci,8))) == bit_nil) || (*COUI == false)) {
		switch(ATOMbasetype(task->type)){
		case TYPE_bte: msg = MOSjoin_bte(task, u, &ci_u, *nil_matches); break;
		case TYPE_sht: msg = MOSjoin_sht(task, u, &ci_u, *nil_matches); break;
		case TYPE_int: msg = MOSjoin_int(task, u, &ci_u, *nil_matches); break;
		case TYPE_lng: msg = MOSjoin_lng(task, u, &ci_u, *nil_matches); break;
		case TYPE_flt: msg = MOSjoin_flt(task, u, &ci_u, *nil_matches); break;
		case TYPE_dbl: msg = MOSjoin_dbl(task, u, &ci_u, *nil_matches); break;
#ifdef HAVE_HGE
		case TYPE_hge: msg = MOSjoin_hge(task, u, &ci_u, *nil_matches); break;
#endif
		}
	}
	else {
		switch(ATOMbasetype(task->type)){
		case TYPE_bte: msg = MOSjoin_COUI_bte(task, u, &ci_u, *nil_matches); break;
		case TYPE_sht: msg = MOSjoin_COUI_sht(task, u, &ci_u, *nil_matches); break;
		case TYPE_int: msg = MOSjoin_COUI_int(task, u, &ci_u, *nil_matches); break;
		case TYPE_lng: msg = MOSjoin_COUI_lng(task, u, &ci_u, *nil_matches); break;
		case TYPE_flt: msg = MOSjoin_COUI_flt(task, u, &ci_u, *nil_matches); break;
		case TYPE_dbl: msg = MOSjoin_COUI_dbl(task, u, &ci_u, *nil_matches); break;
#ifdef HAVE_HGE
		case TYPE_hge: msg = MOSjoin_COUI_hge(task, u, &ci_u, *nil_matches); break;
#endif
		}
	}

	(void) BBPreclaim(bl);
	(void) BBPreclaim(br);

	(void) BBPreclaim(cand_c);
	(void) BBPreclaim(cand_u);

	if (msg) {
		(void) BBPreclaim(bln);
		(void) BBPreclaim(brn);
		return msg;
	}

	BATsettrivprop(bln);
	BATsettrivprop(brn);
    if( swapped){
        BBPkeepref(*ret= brn->batCacheid);
        BBPkeepref(*ret2= bln->batCacheid);
    } else {
        BBPkeepref(*ret= bln->batCacheid);
        BBPkeepref(*ret2= brn->batCacheid);
    }

	GDKfree(task);
    return msg;
}

// The analyse routine runs through the BAT dictionary and assess
// all possible compression options.

/*
 * Start searching for a proper compression scheme.
 * Naive creation of all patterns in increasing number of bits
 */

#define STEP MOSAIC_METHODS
static int
makepatterns(int *patterns, int size, str compressions, BAT* b)
{
	int i,j,k, idx, bit=1, step = MOSAIC_METHODS - 1;
	int lim= 8*7*6*5*4*3*2;
	int candidate[MOSAIC_METHODS]= {0};

	for( i = 0; i < MOSAIC_METHODS-1; i++)
		candidate[i] = (compressions == NULL || strstr(compressions,MOSfiltername[i]) != 0) &&  MOSisTypeAllowed(i, b);

	for( k=0, i=0; i<lim && k <size; i++){
		patterns[k]=0;
		idx =i;
		while(idx > 0) {
			if( idx % step && candidate[ idx % step]) 
					patterns[k] |= 1 <<(idx % step);
			idx /= step;
		}

		// weed out duplicates
		for( j=0; j< k; j++)
			if(patterns[k] == patterns[j]) break;
		if( j < k ) continue;
		
#ifdef _MOSAIC_DEBUG_
		mnstr_printf(GDKstdout,"#");
		for(j=0, bit=1; j < MOSAIC_METHODS-1; j++){
			mnstr_printf(GDKstdout,"%d", (patterns[k] & bit) > 0);
			bit *=2;
		}
		mnstr_printf(GDKstdout,"\n");
#else
		(void) bit;
#endif
		k++;
	}
#ifdef _MOSAIC_DEBUG_
	mnstr_printf(GDKstdout,"lim %d k %d\n",lim,k);
#endif
	return k;
}

/*
 * An analysis of all possible compressors
 * Drop techniques if they are not able to reduce the size below a factor 1.0
 */
#define CANDIDATES 256  /* all three combinations */

struct PAT{
	bool include;
	str technique;
	BUN xsize;
	dbl xf;
	lng clk1, clk2;
}pat[CANDIDATES];

void
MOSAnalysis(BAT *b, BAT *btech, BAT *boutput, BAT *bratio, BAT *bcompress, BAT *bdecompress, str compressions)
{
	int i,j,cases, bit=1, bid= b->batCacheid;
	int pattern[CANDIDATES];
	int antipattern[CANDIDATES];
	int antipatternSize = 0;
	char buf[1024]={0}, *t;

	int filter[MOSAIC_METHODS];

	// create the list of all possible 2^6 compression patterns 
	cases = makepatterns(pattern,CANDIDATES, compressions, b);

	memset(antipattern,0, sizeof(antipattern));
	antipatternSize++; // the first pattern aka 0 is always an antipattern.

	memset((char*)pat,0, sizeof(pat));

	for( i = 1; i< cases; i++) {
		pat[i].include = true;
		// Ignore patterns that have a poor or unused individual compressor
		bool skip = false;
		for( j=1; j < antipatternSize; j++) {
				if ( (pattern[i] & antipattern[j]) == antipattern[j] && pattern[i] > antipattern[j]) {
					pat[i].include = false;
					skip = true;
					break;
				}
			}
		if(skip) continue;

		t= buf;
		*t =0;
		memset(filter, 0, sizeof(filter));
		for(j=0, bit=1; j < MOSAIC_METHODS-1; j++){
			filter[j]= (pattern[i] & bit)>0;
			bit *=2;
			if( filter[j]){
				snprintf(t, 1024-strlen(buf),"%s ", MOSfiltername[j]);
				t= buf + strlen(buf);
			}
		}
		*t = '\0';

		t = buf;

		pat[i].technique= GDKstrdup(buf);
		pat[i].clk1 = GDKms();

		MOSsetLock(b);

		Heap* original = NULL;

		if (BATcheckmosaic(b)){
			original = b->tmosaic;
			b->tmosaic = NULL;
		}

		const char* compressions = buf;
		MOScompressInternal( b, compressions);
		pat[i].clk1 = GDKms()- pat[i].clk1;

		if(b->tmosaic == NULL){
			// aborted compression experiment
			MOSdestroy(BBPdescriptor(bid));

			if (original) {
				b->tmosaic = original;
			}
			pat[i].include = false;
			MOSunsetLock(b);
			continue;
		}

		pat[i].xsize = (BUN) b->tmosaic->free;
		pat[i].xf= ((MosaicHdr)  b->tmosaic->base)->ratio;

		// analyse result block distribution to exclude complicated compression combination that (probably) won't improve compression rate.
		if ( i < MOSAIC_METHODS-1 && pat[i].xf >= 0 && pat[i].xf < 1.0) {
				antipattern[antipatternSize++] = pattern[i];
		}
		else {
			for(j=1; j < MOSAIC_METHODS-1; j++){
				if ( ((MosaicHdr)  b->tmosaic->base)->blks[j] == 0) {
					antipattern[antipatternSize++] = pattern[i];
					pat[i].include = false;
				}
			}
		}

		BAT* decompressed;
		pat[i].clk2 = GDKms();
		MOSdecompressInternal( &decompressed, b);
		pat[i].clk2 = GDKms()- pat[i].clk2;
		MOSdestroy(decompressed);
		BBPunfix(decompressed->batCacheid);

		// get rid of mosaic heap
		MOSdestroy(b);

		if (original) {
			b->tmosaic = original;
		}

		MOSunsetLock(b);
	}

	// Collect the results in a table
	for(i=0;i< CANDIDATES; i++){
		if(pat[i].include) {

			// round down to three decimals.
			pat[i].xf = ((dbl) (int) (pat[i].xf * 1000)) / 1000;

			if( BUNappend(boutput,&pat[i].xsize,false) != GDK_SUCCEED ||
				BUNappend(btech,pat[i].technique,false) != GDK_SUCCEED ||
				BUNappend(bratio,&pat[i].xf,false) != GDK_SUCCEED ||
				BUNappend(bcompress,&pat[i].clk1,false) != GDK_SUCCEED ||
				BUNappend(bdecompress,&pat[i].clk2,false) != GDK_SUCCEED )
					return;
		}

		GDKfree(pat[i].technique);
	}
}

/* slice a fixed size atom into thin bte-wide columns, used for experiments */
str
MOSsliceInternal(bat *slices, BUN size, BAT *b)
{
	BUN i;
	BUN cnt= BATcount(b);
	BAT *bats[8];
	bte *thin[8];
	assert(size < 8);

	for( i = 0; i< size; i++){
		bats[i] = COLnew((oid)0,TYPE_bte, cnt, TRANSIENT);
		if ( bats[i] == NULL){
			for( ;i>0; i--)
				BBPunfix(bats[--i]->batCacheid);
			throw(MAL,"mosaic.slice", MAL_MALLOC_FAIL);
		}
		slices[i] = bats[i]->batCacheid;
		thin[i]= (bte*) Tloc(bats[i],0);
		BATsetcount(bats[i], cnt);
	}
	switch(b->ttype){
	case TYPE_int:
	{ union {
		unsigned int val;
		bte thin[4];
	  } map;
	  unsigned int *val = (unsigned int*) Tloc(b,0);
	  for(i=0; i < cnt; i++, val++){
		map.val = *val;
		*thin[0] = map.thin[0]; thin[0]++;
		*thin[1] = map.thin[1]; thin[1]++;
		*thin[2] = map.thin[2]; thin[2]++;
		*thin[3] = map.thin[3]; thin[3]++;
	  }
	}
	break;
	case TYPE_lng:
	{ union {
		lng val;
		bte thin[8];
	  } map;
	  unsigned int *val = (unsigned int*) Tloc(b,0);
	  for(i=0; i < cnt; i++, val++){
		map.val = *val;
		*thin[0] = map.thin[0]; thin[0]++;
		*thin[1] = map.thin[1]; thin[1]++;
		*thin[2] = map.thin[2]; thin[2]++;
		*thin[3] = map.thin[3]; thin[3]++;
		*thin[4] = map.thin[4]; thin[4]++;
		*thin[5] = map.thin[5]; thin[5]++;
		*thin[6] = map.thin[6]; thin[6]++;
		*thin[7] = map.thin[7]; thin[7]++;
	  }
	}
	break;
	default:
		assert(0);
	}
	return MAL_SUCCEED;
}

str
MOSslice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat slices[8];
	BAT *b;
	BUN s;

	(void) cntxt;
	s = (BUN) ATOMsize(getArgType(mb,pci,pci->retc));
	if( s > 8)
		throw(MAL,"mosaic.slice", "illegal type witdh");
	b = BATdescriptor(* getArgReference_bat(stk,pci, pci->retc));
	if ( b == NULL)
		throw(MAL,"mosaic.slice", RUNTIME_OBJECT_MISSING);
	return MOSsliceInternal( slices, s,b);
}
