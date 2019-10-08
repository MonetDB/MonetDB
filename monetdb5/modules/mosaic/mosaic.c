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
#include "mosaic_delta.h"
#include "mosaic_linear.h"
#include "mosaic_frame.h"
#include "mosaic_prefix.h"

char *MOSfiltername[]={"raw","runlength","capped","delta","linear","frame","prefix","EOL"};

bool MOSisTypeAllowed(int compression, BAT* b) {
	switch (compression) {
	case MOSAIC_RAW:		return MOStypes_raw(b);
	case MOSAIC_RLE:		return MOStypes_runlength(b);
	case MOSAIC_CAPPED:		return MOStypes_capped(b);
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

	if( b->tmosaic == NULL)
			throw(MAL,"mosaic.layout","Compression heap missing");

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

	if( BUNappend(btech, "========", false) != GDK_SUCCEED ||
		BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
		BUNappend(binput, &zero, false) != GDK_SUCCEED ||
		BUNappend(boutput, &zero , false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
			throw(MAL,"mosaic.layout", MAL_MALLOC_FAIL);

	while(task->start< task->stop){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKout, "MOSlayout_raw\n");
			MOSlayout_raw(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_raw(task);
			break;
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKout, "MOSlayout_runlength\n");
			MOSlayout_runlength(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_runlength(task);
			break;
		case MOSAIC_CAPPED:
			ALGODEBUG mnstr_printf(GDKout, "MOSlayout_capped\n");
			MOSlayout_capped(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_capped(task);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKout, "MOSlayout_delta\n");
			MOSlayout_delta(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_delta(task);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKout, "MOSlayout_linear\n");
			MOSlayout_linear(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_linear(task);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKout, "MOSlayout_frame\n");
			MOSlayout_frame(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_frame(task);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKout, "MOSlayout_prefix\n");
			MOSlayout_prefix(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_prefix(task);
			break;
		default:
			assert(0);
		}
	}
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

/* The compression orchestration is dealt with here.
 * We assume that the estimates for each scheme returns
 * the number of elements it applies to. Moreover, we
 * assume that the compression factor holds for any subsequence.
 * This allows us to avoid expensive estimate calls when a small
 * sequence is found with high compression factor.
 */
static str
MOSoptimizerCost(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous) {
	str result = MAL_SUCCEED;

	MosaicEstimation estimations[MOSAICINDEX];
	const int size = sizeof(estimations) / sizeof(MosaicEstimation);
	for (int i = 0; i < size; i++) {
		estimations[i].uncompressed_size = previous->uncompressed_size;
		estimations[i].compressed_size = previous->compressed_size;
		estimations[i].compression_strategy = previous->compression_strategy;
		estimations[i].must_be_merged_with_previous = false;
		estimations[i].is_applicable = false;
	}

	// select candidate amongst those
	if (task->filter[MOSAIC_RAW]){
		if( (result = MOSestimate_raw(task, &estimations[MOSAIC_RAW], previous))) {
			return result;
		}
	}
	if (task->filter[MOSAIC_RLE]){
		if( (result = MOSestimate_runlength(task, &estimations[MOSAIC_RLE], previous))) {
			return result;
		}
	}
	if (task->filter[MOSAIC_CAPPED]){
		if( (result = MOSestimate_capped(task, &estimations[MOSAIC_CAPPED], previous))) {
			return result;
		}
	}
	if (task->filter[MOSAIC_DELTA]){
		if( (result = MOSestimate_delta(task, &estimations[MOSAIC_DELTA], previous))) {
			return result;
		}
	}
	if (task->filter[MOSAIC_LINEAR]){
		if( (result = MOSestimate_linear(task, &estimations[MOSAIC_LINEAR], previous))) {
			return result;
		}
	}
	if (task->filter[MOSAIC_FRAME]){
		if( (result = MOSestimate_frame(task, &estimations[MOSAIC_FRAME], previous))) {
			return result;
		}
	}
	if (task->filter[MOSAIC_PREFIX]){
		if( (result = MOSestimate_prefix(task, &estimations[MOSAIC_PREFIX], previous))) {
			return result;
		}
	}

	flt best_factor = 0.0;
	current->is_applicable = false;

	for (int i = 0; i < size; i++) {
		flt factor = getFactor(estimations[i]);

		if (estimations[i].is_applicable && best_factor < factor) {
			*current = estimations[i];
			best_factor = factor;
		}
	}

	return result;
}

static
str MOSestimate(MOStask task, BAT* estimates, size_t* compressed_size) {
	str result = MAL_SUCCEED;

	*compressed_size = 0;

	MosaicEstimation previous = {
		.is_applicable = false,
		.uncompressed_size = 0,
		.compressed_size = 0,
		.compression_strategy = {.tag = MOSAIC_EOL, .cnt = 0},
		.must_be_merged_with_previous = false
	};

	MosaicEstimation current;
	MosaicBlkRec* cursor = Tloc(estimates,0);

	while(task->start < task->stop ){
		// default is to extend the non-compressed block with a single element
		if ( (result = MOSoptimizerCost(task, &current, &previous)) ) {
			return result;
		}

		if (!current.is_applicable) {
			throw(MAL,"mosaic.compress", "Cannot compress BAT with given compression techniques.");
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

	if( task->filter[MOSAIC_CAPPED])
		MOScreatedictionary(task);
	// always start with an EOL block
	MOSsetTag(task->blk,MOSAIC_EOL);

	BAT* estimates;
	
	if (!(estimates = COLnew(0, TYPE_int, BATcount(bsrc), TRANSIENT)) ) {
		msg = createException(MAL, "mosaic.compress", "Could not allocate temporary estimates BAT.\n");
		MOSdestroy(bsrc);
		goto finalize;
	}

	size_t compressed_size_bytes;
	// First pass: estimation phase
	if ( ( msg = MOSestimate(task, estimates, &compressed_size_bytes) ) != MAL_SUCCEED) {
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

	// second pass: compression phase
	for(BUN i = 0; i < BATcount(estimates); i++) {
		assert (task->dst < bsrc->tmosaic->base + bsrc->tmosaic->size );

		MosaicBlkRec* estimate = Tloc(estimates, i);

		switch(estimate->tag) {
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKout, "MOScompress_runlength\n");
			MOScompress_runlength(task);
			MOSupdateHeader(task);
			MOSadvance_runlength(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_CAPPED:
			ALGODEBUG mnstr_printf(GDKout, "MOScompress_capped\n");
			MOScompress_capped(task);
			MOSupdateHeader(task);
			MOSadvance_capped(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKout, "MOScompress_delta\n");
			MOScompress_delta(task);
			MOSupdateHeader(task);
			MOSadvance_delta(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKout, "MOScompress_linear\n");
			MOScompress_linear(task);
			MOSupdateHeader(task);
			MOSadvance_linear(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKout, "MOScompress_frame\n");
			MOScompress_frame(task);
			MOSupdateHeader(task);
			MOSadvance_frame(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKout, "MOScompress_prefix\n");
			MOScompress_prefix(task);
			MOSupdateHeader(task);
			MOSadvance_prefix(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKout, "MOScompress_raw\n");
			MOScompress_raw( task, estimate);
			MOSupdateHeader(task);
			MOSadvance_raw(task);
			MOSnewBlk(task);
			break;
		default : // Unknown block type. Should not happen.
			assert(0);
		}
	}

	task->bsrc->tmosaic->free = (task->dst - (char*)task->hdr);
	task->timer = GDKusec() - task->timer;

	// TODO: if we couldnt compress well enough, ignore the result

	bsrc->batDirtydesc = true;
	task->hdr->ratio = (flt)task->bsrc->theap.free/ task->bsrc->tmosaic->free;
finalize:
	GDKfree(task);

	t1 = GDKusec();
	ALGODEBUG mnstr_printf(GDKout, "#BATmosaic: mosaic construction " LLFMT " usec\n", t1 - t0);

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

// recreate the uncompressed heap from its mosaic version
static str
MOSdecompressInternal(BAT** res, BAT* bsrc)
{	
	MOStask task;
	int error;

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

 	// TODO: We should also compress the string heap itself somehow.
	// For now we just share the string heap of the original compressed bat.
	if (bsrc->tvheap) {
		BBPshare(bsrc->tvheap->parentid);
		(*res)->tvheap = bsrc->tvheap;
	}

	(*res)->tunique = bsrc->tunique;
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

	while(task->blk){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKout, "MOSdecompress_raw\n");
			MOSdecompress_raw(task);
			MOSskip_raw(task);
			break;
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKout, "MOSdecompress_runlength\n");
			MOSdecompress_runlength(task);
			MOSskip_runlength(task);
			break;
		case MOSAIC_CAPPED:
			ALGODEBUG mnstr_printf(GDKout, "MOSdecompress_capped\n");
			MOSdecompress_capped(task);
			MOSskip_capped(task);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKout, "MOSdecompress_delta\n");
			MOSdecompress_delta(task);
			MOSskip_delta(task);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKout, "MOSdecompress_linear\n");
			MOSdecompress_linear(task);
			MOSskip_linear(task);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKout, "MOSdecompress_frame\n");
			MOSdecompress_frame(task);
			MOSskip_frame(task);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKout, "MOSdecompress_prefix\n");
			MOSdecompress_prefix(task);
			MOSskip_prefix(task);
			break;
		default: assert(0);
		}
	}

	error = 0;
	switch( ATOMbasetype(task->type)){
	case TYPE_bte:
		error = task->hdr->checksum.sumbte != task->hdr->checksum2.sumbte;
		break;
	case TYPE_sht:
		error = task->hdr->checksum.sumsht != task->hdr->checksum2.sumsht;
		break;
	case TYPE_int:
		error = task->hdr->checksum.sumint != task->hdr->checksum2.sumint;
		break;
	case TYPE_lng:
		error = task->hdr->checksum.sumlng != task->hdr->checksum2.sumlng;
		break;
	case TYPE_flt:
		error = task->hdr->checksum.sumflt != task->hdr->checksum2.sumflt;
		break;
	case TYPE_dbl:
		error = task->hdr->checksum.sumdbl != task->hdr->checksum2.sumdbl;
		break;
	}
	if(error) {
		// TODO: handle error
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

/* The algebra operators should fall back to their default
 * when we know that the heap is not compressed
 * The actual decompression should wait until we know that
 * the administration thru SQL layers works properly.
 *
 * The oid-range can be reduced due to partitioning.
 */
str
MOSselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *li, *hi, *anti;
	void *low, *hgh;
	bat *ret, *bid, *cid= 0;
	int i;
	BUN cnt = 0;
	BAT *b, *bn, *cand = NULL;
	str msg = MAL_SUCCEED;
	MOStask task;
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

	// determine block range to scan for partitioned columns
	/*
	** TODO: Figure out how do partitions relate to mosaic chunks.
	** Is it a good idea to set the capacity to the total size of the select operand b?
	*/

	while(task->start < task->stop ){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKout, "MOSselect_runlength\n");
			MOSselect_runlength(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_CAPPED:
			ALGODEBUG mnstr_printf(GDKout, "MOSselect_capped\n");
			MOSselect_capped(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKout, "MOSselect_frame\n");
			MOSselect_frame(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKout, "MOSselect_delta\n");
			MOSselect_delta(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKout, "MOSselect_prefix\n");
			MOSselect_prefix(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKout, "MOSselect_linear\n");
			MOSselect_linear(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKout, "MOSselect_raw\n");
			MOSselect_raw(task,low,hgh,li,hi,anti);
		}
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

	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	GDKfree(task);
	BBPkeepref(bn->batCacheid);
	return msg;
}

str MOSthetaselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx;
	bat *cid =0,  *ret, *bid;
	BAT *b = 0, *cand = 0, *bn = NULL;
	BUN cnt=0;
	str msg= MAL_SUCCEED;
	char **oper;
	void *low;
	MOStask task;

	(void) cntxt;
	(void) mb;
		ret= getArgReference_bat(stk,pci,0);
	bid= getArgReference_bat(stk,pci,1);
	if( pci->argc == 5){ // candidate list included
		cid = getArgReference_bat(stk,pci, 2);
		idx = 3;
	} else idx = 2;
	low= (void*) getArgReference(stk,pci,idx);
	oper= getArgReference_str(stk,pci,idx+1);

	if( !isCompressed(*bid)){
		if( cid)
			return ALGthetaselect2(ret,bid,cid,low, (const char **)oper);
		else
			return ALGthetaselect1(ret,bid,low, (const char **)oper);
	}
	
	b = BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "mosaic.thetaselect", RUNTIME_OBJECT_MISSING);
	// determine the elements in the compressed structure

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.thetaselect", RUNTIME_OBJECT_MISSING);
	}

	// accumulator for the oids
	bn = COLnew((oid)0, TYPE_oid, BATcount(b), TRANSIENT);
	if( bn == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.thetaselect", RUNTIME_OBJECT_MISSING);
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
			throw(MAL, "mosaic.thetaselect", RUNTIME_OBJECT_MISSING);
		}
		task->cl = (oid*) Tloc(cand, 0);
		task->n = BATcount(cand);
	}

	while(task->start < task->stop ){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKout, "MOSthetaselect_runlength\n");
			MOSthetaselect_runlength(task,low,*oper);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKout, "MOSthetaselect_delta\n");
			MOSthetaselect_delta(task,low,*oper);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKout, "MOSthetaselect_prefix\n");
			MOSthetaselect_prefix(task,low,*oper);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKout, "MOSthetaselect_linear\n");
			MOSthetaselect_linear(task,low,*oper);
			break;
		case MOSAIC_CAPPED:
			ALGODEBUG mnstr_printf(GDKout, "MOSthetaselect_capped\n");
			MOSthetaselect_capped(task,low,*oper);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKout, "MOSthetaselect_frame\n");
			MOSthetaselect_frame(task,low,*oper);
			break;
		case MOSAIC_RAW:
		default:
			ALGODEBUG mnstr_printf(GDKout, "MOSthetaselect_raw\n");
			MOSthetaselect_raw(task,low,*oper);
		}
	}
	// derive the filling
	cnt = (BUN)( task->lb - (oid*) Tloc(bn,0));
	
	if( cid)
		BBPunfix(*cid);
	if( bn){
		BATsetcount(bn,cnt);
		bn->tnil = false;
		bn->tnonil = true;
		bn->tsorted = true;
		bn->trevsorted = cnt <=1;
		bn->tkey = true;
		BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
	}
	GDKfree(task);
	return msg;
}

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

	if (BATtdense(bl) || /*not a candidate list*/ !(bl->tkey && bl->tsorted && bl->tnonil)) {

		msg = ALGprojection(ret, lid, rid);
		BBPunfix(*lid);
		BBPunfix(*rid);

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

	// loop thru all the chunks and fetch all results
	while(task->start<task->stop )
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKout, "MOSprojection_runlength\n");
			MOSprojection_runlength( task);
			break;
		case MOSAIC_CAPPED:
			ALGODEBUG mnstr_printf(GDKout, "MOSprojection_capped\n");
			MOSprojection_capped( task);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKout, "MOSprojection_frame\n");
			MOSprojection_frame( task);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKout, "MOSprojection_delta\n");
			MOSprojection_delta( task);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKout, "MOSprojection_prefix\n");
			MOSprojection_prefix( task);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKout, "MOSprojection_linear\n");
			MOSprojection_linear( task);
			break;
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKout, "MOSprojection_raw\n");
			MOSprojection_raw( task);
			break;
		default:
			assert(0);
		}

	/* adminstrative wrapup of the projection */
	BBPunfix(*lid);
	BBPunfix(*rid);

	BATsetcount(bn,task->cnt);
	bn->tnil = 0;
	bn->tnonil = 1;
	bn->tsorted = bn->trevsorted = cnt <= 1;
	BBPkeepref(*ret = bn->batCacheid);
	GDKfree(task);
	return msg;
}

/* A mosaic join operator that works when either the left or the right side is compressed
 * and there are no additional candidate lists.
 * Furthermore if both sides are in possesion of a mosaic index,
 * the operator implementation currently only uses the mosaic index of the left side.
 */
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
	if( (!isCompressed(*lid) && !isCompressed(*rid)) || (*sl != bat_nil || *sr != bat_nil)) 
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
	if ( bl->tmosaic){
		MOSinit(task,bl);
		MOSinitializeScan(task, bl);
	} else {
		MOSinit(task,br);
		MOSinitializeScan(task, br);
		swapped=1;
	}
	task->lbat = bln;
	task->rbat = brn;

	if ( bl->tmosaic){
		task->stop = BATcount(br);
		task->src= Tloc(br,0);
	} else {
		task->stop = BATcount(bl);
		task->src= Tloc(bl,0);
	}
	// loop thru all the chunks and collect the results
	while(task->blk )
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			ALGODEBUG mnstr_printf(GDKout, "MOSjoin_runlength\n");
			MOSjoin_runlength( task);
			break;
		case MOSAIC_CAPPED:
			ALGODEBUG mnstr_printf(GDKout, "MOSjoin_capped\n");
			MOSjoin_capped( task);
			break;
		case MOSAIC_FRAME:
			ALGODEBUG mnstr_printf(GDKout, "MOSjoin_frame\n");
			MOSjoin_frame( task);
			break;
		case MOSAIC_DELTA:
			ALGODEBUG mnstr_printf(GDKout, "MOSjoin_delta\n");
			MOSjoin_delta( task);
			break;
		case MOSAIC_PREFIX:
			ALGODEBUG mnstr_printf(GDKout, "MOSjoin_prefix\n");
			MOSjoin_prefix( task);
			break;
		case MOSAIC_LINEAR:
			ALGODEBUG mnstr_printf(GDKout, "MOSjoin_linear\n");
			MOSjoin_linear( task);
			break;
		case MOSAIC_RAW:
			ALGODEBUG mnstr_printf(GDKout, "MOSjoin_raw\n");
			MOSjoin_raw( task);
			break;
		default:
			assert(0);
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

// The analyse routine runs through the BAT capped and assess
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
		mnstr_printf(GDKout,"#");
		for(j=0, bit=1; j < MOSAIC_METHODS-1; j++){
			mnstr_printf(GDKout,"%d", (patterns[k] & bit) > 0);
			bit *=2;
		}
		mnstr_printf(GDKout,"\n");
#else
		(void) bit;
#endif
		k++;
	}
#ifdef _MOSAIC_DEBUG_
	mnstr_printf(GDKout,"lim %d k %d\n",lim,k);
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
