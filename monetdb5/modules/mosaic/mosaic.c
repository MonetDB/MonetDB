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
#include "mosaic_dictionary.h"
#include "mosaic_delta.h"
#include "mosaic_linear.h"
#include "mosaic_frame.h"
#include "mosaic_prefix.h"
#include "mosaic_calendar.h"

char *MOSfiltername[]={"raw","runlength","dictionary","delta","linear","frame","prefix","calendar","EOL"};

bool MOSisTypeAllowed(int compression, BAT* b) {
	switch (compression) {
	case MOSAIC_RAW:		return MOStypes_raw(b);
	case MOSAIC_RLE:		return MOStypes_runlength(b);
	case MOSAIC_DICT:		return MOStypes_dictionary(b);
	case MOSAIC_DELTA:		return MOStypes_delta(b);
	case MOSAIC_LINEAR:		return MOStypes_runlength(b);
	case MOSAIC_FRAME:		return MOStypes_frame(b);
	case MOSAIC_PREFIX:		return MOStypes_prefix(b);
	case MOSAIC_CALENDAR:	return MOStypes_calendar(b);
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
	if( task->hdr->blks[MOSAIC_DICT])
		MOSlayout_dictionary_hdr(task,btech,bcount,binput,boutput,bproperties);
	if( task->hdr->blks[MOSAIC_CALENDAR])
		MOSlayout_calendar(task,btech,bcount,binput,boutput,bproperties);

	if( BUNappend(btech, "========", false) != GDK_SUCCEED ||
		BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
		BUNappend(binput, &zero, false) != GDK_SUCCEED ||
		BUNappend(boutput, &zero , false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
			throw(MAL,"mosaic.layout", MAL_MALLOC_FAIL);

	while(task->start< task->stop){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RAW:
			MOSlayout_raw(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_raw(task);
			break;
		case MOSAIC_RLE:
			MOSlayout_runlength(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_runlength(task);
			break;
		case MOSAIC_DICT:
			MOSlayout_dictionary(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_dictionary(task);
			break;
		case MOSAIC_DELTA:
			MOSlayout_delta(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_delta(task);
			break;
		case MOSAIC_LINEAR:
			MOSlayout_linear(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_linear(task);
			break;
		case MOSAIC_FRAME:
			MOSlayout_frame(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_frame(task);
			break;
		case MOSAIC_PREFIX:
			MOSlayout_prefix(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_prefix(task);
			break;
		case MOSAIC_CALENDAR:
			MOSlayout_calendar(task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_calendar(task);
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

/* The compression orchestration is dealt with here.
 * We assume that the estimates for each scheme returns
 * the number of elements it applies to. Moreover, we
 * assume that the compression factor holds for any subsequence.
 * This allows us to avoid expensive estimate calls when a small
 * sequence is found with high compression factor.
 */
static int
MOSoptimizerCost(MOStask task, int typewidth)
{
	int cand = MOSAIC_RAW;
	float ratio = 1.0, fac = 1.0;

	// select candidate amongst those
	if ( task->filter[MOSAIC_RLE]){
		fac = MOSestimate_runlength(task);
		if (fac > ratio){
			cand = MOSAIC_RLE;
			ratio = fac;
		}
	}
	if ( task->filter[MOSAIC_LINEAR]){
		fac = MOSestimate_linear(task);
		if ( fac >ratio){
			cand = MOSAIC_LINEAR;
			ratio = fac;
		}
	}
	if (ratio < typewidth && task->filter[MOSAIC_PREFIX]){
		fac = MOSestimate_prefix(task);
		if ( fac > ratio ){
			cand = MOSAIC_PREFIX;
			ratio = fac;
		}
	}
	if (ratio < 64 && task->filter[MOSAIC_DICT]){
		fac = MOSestimate_dictionary(task);
		if (fac > ratio){
			cand = MOSAIC_DICT;
			ratio = fac;
		}
	}
	if (ratio < 64 && task->filter[MOSAIC_FRAME]){
		fac = MOSestimate_frame(task);
		if (fac > ratio){
			cand = MOSAIC_FRAME;
			ratio = fac;
		}
	}
	if (ratio < 64 && task->filter[MOSAIC_DELTA]){
		fac = MOSestimate_delta(task);
		if ( fac > ratio ){
			cand = MOSAIC_DELTA;
			ratio = fac;
		}
	}
	if (ratio < 64 && task->filter[MOSAIC_CALENDAR]){
		fac = MOSestimate_calendar(task);
		if (fac > ratio){
			cand = MOSAIC_CALENDAR;
			ratio = fac;
		}
	}
	return cand;
}

/* the source is extended with a BAT mosaic heap */
str
MOScompressInternal(BAT* bsrc, const char* compressions)
{
	MOStask task;
	str msg = MAL_SUCCEED;
	int cand;
	int typewidth;
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
		throw(MAL, "mosaic.compress", MAL_MALLOC_FAIL);
	}
	
	// initialize the non-compressed read pointer
	task->src = Tloc(bsrc, 0);
	task->stop = BATcount(bsrc);
	task->start = 0;
	task->stop = BATcount(bsrc);
	task->timer = GDKusec();

	MOSinit(task,bsrc);
	task->blk->cnt= 0;
	MOSinitHeader(task);
	if (MOSinitializeFilter(task, compressions)) {
		MOSdestroy(bsrc);
		msg = createException(MAL, "mosaic.compress", "No valid compression technique given or available for type: %s", ATOMname(task->type));
		goto finalize;
	}

	typewidth = ATOMsize(task->type) * CHAR_BIT;

	if( task->filter[MOSAIC_DICT])
		MOScreatedictionary(task);
	if( task->filter[MOSAIC_CALENDAR])
		MOScreatecalendar(task);
	// always start with an EOL block
	MOSsetTag(task->blk,MOSAIC_EOL);

	while(task->start < task->stop ){
		// default is to extend the non-compressed block with a single element
		cand = MOSoptimizerCost( task, typewidth);
		if( task->dst >= bsrc->tmosaic->base + bsrc->tmosaic->size - 2 * MosaicBlkSize ){
			MOSdestroy(bsrc);
			msg= createException(MAL,"mosaic.compress","abort compression due to size");
			goto finalize;
		}
		assert (task->dst < bsrc->tmosaic->base + bsrc->tmosaic->size );

		if ( MOSgetTag(task->blk) == MOSAIC_RAW) {
			if( cand != MOSAIC_RAW ||  MOSgetCnt(task->blk) +1 == MOSAICMAXCNT) {
				// We close the old MOSAIC_RAW block if estimation decides to use a different block type
				// or when the current MOSAIC_RAW block has become too big.
				task->start -= MOSgetCnt(task->blk);
				MOSupdateHeader(task);
				MOSadvance_raw(task);
				// always start with an EOL block
				task->dst = MOScodevector(task);
				MOSsetTag(task->blk,MOSAIC_EOL);
				MOSsetCnt(task->blk,0);
			}
		}

		// apply the compression to a chunk
		switch(cand){
		case MOSAIC_RLE:
			MOScompress_runlength(task);
			MOSupdateHeader(task);
			MOSadvance_runlength(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_DICT:
			MOScompress_dictionary(task);
			MOSupdateHeader(task);
			MOSadvance_dictionary(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_DELTA:
			MOScompress_delta(task);
			MOSupdateHeader(task);
			MOSadvance_delta(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_LINEAR:
			MOScompress_linear(task);
			MOSupdateHeader(task);
			MOSadvance_linear(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_FRAME:
			MOScompress_frame(task);
			MOSupdateHeader(task);
			MOSadvance_frame(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_PREFIX:
			MOScompress_prefix(task);
			MOSupdateHeader(task);
			MOSadvance_prefix(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_CALENDAR:
			MOScompress_calendar(task);
			MOSupdateHeader(task);
			MOSadvance_calendar(task);
			MOSnewBlk(task);
			break;
		case MOSAIC_RAW: // This is basically the default case.
			/* This tries to insert the single value at task->start into this MOSAIC_RAW block.
			 * After that compressions tries to re-evaluate through MOSoptimizerCost
			 * from ++task->start and unwards to estimate a more efficient block type.
			*/
			MOScompress_raw( task);
				task->start++;
			break;
		default : // Unknown block type. Should not happen.
			assert(0);
		}
	}

	if( MOSgetTag(task->blk) == MOSAIC_RAW ) {
		MOSupdateHeader(task);
		MOSadvance_raw(task);
		MOSnewBlk(task);
	}

	task->bsrc->tmosaic->free = (task->dst - (char*)task->hdr);
	task->timer = GDKusec() - task->timer;

	// TODO: if we couldnt compress well enough, ignore the result

	bsrc->batDirtydesc = true;
	task->hdr->ratio = (flt)task->bsrc->theap.free/ task->bsrc->tmosaic->free;
finalize:
	GDKfree(task);

	t1 = GDKusec();
	ALGODEBUG fprintf(stderr, "#BATmosaic: mosaic construction " LLFMT " usec\n", t1 - t0);

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
			MOSdecompress_raw(task);
			MOSskip_raw(task);
			break;
		case MOSAIC_RLE:
			MOSdecompress_runlength(task);
			MOSskip_runlength(task);
			break;
		case MOSAIC_DICT:
			MOSdecompress_dictionary(task);
			MOSskip_dictionary(task);
			break;
		case MOSAIC_DELTA:
			MOSdecompress_delta(task);
			MOSskip_delta(task);
			break;
		case MOSAIC_LINEAR:
			MOSdecompress_linear(task);
			MOSskip_linear(task);
			break;
		case MOSAIC_FRAME:
			MOSdecompress_frame(task);
			MOSskip_frame(task);
			break;
		case MOSAIC_PREFIX:
			MOSdecompress_prefix(task);
			MOSskip_prefix(task);
			break;
		case MOSAIC_CALENDAR:
			MOSdecompress_calendar(task);
			MOSskip_calendar(task);
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
			MOSselect_runlength(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DICT:
			MOSselect_dictionary(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_FRAME:
			MOSselect_frame(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DELTA:
			MOSselect_delta(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_PREFIX:
			MOSselect_prefix(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_LINEAR:
			MOSselect_linear(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_CALENDAR:
			MOSselect_calendar(task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_RAW:
		default:
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
			MOSthetaselect_runlength(task,low,*oper);
			break;
		case MOSAIC_DELTA:
			MOSthetaselect_delta(task,low,*oper);
			break;
		case MOSAIC_PREFIX:
			MOSthetaselect_prefix(task,low,*oper);
			break;
		case MOSAIC_LINEAR:
			MOSthetaselect_linear(task,low,*oper);
			break;
		case MOSAIC_DICT:
			MOSthetaselect_dictionary(task,low,*oper);
			break;
		case MOSAIC_FRAME:
			MOSthetaselect_frame(task,low,*oper);
			break;
		case MOSAIC_CALENDAR:
			MOSthetaselect_calendar(task,low,*oper);
			break;
		case MOSAIC_RAW:
		default:
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
			MOSprojection_runlength( task);
			break;
		case MOSAIC_DICT:
			MOSprojection_dictionary( task);
			break;
		case MOSAIC_FRAME:
			MOSprojection_frame( task);
			break;
		case MOSAIC_DELTA:
			MOSprojection_delta( task);
			break;
		case MOSAIC_PREFIX:
			MOSprojection_prefix( task);
			break;
		case MOSAIC_LINEAR:
			MOSprojection_linear( task);
			break;
		case MOSAIC_CALENDAR:
			MOSprojection_calendar( task);
			break;
		case MOSAIC_RAW:
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
			MOSjoin_runlength( task);
			break;
		case MOSAIC_DICT:
			MOSjoin_dictionary( task);
			break;
		case MOSAIC_FRAME:
			MOSjoin_frame( task);
			break;
		case MOSAIC_DELTA:
			MOSjoin_delta( task);
			break;
		case MOSAIC_PREFIX:
			MOSjoin_prefix( task);
			break;
		case MOSAIC_LINEAR:
			MOSjoin_linear( task);
			break;
		case MOSAIC_CALENDAR:
			MOSjoin_calendar( task);
			break;
		case MOSAIC_RAW:
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
