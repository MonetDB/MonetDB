/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * authors Martin Kersten, A. Koning
 * Adaptive compression scheme to reduce the storage footprint for stable persistent data.
 * The permissible compression MOSmethods can be controlled thru an argument list
*/

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_hdr.h"
#include "mosaic_raw.h"
#include "mosaic_runlength.h"
#include "mosaic_dict256.h"
#include "mosaic_dict.h"
#include "mosaic_delta.h"
#include "mosaic_linear.h"
#include "mosaic_frame.h"
#include "mosaic_prefix.h"

#define DEFINE_METHOD(METHOD) \
{\
	.bit	= (1 << METHOD),\
	.name	= #METHOD\
}

const Method MOSmethods[] = {
	DEFINE_METHOD(raw),
	DEFINE_METHOD(runlength),
	DEFINE_METHOD(dict256),
	DEFINE_METHOD(dict),
	DEFINE_METHOD(delta),
	DEFINE_METHOD(linear),
	DEFINE_METHOD(frame),
	DEFINE_METHOD(prefix)
};

#define METHOD_IS_SET(FILTER, IDX)	( (FILTER) & MOSmethods[IDX].bit )
#define SET_METHOD(FILTER, IDX) 	( (FILTER) |= MOSmethods[IDX].bit )
#define UNSET_METHOD(FILTER, IDX) 	( (FILTER) &= ~MOSmethods[IDX].bit )

bit MOSisTypeAllowed(char compression, BAT* b) {
	switch (compression) {
	case raw:		return MOStypes_raw(b);
	case runlength:	return MOStypes_runlength(b);
	case dict256:	return MOStypes_dict256(b);
	case dict:		return MOStypes_dict(b);
	case delta:		return MOStypes_delta(b);
	case linear:	return MOStypes_linear(b);
	case frame:		return MOStypes_frame(b);
	case prefix:	return MOStypes_prefix(b);
	default: /* should not happen*/ assert(0);
	}

	return false;
}

static void
_construct_compression_mask(sht* compression_mask, char* compressions) {
	if (GDK_STRNIL(compressions)) {
		*compression_mask = ~0;
		return;
	}

	*compression_mask = 0;

	char* _dict256;
	/* The dict256 dictionary technique 'dict256' has to be processed upfront
	 * to prevent search collision with the variable dictionary technique 'dict'.
	 */
	const char* erased = "_______";
	while ( (_dict256 = strstr(compressions, MOSmethods[dict256].name)) ) {
		memcpy (_dict256, erased, strlen(erased));

		*compression_mask |= MOSmethods[dict256].bit;
	}

	for(unsigned i = 0; i< MOSAIC_METHODS; i++) {
		if ( strstr(compressions, MOSmethods[i].name) ) {
			*compression_mask |= MOSmethods[i].bit;
		}
	}
}

static str
construct_compression_mask(MOStask* task, const char* compressions) {

	char* copy = NULL;
	if (compressions) {
		copy = GDKzalloc(strlen(compressions)+1);
		strcpy(copy, compressions);
		if (copy == NULL) {
			throw(MAL, "mosaic.construct_compression_mask", MAL_MALLOC_FAIL);
		}
	}

	sht compression_mask;
	_construct_compression_mask(&compression_mask, copy);

	if (copy) {
		GDKfree(copy);
	}

	task->mask = compression_mask;

	bool type_is_allowed = false;

	for(unsigned i = 0; i< MOSAIC_METHODS; i++) {
		if ( METHOD_IS_SET(task->mask, i) && MOSisTypeAllowed(i, task->bsrc) ) {
			task->hdr->elms[i] = task->hdr->blks[i] = 0;
			type_is_allowed = true;
		}
	}

	if (!type_is_allowed) {
		throw(MAL, "mosaic.construct_compression_mask", "No valid compression technique given or available for type: %s", ATOMname(task->type));
	}

	return MAL_SUCCEED;
}

static void
MOSinit(MOStask* task, BAT *b) {
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
	task->dict256_info = NULL;
	task->dict_info = NULL;
	task->padding = NULL;
}

/*
 * Compression is focussed on a single column.
 * Multiple compression MOSmethods are applied at the same time.
 */

#define MOSnewBlk(TASK)\
			MOSsetCnt(TASK->blk,0);\
			TASK->dst = MOScodevector(TASK);

static inline BUN get_normalized_compression(MosaicEstimation* current, const MosaicEstimation* previous) {
	(void) previous;
	BUN old = current->previous_compressed_size;
	BUN new = current->compressed_size;
	BUN cnt = current->compression_strategy.cnt;
	BUN normalized_cnt = *current->max_compression_length ? *current->max_compression_length: 100;
	assert (new >= old);
	return (((new - old) * normalized_cnt) / cnt);
}

#define getFactor(ESTIMATION) ((flt) (ESTIMATION).uncompressed_size / (ESTIMATION).compressed_size)


static str
MOSprepareDictionaryContext(MOStask* task) {
	str error;
	if (METHOD_IS_SET(task->mask, MOSAIC_DICT256)){
		if ( (error = MOSprepareDictionaryContext_ID(dict256)(task))) {
			return error;
		}
	}
	if (METHOD_IS_SET(task->mask, MOSAIC_DICT)){
		if ( (error = MOSprepareDictionaryContext_ID(dict)(task))) {
			return error;
		}
	}
	return MAL_SUCCEED;
}

#define TPE bte
#include "mosaic_template.h"
#undef TPE
#define TPE sht
#include "mosaic_template.h"
#undef TPE
#define TPE int
#include "mosaic_template.h"
#undef TPE
#define TPE lng
#include "mosaic_template.h"
#undef TPE
#define TPE flt
#include "mosaic_template.h"
#undef TPE
#define TPE dbl
#include "mosaic_template.h"
#undef TPE
#ifdef HAVE_HGE
#define TPE hge
#include "mosaic_template.h"
#undef TPE
#endif

str
MOSlayout(BAT *b, BAT *bbsn, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties) {
	str msg = MAL_SUCCEED;

	if( b->tmosaic == NULL)
		throw(MAL,"mosaic.layout","Compression heap missing");

	MOStask task = {0};
	MOSinit(&task,b);
	MOSinitializeScan(&task, b);

	MosaicLayout layout = {
		.bsn		= bbsn,
		.tech		= btech,
		.count		= bcount,
		.input		= binput,
		.output		= boutput,
		.properties = bproperties
	};

	if ((msg = MOSlayout_hdr(&task, &layout)) != MAL_SUCCEED) {
		return msg;
	}

	switch(ATOMbasetype(task.type)){
	case TYPE_bte: return MOSlayout_bte(&task, &layout);
	case TYPE_sht: return MOSlayout_sht(&task, &layout);
	case TYPE_int: return MOSlayout_int(&task, &layout);
	case TYPE_lng: return MOSlayout_lng(&task, &layout);
	case TYPE_flt: return MOSlayout_flt(&task, &layout);
	case TYPE_dbl: return MOSlayout_dbl(&task, &layout);
#ifdef HAVE_HGE
	case TYPE_hge: return MOSlayout_hge(&task, &layout);
#endif
	default:
		// Unknown type. Should not happen.
		assert(0);
	}

	return MAL_SUCCEED;
}


/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 *  authors Martin Kersten, Aris Koning
 * The header block contains the mapping from OIDs to chunks, which should become
 * the basis for processing mitosis over a mosaic file.
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_hdr.h"
#include "mosaic_utility.h"

// add the chunk to the index to facilitate 'fast' OID-based access
void
MOSupdateHeader(MOStask* task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;

    hdr->blks[MOSgetTag(task->blk)]++;
    hdr->elms[MOSgetTag(task->blk)] += MOSgetCnt(task->blk);

	if( hdr->top < METHOD_NOT_AVAILABLE -1 ){
		if( hdr->top == 0){
			hdr->top++;
		}
		hdr->top++;
	}
}

void
MOSinitHeader(MOStask* task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	int i;
	for(i=0; i < MOSAIC_METHODS; i++){
		hdr->elms[i] = hdr->blks[i] = METHOD_NOT_AVAILABLE;
	}
	hdr->ratio = 0;
	hdr->version = MOSAIC_VERSION;
	hdr->top = 0;
	hdr->bits_dict = 0;
	hdr->pos_dict = 0;
	hdr->length_dict = 0;
	hdr->bits_dict256 = 0;
	hdr->pos_dict256 = 0;
	hdr->length_dict256 = 0;

	task->bsrc->tmosaic->free = MosaicHdrSize;
}

// position the task on the mosaic blk to be scanned
void
MOSinitializeScan(MOStask* task, BAT* /*compressed*/ b)
{
	task->blk = (MosaicBlk) (((char*)task->hdr) + MosaicHdrSize);

	task->start = 0;
	task->stop = b->batCount;
}

#define LAYOUT_BUFFER_SIZE 10000
str
MOSlayout_hdr(MOStask* task, MosaicLayout* layout) {
	size_t written;

	char buffer1[LAYOUT_BUFFER_SIZE] = {0};
	char buffer2[LAYOUT_BUFFER_SIZE] = {0};

	char* pbuffer2 = &buffer2[0];

	size_t buffer_size = LAYOUT_BUFFER_SIZE;

	strcpy(buffer1, "{");

	strcat(buffer1, "\"blks\":[");
	for(int j=0; j < MOSAIC_METHODS; j++) {
		if (task->hdr->blks[j] > 0) {

			written = lngToStr(&pbuffer2, &buffer_size, &(task->hdr->blks[j]), true);

			if (buffer1[strlen(buffer1)] == '[') {
				strcat(buffer1, ",");
			}

			strcat(buffer1, "{\"");
			strcat(buffer1, MOSmethods[j].name);
			strcat(buffer1, "\":");
			strcat(buffer1, buffer2);
			memset(buffer2, 0, written);
			strcat(buffer1, "}");
		}
	}
	strcat(buffer1, "]");

	strcat(buffer1, ",");

	strcat(buffer1, "\"elms\":[");
	for(int j=0; j < MOSAIC_METHODS; j++) {
		if (task->hdr->blks[j] > 0) {

			written = lngToStr(&pbuffer2, &buffer_size, &(task->hdr->elms[j]), true);

			if (buffer1[strlen(buffer1)] == '[') {
				strcat(buffer1, ",");
			}

			strcat(buffer1, "{\"");
			strcat(buffer1, MOSmethods[j].name);
			strcat(buffer1, "\":");
			strcat(buffer1, buffer2);
			memset(buffer2, 0, written);
			strcat(buffer1, "}");
		}
	}
	strcat(buffer1, "]");

	written = fltToStr(&pbuffer2, &buffer_size, &((task->hdr)->ratio), true);

	strcat(buffer1, ",\"ratio\":");

	strcat(buffer1, buffer2);
	memset(buffer2, 0, written);

	strcat(buffer1, "}");

	LAYOUT_INSERT(
		tech = "header";
		properties = buffer1;
		 /*TODO: These parameters might be problematic for large datasets.*/
		count = BATcount(task->bsrc);
		input = count * task->bsrc->twidth * CHAR_BIT;
		output = task->bsrc->tmosaic->free;
	);
	str msg;
	if( (task->hdr)->blks[MOSAIC_DICT256] > 0 && (msg = MOSlayoutDictionary_ID(dict256)(task,layout, 0)) != MAL_SUCCEED)
		return msg;

	if( (task->hdr)->blks[MOSAIC_DICT] > 0 && (msg = MOSlayoutDictionary_ID(dict)(task,layout, 0)) != MAL_SUCCEED)
		return msg;

	return MAL_SUCCEED;
}
#undef LAYOUT_BUFFER_SIZE

static
str MOSestimate(MOStask* task, BAT* estimates, size_t* compressed_size) {
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

#define finalize_dictionary(TPE)\
{\
	str error;\
	if (METHOD_IS_SET(task->mask, MOSAIC_DICT)) {\
		if ((error = MOSfinalizeDictionary_ID(dict, TPE)(task))) {\
			return error;\
		}\
	}\
	if (METHOD_IS_SET(task->mask, MOSAIC_DICT256)) {\
		if ((error = MOSfinalizeDictionary_ID(dict256, TPE)(task))) {\
			return error;\
		}\
	}\
\
	return MAL_SUCCEED;\
}

static str
MOSfinalizeDictionary(MOStask* task) {

	task->bsrc->tvmosaic->free = 0;

	switch(ATOMbasetype(task->type)) {
	case TYPE_bte: finalize_dictionary(bte);
	case TYPE_sht: finalize_dictionary(sht);
	case TYPE_int: finalize_dictionary(int);
	case TYPE_lng: finalize_dictionary(lng);
	case TYPE_flt: finalize_dictionary(flt);
	case TYPE_dbl: finalize_dictionary(dbl);
#ifdef HAVE_HGE
	case TYPE_hge: finalize_dictionary(hge);
#endif
	default:
		// Unknown block type. Should not happen.
		assert(0);
	}

	throw(MAL, "mosaic.estimate", TYPE_NOT_SUPPORTED);
}

/* the source is extended with a BAT mosaic heap */
str
MOScompressInternal(BAT* bsrc, const char* compressions)
{
	MOStask task = {0};
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

	// initialize the non-compressed read pointer
	task.src = Tloc(bsrc, 0);
	task.start = 0;
	task.stop = BATcount(bsrc);
	task.timer = GDKusec();

	MOSinit(&task,bsrc);
	task.blk->cnt= 0;
	MOSinitHeader(&task);

	if ( (msg = construct_compression_mask(&task, compressions)) != MAL_SUCCEED) {
			MOSdestroy(bsrc);
			goto finalize;
	}

	// Zero pass: estimation preparation phase
	if ((msg = MOSprepareDictionaryContext(&task))) {
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
	if ( ( msg = MOSestimate(&task, estimates, &compressed_size_bytes) )) {
		BBPreclaim(estimates);
		MOSdestroy(bsrc);
		goto finalize;
	}

	if ((msg = MOSfinalizeDictionary(&task))) {
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

	MOSinit(&task, bsrc);

	task.start = 0;

	switch(ATOMbasetype(task.type)){
	case TYPE_bte: MOScompressInternal_bte(&task, estimates); break;
	case TYPE_sht: MOScompressInternal_sht(&task, estimates); break;
	case TYPE_int: MOScompressInternal_int(&task, estimates); break;
	case TYPE_lng: MOScompressInternal_lng(&task, estimates); break;
	case TYPE_flt: MOScompressInternal_flt(&task, estimates); break;
	case TYPE_dbl: MOScompressInternal_dbl(&task, estimates); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOScompressInternal_hge(&task, estimates); break;
#endif
	default: // Unknown block type. Should not happen.
		assert(0);
	}

	task.bsrc->tmosaic->free = (task.dst - (char*)task.hdr);
	task.timer = GDKusec() - task.timer;

	// TODO: if we couldnt compress well enough, ignore the result

	bsrc->batDirtydesc = true;
	task.hdr->ratio =
		(flt)task.bsrc->theap.free /
		(task.bsrc->tmosaic->free + (task.bsrc->tvmosaic? task.bsrc->tvmosaic->free : 0));
	BBPreclaim(estimates);
finalize:

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

	char* compressions = NULL;

	if( pci->argc == 3) {
		compressions = *getArgReference_str(stk,pci,2);
	}

	MOSsetLock(b);
	msg= MOScompressInternal(b, compressions);
	MOSunsetLock(b);

	BBPunfix(*ret = b->batCacheid);
	return msg;
}

// recreate the uncompressed heap from its mosaic version
static str
MOSdecompressInternal(BAT** res, BAT* bsrc)
{	
	MOStask task = {0};

	if (BATcheckmosaic(bsrc) == 0 ){
		*res = bsrc;
		BBPfix(bsrc->batCacheid); // We are just returning a reference to bsrc.
		return MAL_SUCCEED;
	}

	if (VIEWtparent(bsrc)) {
		throw(MAL, "mosaic.decompress", "cannot decompress tail-VIEW");
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

	MOSinit(&task,bsrc);

	task.bsrc = *res;
	task.src = Tloc(*res, 0);

	task.timer = GDKusec();

	MOSinitializeScan(&task, task.bsrc);

	switch(ATOMbasetype(task.type)){
	case TYPE_bte: MOSdecompressInternal_bte(&task); break;
	case TYPE_sht: MOSdecompressInternal_sht(&task); break;
	case TYPE_int: MOSdecompressInternal_int(&task); break;
	case TYPE_lng: MOSdecompressInternal_lng(&task); break;
	case TYPE_flt: MOSdecompressInternal_flt(&task); break;
	case TYPE_dbl: MOSdecompressInternal_dbl(&task); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOSdecompressInternal_hge(&task); break;
#endif
	default: // Unknown block type. Should not happen.
		assert(0);
	}

	task.timer = GDKusec() - task.timer;

	BATsettrivprop(task.bsrc); // TODO: What's the purpose of this statement?

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
MOSselect2(bat *ret, const bat *bid, const bat *cid	, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	BAT *b, *bn, *cand = NULL;
	str msg = MAL_SUCCEED;
	BUN cnt = 0;
	MOStask task = {0};
	//
	// use default implementation if possible
	if( !isCompressed(*bid)){
		if(cid)
			return ALGselect2(ret,bid,cid,low,high,li,hi,anti);
		else
			return ALGselect1(ret,bid,low,high,li,hi,anti);
	}

	if ((*li != 0 && *li != 1) || (*hi != 0 && *hi != 1) || (*anti != 0 && *anti != 1)) throw(MAL, "mosaic.select", ILLEGAL_ARGUMENT);

	b= BATdescriptor(*bid);
	if( b == NULL)
			throw(MAL, "mosaic.select",RUNTIME_OBJECT_MISSING);

	// accumulator for the oids
	bn = COLnew((oid)0, TYPE_oid, BATcount(b), TRANSIENT);
	if( bn == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.select", RUNTIME_OBJECT_MISSING);
	}
	task.lb = (oid*) Tloc(bn,0);

	MOSinit(&task,b);
	MOSinitializeScan(&task, b);
	// drag along the candidate list into the task descriptor
	if (cid) {
		cand = BATdescriptor(*cid);
		if (cand == NULL){
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			throw(MAL, "mosaic.select", RUNTIME_OBJECT_MISSING);
		}
		task.cl = (oid*) Tloc(cand, 0);
		task.n = BATcount(cand);
	}

	struct canditer ci;
	task.ci = &ci;
	canditer_init(task.ci, b, cand);

	// determine block range to scan for partitioned columns
	/*
	** TODO: Figure out how do partitions relate to mosaic chunks.
	** Is it a good idea to set the capacity to the total size of the select operand b?
	*/

	switch(ATOMbasetype(task.type)){
	case TYPE_bte: MOSselect_bte(&task, low, high, li, hi, anti); break;
	case TYPE_sht: MOSselect_sht(&task, low, high, li, hi, anti); break;
	case TYPE_int: MOSselect_int(&task, low, high, li, hi, anti); break;
	case TYPE_lng: MOSselect_lng(&task, low, high, li, hi, anti); break;
	case TYPE_flt: MOSselect_flt(&task, low, high, li, hi, anti); break;
	case TYPE_dbl: MOSselect_dbl(&task, low, high, li, hi, anti); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOSselect_hge(&task, low, high, li, hi, anti); break;
#endif
	}
	// derive the filling
	cnt = (BUN) (task.lb - (oid*) Tloc(bn,0));
	assert(bn->batCapacity >= cnt);
	BATsetcount(bn,cnt);
	bn->tnil = false;
	bn->tnonil = true;
	bn->tsorted = true;
	bn->trevsorted = cnt <=1;
	bn->tkey = true;
	MOSvirtualize(bn);

	*ret = bn->batCacheid;

	BBPunfix(b->batCacheid);
	if (cand != NULL) BBPunfix(cand->batCacheid);
	BBPkeepref(bn->batCacheid);
	return msg;
}

str
MOSselect2nil(bat *ret, const bat *bid, const bat *cid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti, const bit *unknown) {
	str msg;

	if (!*unknown)
		return MOSselect2(ret, bid, cid, low, high, li, hi, anti);

	if ((*li != 0 && *li != 1) || (*hi != 0 && *hi != 1) || (*anti != 0 && *anti != 1)) throw(MAL, "mosaic.select", ILLEGAL_ARGUMENT);

	BAT* b = BATdescriptor(*bid);

	/* here we don't need open ended parts with nil */
	const void* nilptr = ATOMnilptr(b->ttype);
	if (*li == 1 && ATOMcmp(b->ttype, low, nilptr) == 0) 
		low = high; 
	else if (*hi == 1 && ATOMcmp(b->ttype, high, nilptr) == 0)
		high = low;
	if (ATOMcmp(b->ttype, low, high) == 0 && ATOMcmp(b->ttype, high, nilptr) == 0) /* ugh sql nil != nil */ {
		const bit nanti = !*anti;

		 /* GOD AWFUL UGLY work around to match up semantics MOSselect and BATselect.
		  *	nil	nil	A*		B*		false	x != nil *it must hold that A && B == false.
		  *	nil	nil	A*		B*		true	NOTHING *it must hold that A && B == false.
		  */
		const bit new_li = false;
		msg = MOSselect2(ret, bid, cid, low, high, &new_li, hi, &nanti);
	}
	else {
		msg =  MOSselect2(ret, bid, cid, low, high, li, hi, anti);
	}

	BBPunfix(b->batCacheid);

	return msg;
}

str
MOSselect1(bat *ret, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti) {
	return MOSselect2(ret, bid, NULL, low, high, li, hi, anti);
}

str
MOSselect1nil(bat *ret, const bat *bid, const void *low, const void *high, const bit *li, const bit *hi, const bit *anti, const bit *unknown) {
	return MOSselect2nil(ret, bid, NULL, low, high, li, hi, anti, unknown);
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

str MOSprojection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret, *lid =0, *rid=0;
	BAT *bl = NULL, *br = NULL, *bn;
	BUN cnt;
	oid *ol =0, o = 0;
	str msg= MAL_SUCCEED;
	MOStask task = {0};

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

	MOSinit(&task,br);
	MOSinitializeScan(&task, br);
	task.src = (char*) Tloc(bn,0);

	task.cl = ol;
	task.n = cnt;

	struct canditer ci;
	task.ci = &ci;
	canditer_init(task.ci, NULL, bl);

	switch(ATOMbasetype(task.type)){
	case TYPE_bte: MOSprojection_bte(&task); break;
	case TYPE_sht: MOSprojection_sht(&task); break;
	case TYPE_int: MOSprojection_int(&task); break;
	case TYPE_lng: MOSprojection_lng(&task); break;
	case TYPE_flt: MOSprojection_flt(&task); break;
	case TYPE_dbl: MOSprojection_dbl(&task); break;
#ifdef HAVE_HGE
	case TYPE_hge: MOSprojection_hge(&task); break;
#endif
	}

	/* adminstrative wrapup of the projection */
	BBPunfix(*lid);
	BBPunfix(*rid);

	BATsetcount(bn,task.cnt);
	bn->tnil = 0;
	bn->tnonil = br->tnonil;
	bn->tsorted = bn->trevsorted = cnt <= 1;
	BBPkeepref(*ret = bn->batCacheid);
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

/* A mosaic join operator that works when either the left or the right side is compressed.
 * Furthermore if both sides are in possesion of a mosaic index,
 * the operator implementation currently only uses the mosaic index of the left side.
 */

#define PREPARE_JOIN_CONTEXT(COMPRESSED_BAT, COMPRESSED_BAT_CL, UNCOMPRESSED_BAT, UNCOMPRESSED_BAT_CL) \
{\
	MOSinit(&task,COMPRESSED_BAT);\
	MOSinitializeScan(&task, COMPRESSED_BAT);\
	task.stop = BATcount(COMPRESSED_BAT);\
	task.src= Tloc(UNCOMPRESSED_BAT,0);\
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
	MOStask task = {0};

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
	if( bln == NULL || brn == NULL){
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

	task.ci = &ci_c;

	if ( bl->tmosaic){
		PREPARE_JOIN_CONTEXT(bl, sl, br, sr);
	} else {
		PREPARE_JOIN_CONTEXT(br, sr, bl, sl);
		swapped=1;
	}
	task.lbat = bln;
	task.rbat = brn;

	bit* COUI = NULL;

	if ((pci->argc != 9) || ((*(COUI = getArgReference_bit(stk,pci,8))) == bit_nil) || (*COUI == false)) {
		switch(ATOMbasetype(task.type)){
		case TYPE_bte: msg = MOSjoin_bte(&task, u, &ci_u, *nil_matches); break;
		case TYPE_sht: msg = MOSjoin_sht(&task, u, &ci_u, *nil_matches); break;
		case TYPE_int: msg = MOSjoin_int(&task, u, &ci_u, *nil_matches); break;
		case TYPE_lng: msg = MOSjoin_lng(&task, u, &ci_u, *nil_matches); break;
		case TYPE_flt: msg = MOSjoin_flt(&task, u, &ci_u, *nil_matches); break;
		case TYPE_dbl: msg = MOSjoin_dbl(&task, u, &ci_u, *nil_matches); break;
#ifdef HAVE_HGE
		case TYPE_hge: msg = MOSjoin_hge(&task, u, &ci_u, *nil_matches); break;
#endif
		}
	}
	else {
		switch(ATOMbasetype(task.type)){
		case TYPE_bte: msg = MOSjoin_COUI_bte(&task, u, &ci_u, *nil_matches); break;
		case TYPE_sht: msg = MOSjoin_COUI_sht(&task, u, &ci_u, *nil_matches); break;
		case TYPE_int: msg = MOSjoin_COUI_int(&task, u, &ci_u, *nil_matches); break;
		case TYPE_lng: msg = MOSjoin_COUI_lng(&task, u, &ci_u, *nil_matches); break;
		case TYPE_flt: msg = MOSjoin_COUI_flt(&task, u, &ci_u, *nil_matches); break;
		case TYPE_dbl: msg = MOSjoin_COUI_dbl(&task, u, &ci_u, *nil_matches); break;
#ifdef HAVE_HGE
		case TYPE_hge: msg = MOSjoin_COUI_hge(&task, u, &ci_u, *nil_matches); break;
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

    return msg;
}

// The analyse routine runs through the BAT dictionary and assess
// all possible compression options.

/*
 * Start searching for a proper compression scheme.
 * Naive creation of all patterns in increasing number of bits
 */

static int
makepatterns(uint16_t *patterns, int size, str compressions, BAT* b)
{
	int i,j,k, idx, bit=1, step = MOSAIC_METHODS;
	int lim= 8*7*6*5*4*3*2;
	// int candidate[MOSAIC_METHODS]= {0};
	sht compression_mask = 0;

	_construct_compression_mask(&compression_mask, compressions);

	for(unsigned i = 0; i< MOSAIC_METHODS; i++) {
		if ( METHOD_IS_SET(compression_mask, i) && !MOSisTypeAllowed(i, b) ) {
			// Unset corresponding bit if type is not allowed.
			UNSET_METHOD(compression_mask, i);
		}
	}

	for( k=0, i=0; i<lim && k <size; i++){
		patterns[k]=0;
		idx =i;
		while(idx > 0) {
			if(METHOD_IS_SET(compression_mask, idx % step) )  {
				SET_METHOD(patterns[k], (idx % step));
			}
			idx /= step;
		}

		// weed out duplicates
		for( j=0; j< k; j++)
			if(patterns[k] == patterns[j]) break;
		if( j < k ) continue;
		
#ifdef _MOSAIC_DEBUG_
		mnstr_printf(GDKstdout,"#");
		for(j=0, bit=1; j < MOSAIC_METHODS; j++){
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
 * Drop MOSmethods if they are not able to reduce the size below a factor 1.0
 */
#define CANDIDATES 256  /* all three combinations */

struct PAT{
	bool include;
	str technique;
	BUN xsize;
	dbl xf;
	lng clk1, clk2;
}pat[CANDIDATES];

str
MOSAnalysis(BAT *b, BAT *btech, BAT *boutput, BAT *bratio, BAT *bcompress, BAT *bdecompress, str compressions)
{
	unsigned i,j,cases, bid= b->batCacheid;
	uint16_t pattern[CANDIDATES];
	uint16_t antipattern[CANDIDATES];
	unsigned antipatternSize = 0;
	char buf[1024]={0}, *t;
	str msg = MAL_SUCCEED;

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
		for(j=0; j < MOSAIC_METHODS; j++){
			if( METHOD_IS_SET(pattern[i], j)){
				snprintf(t, 1024 - strlen(buf),"%s ", MOSmethods[j].name);
				t= buf + strlen(buf);
			}
		}
		*t = '\0';

		t = buf;

		pat[i].technique= GDKstrdup(buf);
		pat[i].clk1 = GDKms();

		MOSsetLock(b);

		Heap* original = NULL;
		Heap* voriginal = NULL;

		if (BATcheckmosaic(b)){
			original = b->tmosaic;
			b->tmosaic = NULL;
			voriginal = b->tvmosaic;
			b->tvmosaic = NULL;
		}

		const char* compressions = buf;
		msg = MOScompressInternal( b, compressions);

		pat[i].clk1 = GDKms()- pat[i].clk1;

		if(msg != MAL_SUCCEED || b->tmosaic == NULL){
			if (msg != MAL_SUCCEED) {
				freeException(msg);
				msg = MAL_SUCCEED;
			}
			// aborted compression experiment
			MOSdestroy(BBPdescriptor(bid));

			if (original) {
				b->tmosaic = original;
			}
			if (voriginal) {
				b->tvmosaic = voriginal;
			}
			pat[i].include = false;
			MOSunsetLock(b);
			continue;
		}

		pat[i].xsize = (BUN) b->tmosaic->free + (BUN) b->tvmosaic->free;
		pat[i].xf= ((MosaicHdr)  b->tmosaic->base)->ratio;

		/* analyse result block distribution to exclude complicated compression combination that
		 * (probably) won't improve compression rate.
		 */
		if (pat[i].xf >= 0 && pat[i].xf < 1.0) {;
			bool keep = false;
			for(j=0; j < MOSAIC_METHODS; j++){
				if (pattern[i] == MOSmethods[j].bit ) {
					/* We'll keep it if is a singleton compression strategy.
						* It might still compress well in combination with another compressor.
						*/
					keep = true;
				}
			}
			if (!keep) {
				antipattern[antipatternSize++] = pattern[i];
			}
		}

		for(j=0; j < MOSAIC_METHODS; j++){
			if ( ((MosaicHdr)  b->tmosaic->base)->blks[j] == 0) {
				antipattern[antipatternSize++] = pattern[i];
				pat[i].include = false;
			}
		}

		BAT* decompressed;
		pat[i].clk2 = GDKms();
		msg = MOSdecompressInternal( &decompressed, b);
		pat[i].clk2 = GDKms()- pat[i].clk2;
		MOSdestroy(decompressed);
		BBPreclaim(decompressed);

		// get rid of mosaic heap
		MOSdestroy(b);

		if (original) {
			b->tmosaic = original;
		}

		if (voriginal) {
			b->tvmosaic = voriginal;
		}

		MOSunsetLock(b);

		if(msg != MAL_SUCCEED) return msg; // Probably a malloc failure.
	}

	// Collect the results in a table
	for(i=0;i< CANDIDATES; i++){
		if(pat[i].include) {
			// round down to three decimals.
			pat[i].xf = ((dbl) (int) (pat[i].xf * 1000)) / 1000;

			if(msg == MAL_SUCCEED && (BUNappend(boutput,&pat[i].xsize,false) != GDK_SUCCEED ||
				BUNappend(btech,pat[i].technique,false) != GDK_SUCCEED ||
				BUNappend(bratio,&pat[i].xf,false) != GDK_SUCCEED ||
				BUNappend(bcompress,&pat[i].clk1,false) != GDK_SUCCEED ||
				BUNappend(bdecompress,&pat[i].clk2,false) != GDK_SUCCEED ))
				msg = createException(MAL, "mosaic.analysis", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}

		GDKfree(pat[i].technique);
	}

	return msg;
}
