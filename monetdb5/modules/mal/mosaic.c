/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) 2014 author Martin Kersten
 * Adaptive compression scheme to reduce the storage footprint for stable persistent data.
 * The permissible compression techniques can be controlled thru an argument list
*/

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_hdr.h"
#include "mosaic_none.h"
#include "mosaic_rle.h"
#include "mosaic_dict.h"
#include "mosaic_zone.h"
#include "mosaic_delta.h"
#include "mosaic_linear.h"

static char *filtername[]={"none","rle","dict","delta","linear","zone","EOL"};

static void
MOSinit(MOStask task, BAT *b){
	char * base = Tloc(b,BUNfirst(b));
	task->type = b->ttype;
	task->hdr = (MosaicHdr) base;
	base += MosaicHdrSize;
	task->blk = (MosaicBlk)  base;
	task->dst = base + MosaicBlkSize;
}

static void
MOSdumpTask(Client cntxt,MOStask task)
{
	int i;
	mnstr_printf(cntxt->fdout,"# ");
	for ( i=0; i < MOSAIC_METHODS; i++){
		mnstr_printf(cntxt->fdout, "%s\t"LLFMT "\t"LLFMT "\t" LLFMT "\t",
			filtername[i], task->wins[i], task->elms[i],task->time[i]);
	}
}

// dump a compressed BAT
static void
MOSdumpInternal(Client cntxt, BAT *b){
	MOStask task=0;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		return;
	MOSinit(task,b);
	while(task->blk){
		switch(task->blk->tag){
		case MOSAIC_NONE:
			MOSdump_none(cntxt,task);
			MOSskip_none(task);
			break;
		case MOSAIC_RLE:
			MOSdump_rle(cntxt,task);
			MOSskip_rle(task);
			break;
		case MOSAIC_DICT:
			MOSdump_dict(cntxt,task);
			MOSskip_dict(task);
			break;
		case MOSAIC_DELTA:
			MOSdump_delta(cntxt,task);
			MOSskip_delta(task);
			break;
		case MOSAIC_ZONE:
			MOSdump_zone(cntxt,task);
			MOSskip_zone(task);
		}
	}
}

str
MOSdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	int bid = *(int*) getArgReference(stk,pci,1);
	BAT *b;
	str msg = MAL_SUCCEED;

	(void) mb;

	if  ((b = BATdescriptor(bid)) == NULL)
		throw(MAL,"mosaic.dump",INTERNAL_BAT_ACCESS);
	if ( !b->T->heap.compressed){
		BBPreleaseref(bid);
		return MAL_SUCCEED;
	}
	MOSdumpInternal(cntxt,b);
	BBPreleaseref(bid);
	return msg;
}

static BAT*
inheritCOL( BAT *bn, COLrec *cn, BAT *b, COLrec *c, bat p )
{
	/* inherit column as view */
	str nme = cn->id;

	assert((b->H == c && p == VIEWhparent(b)) ||
	       (b->T == c && p == VIEWtparent(b)));
	assert(bn->H == cn || bn->T == cn);
	assert(cn->props == NULL);
	assert(cn->vheap == NULL);
	assert(cn->hash  == NULL);
	assert(bn->S->deleted  == b->S->deleted );
	assert(bn->S->first    == b->S->first   );
	assert(bn->S->inserted == b->S->inserted);
	assert(bn->S->count    == b->S->count   );

	HEAPfree(&cn->heap);

	if (p == 0)
		p = b->batCacheid;
	bn->S->capacity = MIN( bn->S->capacity, b->S->capacity );
	*cn = *c;
	BBPshare(p);
	if (cn->vheap) {
		assert(cn->vheap->parentid > 0);
		BBPshare(cn->vheap->parentid);
	}
	cn->heap.copied = 0;
	cn->props = NULL;
	cn->heap.parentid = p;
	cn->id = nme;
	if (isVIEW(b))
		cn->hash = NULL;
	else
		cn->hash = c->hash;

	return bn;
}

/*
 * Compression is focussed on a single column.
 * Multiple compression techniques are applied at the same time.
 */

str
MOScompressInternal(Client cntxt, int *ret, int *bid, str properties)
{
	BAT *b, *bn;
	BUN cnt;
	int i;
	char *c;
	str msg = MAL_SUCCEED;
	MOStask task;
	int cand;
	lng percentage=0, perc;
	int filter[MOSAIC_METHODS];
	
	if( properties)
		for( i = 0; i< MOSAIC_METHODS; i++)
			filter[i]= strstr(properties,filtername[i]) != 0;
	else
		for( i = 0; i< MOSAIC_METHODS; i++)
			filter[i]= 1;
	if( properties && (c = strstr(properties,"test")) ){
		if ( atoi(c+4) < DICTSIZE){
			if( atoi(c+4))
				dictsize = atoi(c+4);
		} else
			dictsize = 2;
	}

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mosaic.compress", INTERNAL_BAT_ACCESS);

	if ( b->ttype == TYPE_void){
		// void columns are already compressed
		BBPkeepref(*ret = b->batCacheid);
		return msg;
	}

	if (b->T->heap.compressed) {
		BBPkeepref(*ret = b->batCacheid);
		return msg;	// don't compress twice
	}

	cnt = BATcount(b);
	if ( isVIEWCOMBINE(b) || cnt < MIN_INPUT_COUNT ){
		/* no need to compress */
		BBPkeepref(*ret = b->batCacheid);
		return msg;
	}

#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#compress bat %d properties %s\n",*bid,properties?properties:"");
#endif
	// allocate space for the compressed version.
	// It should always take less space then the orginal column.
	// But be prepared that a last block header may  be stored
	// use a size overshoot. Also be aware of possible dictionary headers
	bn = BATnew( TYPE_void, b->ttype, cnt + 3 *  MosaicBlkSize + MosaicHdrSize, TRANSIENT);
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL,"mosaic.compress", MAL_MALLOC_FAIL);
	}

	// actual compression mosaic
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bn->batCacheid);
		throw(MAL, "mosaic.compress", MAL_MALLOC_FAIL);
	}
	// initialize the non-compressed read pointer
	task->src = Tloc(b, BUNfirst(b));
	task->elm = BATcount(b);

	// prepare a compressed heap
	MOSinit(task,bn);
	MOSinitHeader(task);

	// always start with an EOL block
	task->blk->tag = MOSAIC_EOL;
	task->blk->cnt = 0;

	while(task->elm > 0){
		// default is to extend the non-compressed block
		//mnstr_printf(cntxt->fdout,"#elements "BUNFMT"\n",task->elm);
		cand = MOSAIC_NONE;
		perc = 100;
		percentage = 100;
		
		// select candidate amongst those
		if ( filter[MOSAIC_RLE]){
			perc = MOSestimate_rle(cntxt,task);
			if ( perc < percentage){
				cand = MOSAIC_RLE;
				percentage = perc;
			}
		}
		if ( filter[MOSAIC_DICT]){
			perc = MOSestimate_dict(cntxt,task);
			if (perc >= 0 && perc <= percentage){
				cand = MOSAIC_DICT;
				percentage = perc;
			}
		}
		if ( filter[MOSAIC_ZONE]){
			perc = MOSestimate_zone(cntxt,task);
			if (perc >= 0 && perc < percentage){
				cand = MOSAIC_ZONE;
				percentage = perc;
			}
		}
		if ( filter[MOSAIC_DELTA]){
			perc = MOSestimate_delta(cntxt,task);
			if ( perc >=0 &&  perc < percentage){
				cand = MOSAIC_DELTA;
				percentage = perc;
			}
		}
		if ( filter[MOSAIC_LINEAR]){
			perc = MOSestimate_linear(cntxt,task);
			if ( perc >=0 &&  perc < percentage){
				cand = MOSAIC_LINEAR;
				percentage = perc;
			}
		}

		// wrapup previous block
		switch(cand){
		case MOSAIC_RLE:
		case MOSAIC_DICT:
		case MOSAIC_DELTA:
		case MOSAIC_LINEAR:
			// close the non-compressed part
			if( (task->blk->tag == MOSAIC_NONE || task->blk->tag == MOSAIC_ZONE) && task->blk->cnt ){
				MOSupdateHeader(cntxt,task);
				if( task->blk->tag == MOSAIC_NONE)
					MOSskip_none(task);
				else
					MOSskip_zone(task);
				// always start with an EOL block
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				task->blk->tag = MOSAIC_EOL;
				task->blk->cnt = 0;
			}
		}
		// apply the compression to a chunk
		switch(cand){
		case MOSAIC_DICT:
			MOScompress_dict(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= task->blk->cnt;
			MOSadvance_dict(task);
			task->blk->tag = MOSAIC_EOL;
			task->blk->cnt = 0;
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_DELTA:
			MOScompress_delta(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= task->blk->cnt;
			MOSadvance_delta(task);
			task->blk->tag = MOSAIC_EOL;
			task->blk->cnt = 0;
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_LINEAR:
			MOScompress_linear(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= task->blk->cnt;
			MOSadvance_linear(task);
			task->blk->tag = MOSAIC_EOL;
			task->blk->cnt = 0;
			task->dst = ((char*) task->blk)+ 3 * MosaicBlkSize;
			break;
		case MOSAIC_RLE:
			MOScompress_rle(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= task->blk->cnt;
			MOSadvance_rle(task);
			task->blk->tag = MOSAIC_EOL;
			task->blk->cnt = 0;
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_ZONE:
			if( task->blk->cnt == 0)
				task->dst += 2 * MosaicBlkSize;
			if ( task->blk->cnt > MAXZONESIZE){
				MOSupdateHeader(cntxt,task);
				MOSadvance_zone(task);
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				task->blk->tag = MOSAIC_EOL;
				task->blk->cnt = 0;
			}
			MOScompress_zone(cntxt,task);
			break;
		default :
			// continue to use the last block header.
			MOScompress_none(cntxt,task);
		}
	}
	if( (task->blk->tag == MOSAIC_NONE || task->blk->tag == MOSAIC_ZONE) && task->blk->cnt){
		MOSupdateHeader(cntxt,task);
		if( task->blk->tag == MOSAIC_NONE ){
			MOSadvance_none(task);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
		} else{
			MOSadvance_zone(task);
			task->dst = ((char*) task->blk)+ 3 * MosaicBlkSize;
		}
		task->blk->tag = MOSAIC_EOL;
		task->blk->cnt = 0;
	}
//#ifdef _DEBUG_MOSAIC_
	MOSdumpTask(cntxt,task);
	mnstr_printf(cntxt->fdout,"\n");
//#endif
	// if we couldnt compress ignore the result
	if( task->elms[MOSAIC_NONE] == (lng) cnt){
		GDKfree(task);
		BBPreleaseref(bn->batCacheid);
		BBPkeepref(*ret = b->batCacheid);
		return MAL_SUCCEED;
	}

	BATsetcount(bn, cnt);
	BATseqbase(bn,b->hseqbase);
	bn->hdense = 1;
	bn->hkey = 1;
	bn->batDirty = 1;
	bn->T->heap.compressed= 1;
	bn->T->heap.count = cnt;
	bn->T->nonil = b->T->nonil;
	bn->tsorted = b->tsorted;
	bn->trevsorted = b->trevsorted;
	bn->tkey = b->tkey;
	BBPkeepref(*ret = bn->batCacheid);
	BBPreleaseref(b->batCacheid);
#ifdef _DEBUG_MOSAIC_
	MOSdumpInternal(cntxt,bn);
#endif
	GDKfree(task);
	return msg;
}

str
MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	str prop = NULL;
	(void) mb;
	if( pci->argc == 3)
		prop = *(str*) getArgReference(stk,pci,2);
	return MOScompressInternal(cntxt, (int*) getArgReference(stk,pci,0), (int*) getArgReference(stk,pci,1), prop);
}

// bulk decompress a heap
str
MOSdecompressInternal(Client cntxt, int *ret, int *bid)
{	
	BAT *b, *bn;
	MOStask task;
	BUN elm;
	(void) cntxt;

#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#decompress bat %d\n",*bid);
#endif
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mosaic.decompress", INTERNAL_BAT_ACCESS);

	if (!b->T->heap.compressed) {
		BBPkeepref(*ret = b->batCacheid);
		return MAL_SUCCEED;
	}
	if (isVIEWCOMBINE(b)) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.decompress", "cannot decompress VIEWCOMBINE");
	}
	if (b->T->heap.compressed && VIEWtparent(b)) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.decompress", "cannot decompress tail-VIEW");
	}

	if ( b->T->heap.compressed ) {
		elm = b->T->heap.count;
	} else {
		//  deal with views
		elm = BATcount(b);
	}
	
	bn = BATnew( TYPE_void, b->ttype, elm, TRANSIENT);
	if ( bn == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.decompress", MAL_MALLOC_FAIL);
	}

	// inject the de-decompression
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bn->batCacheid);
		throw(MAL, "mosaic.decompress", MAL_MALLOC_FAIL);
	}
	MOSinit(task,b);;
	task->src = Tloc(bn, BUNfirst(bn));
	while(task->blk){
		switch(task->blk->tag){
		case MOSAIC_DICT:
			MOSdecompress_dict(cntxt,task);
			MOSskip_dict(task);
			break;
		case MOSAIC_DELTA:
			MOSdecompress_delta(cntxt,task);
			MOSskip_delta(task);
			break;
		case MOSAIC_LINEAR:
			MOSdecompress_linear(cntxt,task);
			MOSskip_linear(task);
			break;
		case MOSAIC_NONE:
			MOSdecompress_none(cntxt,task);
			MOSskip_none(task);
			break;
		case MOSAIC_RLE:
			MOSdecompress_rle(cntxt,task);
			MOSskip_rle(task);
			break;
		case MOSAIC_ZONE:
			MOSdecompress_zone(cntxt,task);
			MOSskip_zone(task);
			break;
		default: assert(0);
		}
	}

	BATsetcount(bn, elm);
	BATseqbase(bn,b->hseqbase);
    bn->hdense = 1;
    bn->hkey = 1;
	bn->T->nonil = b->T->nonil;
	bn->T->nil = b->T->nil;
	bn->T->seq = b->T->seq;
	bn->tsorted = b->tsorted;
	bn->trevsorted = b->trevsorted;
	//bn->tkey = b->tkey;
	//bn->batDirty = 1;

	if (!b->T->heap.compressed && b->ttype != TYPE_void) {
		/* inherit original uncompressed tail as view */
		bn = inheritCOL( bn, bn->T, b, b->T, VIEWtparent(b) );
	}
	GDKfree(task);
	BBPreleaseref(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MOSdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) mb;
	return MOSdecompressInternal(cntxt, (int*) getArgReference(stk,pci,0), (int*) getArgReference(stk,pci,1));
}

// The remainders is cloned from the generator code base
// overload the algebra functions to check for compressed heaps.
static int
isCompressed(int bid)
{
	BAT *b;
	int r=0;
	if( bid == 0)
		return 0;
	b = BATdescriptor(bid);
	r = b->T->heap.compressed;
	BBPreleaseref(bid);
	return r;
}

#ifdef _MSC_VER
#define nextafter   _nextafter
float nextafterf(float x, float y);
#endif

#define PREVVALUEbte(x) ((x) - 1)
#define PREVVALUEsht(x) ((x) - 1)
#define PREVVALUEint(x) ((x) - 1)
#define PREVVALUElng(x) ((x) - 1)
#define PREVVALUEoid(x) ((x) - 1)
#define PREVVALUEflt(x) nextafterf((x), -GDK_flt_max)
#define PREVVALUEdbl(x) nextafter((x), -GDK_dbl_max)

#define NEXTVALUEbte(x) ((x) + 1)
#define NEXTVALUEsht(x) ((x) + 1)
#define NEXTVALUEint(x) ((x) + 1)
#define NEXTVALUElng(x) ((x) + 1)
#define NEXTVALUEoid(x) ((x) + 1)
#define NEXTVALUEflt(x) nextafterf((x), GDK_flt_max)
#define NEXTVALUEdbl(x) nextafter((x), GDK_dbl_max)

/* The algebra operators should fall back to their default
 * when we know that the heap is not compressed
 * The actual decompression should wait until we know that
 * the administration thru SQL layers works properly.
 */
str
MOSsubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *li, *hi, *anti;
	void *low, *hgh;
	int *ret, *bid, *cid= 0;
	int i;
	BUN cnt = 0;
	BUN first =0, last = 0;
	BAT *b, *bn, *cand = NULL;
	str msg = MAL_SUCCEED;
	MOStask task;

	(void) cntxt;
	(void) mb;
	ret = (int *) getArgReference(stk, pci, 0);
	bid = (int *) getArgReference(stk, pci, 1);

	if (pci->argc == 8) {	/* candidate list included */
		cid = (int *) getArgReference(stk, pci, 2);
		i = 3;
	} else
		i = 2;
	low = (void *) getArgReference(stk, pci, i);
	hgh = (void *) getArgReference(stk, pci, i + 1);
	li = (bit *) getArgReference(stk, pci, i + 2);
	hi = (bit *) getArgReference(stk, pci, i + 3);
	anti = (bit *) getArgReference(stk, pci, i + 4);
	//
	// use default implementation if possible
	if( !isCompressed(*bid))
		return ALGsubselect1(ret,bid,low,hgh,li,hi,anti);

	b= BATdescriptor(*bid);
	if( b == NULL)
			throw(MAL, "mosaic.subselect",RUNTIME_OBJECT_MISSING);
	// determine the elements in the compressed structure
	last = b->T->heap.count;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
	}

	// accumulator for the oids
	bn = BATnew(TYPE_void, TYPE_oid, last, TRANSIENT);
	if( bn == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
	}
	task->lb = (oid*) Tloc(bn,BUNfirst(bn));

	MOSinit(task,b);
	// drag along the candidate list into the task descriptor
	if (cid) {
		cand = BATdescriptor(*cid);
		if (cand == NULL){
			BBPreleaseref(b->batCacheid);
			BBPreleaseref(bn->batCacheid);
			throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
		}
		task->cl = (oid*) Tloc(cand, BUNfirst(cand));
		task->n = BATcount(cand);
	}

	// loop thru all the chunks and collect the partial results
	if ( task->cl && task->n && *task->cl > (oid) first)
		first = (BUN)  *task->cl;
	first = MOSfindChunk(cntxt,task,first);
	while(task->blk && first < last ){
		switch(task->blk->tag){
		case MOSAIC_RLE:
			MOSsubselect_rle(cntxt,task,first,first + task->blk->cnt,low,hgh,li,hi,anti);
			first += task->blk->cnt;
			MOSskip_rle(task);
			break;
		case MOSAIC_DICT:
			MOSsubselect_dict(cntxt,task,first,first + task->blk->cnt,low,hgh,li,hi,anti);
			first += task->blk->cnt;
			MOSskip_dict(task);
			break;
		case MOSAIC_DELTA:
			MOSsubselect_delta(cntxt,task,first,first + task->blk->cnt,low,hgh,li,hi,anti);
			first += task->blk->cnt;
			MOSskip_delta(task);
			break;
		case MOSAIC_LINEAR:
			MOSsubselect_linear(cntxt,task,first,first + task->blk->cnt,low,hgh,li,hi,anti);
			first += task->blk->cnt;
			MOSskip_linear(task);
			break;
		case MOSAIC_ZONE:
			MOSsubselect_zone(cntxt,task,first,first + task->blk->cnt,low,hgh,li,hi,anti);
			first += task->blk->cnt;
			MOSskip_zone(task);
			break;
		case MOSAIC_NONE:
		default:
			MOSsubselect_none(cntxt,task,first,first + task->blk->cnt,low,hgh,li,hi,anti);
			first += task->blk->cnt;
			MOSskip_none(task);
		}
	}
	// derive the filling
	cnt = (BUN) (task->lb - (oid*) Tloc(bn,BUNfirst(bn)));
	BATsetcount(bn,cnt);
	BATseqbase(bn,b->hseqbase);
    bn->hdense = 1;
    bn->hkey = 1;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	bn->tsorted = 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = 1;
	* (bat *) getArgReference(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return msg;
}


str MOSthetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, cid =0,  *ret, *bid;
	BAT *b = 0, *cand = 0, *bn = NULL;
	BUN first = 0,last = 00;
	BUN cnt=0;
	str msg= MAL_SUCCEED;
	char **oper;
	void *low;
	MOStask task;

	(void) mb;
	(void) cntxt;
	ret= (int*) getArgReference(stk,pci,0);
	bid= (int*) getArgReference(stk,pci,1);
	if( pci->argc == 5){ // candidate list included
		cid = *(int*) getArgReference(stk,pci, 2);
		idx = 3;
	} else idx = 2;
	low= (void*) getArgReference(stk,pci,idx);
	oper= (char**) getArgReference(stk,pci,idx+1);

	if( !isCompressed(*bid))
		return ALGthetasubselect1(ret,bid,low, (const char **)oper);
	
	b = BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	// determine the elements in the compressed structure
	last = b->T->heap.count;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	}

	// accumulator for the oids
	bn = BATnew(TYPE_void, TYPE_oid, last, TRANSIENT);
	if( bn == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	}
	BATseqbase(bn,0);
	task->lb = (oid*) Tloc(bn,BUNfirst(bn));

	MOSinit(task,b);
	// drag along the candidate list into the task descriptor
	if (cid) {
		cand = BATdescriptor(cid);
		if (cand == NULL){
			BBPreleaseref(b->batCacheid);
			BBPreleaseref(bn->batCacheid);
			throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
		}
		task->cl = (oid*) Tloc(cand, BUNfirst(cand));
		task->n = BATcount(cand);
	}

	// loop thru all the chunks and collect the partial results
	if ( task->cl && task->n && *task->cl > (oid) first)
		first = (BUN)  *task->cl;
	first = MOSfindChunk(cntxt,task,first);

	while(task->blk && first < last ){
		switch(task->blk->tag){
		case MOSAIC_RLE:
			MOSthetasubselect_rle(cntxt,task,first,first + task->blk->cnt,low,*oper);
			first += task->blk->cnt;
			MOSskip_rle(task);
			break;
		case MOSAIC_DELTA:
			MOSthetasubselect_delta(cntxt,task,first,first + task->blk->cnt,low,*oper);
			first += task->blk->cnt;
			MOSskip_delta(task);
			break;
		case MOSAIC_LINEAR:
			MOSthetasubselect_linear(cntxt,task,first,first + task->blk->cnt,low,*oper);
			first += task->blk->cnt;
			MOSskip_linear(task);
			break;
		case MOSAIC_DICT:
			MOSthetasubselect_dict(cntxt,task,first,first + task->blk->cnt,low,*oper);
			first += task->blk->cnt;
			MOSskip_dict(task);
			break;
		case MOSAIC_ZONE:
			MOSthetasubselect_zone(cntxt,task,first,first + task->blk->cnt,low,*oper);
			first += task->blk->cnt;
			MOSskip_zone(task);
			break;
		case MOSAIC_NONE:
		default:
			MOSthetasubselect_none(cntxt,task,first,first + task->blk->cnt,low,*oper);
			first += task->blk->cnt;
			MOSskip_none(task);
		}
	}
	// derive the filling
	cnt = (BUN)( task->lb - (oid*) Tloc(bn,BUNfirst(bn)));
	
	if( cid)
		BBPreleaseref(cid);
	if( bn){
		BATsetcount(bn,cnt);
		bn->hdense = 1;
		bn->hkey = 1;
		bn->T->nil = 0;
		bn->T->nonil = 1;
		bn->tsorted = 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->tkey = 1;
		BBPkeepref(*(int*)getArgReference(stk,pci,0)= bn->batCacheid);
	}
	return msg;
}

str MOSleftfetchjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret, *lid =0, *rid=0;
	BAT *bl = NULL, *br = NULL, *bn;
	BUN cnt, first=0;
	oid *ol =0, o = 0;
	str msg= MAL_SUCCEED;
	MOStask task;

	(void) mb;
	(void) cntxt;

	ret = (int*) getArgReference(stk,pci,0);
	lid = (int*) getArgReference(stk,pci,1);
	rid = (int*) getArgReference(stk,pci,2);

	if( !isCompressed(*rid))
		return ALGleftfetchjoin(ret,lid,rid);

	bl = BATdescriptor(*lid);
	if( bl == NULL)
		throw(MAL,"mosaic.leftfetchjoin",RUNTIME_OBJECT_MISSING);
	br = BATdescriptor(*rid);
	if( br == NULL){
		BBPreleaseref(*rid);
		throw(MAL,"mosaic.leftfetchjoin",RUNTIME_OBJECT_MISSING);
	}
	cnt = BATcount(bl);
	bn = BATnew(TYPE_void,br->ttype, cnt, TRANSIENT);
	if ( bn == NULL){
		BBPreleaseref(*lid);
		BBPreleaseref(*rid);
		throw(MAL,"mosaic.leftfetchjoin",MAL_MALLOC_FAIL);
	}

	if ( bl->ttype == TYPE_void)
		o = bl->tseqbase;
	else
		ol = (oid*) Tloc(bl,BUNfirst(bl));

	(void) o;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(bl->batCacheid);
		BBPreleaseref(br->batCacheid);
		throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
	}
	MOSinit(task,br);
	task->src = (char*) Tloc(bn,BUNfirst(bn));

	task->cl = ol;
	task->n = cnt;
	if( cnt)
		first = *ol;

	first = MOSfindChunk(cntxt,task,first);

	// loop thru all the chunks and fetch all results
	while(task->blk )
		switch(task->blk->tag){
		case MOSAIC_RLE:
			MOSleftfetchjoin_rle(cntxt, task, first, first + task->blk->cnt);
			first += task->blk->cnt;
			MOSskip_rle(task);
			break;
		case MOSAIC_DICT:
			MOSleftfetchjoin_dict(cntxt, task, first, first + task->blk->cnt);
			first += task->blk->cnt;
			MOSskip_dict(task);
			break;
		case MOSAIC_DELTA:
			MOSleftfetchjoin_delta(cntxt, task, first, first + task->blk->cnt);
			first += task->blk->cnt;
			MOSskip_delta(task);
			break;
		case MOSAIC_LINEAR:
			MOSleftfetchjoin_linear(cntxt, task, first, first + task->blk->cnt);
			first += task->blk->cnt;
			MOSskip_linear(task);
			break;
		case MOSAIC_ZONE:
			MOSleftfetchjoin_zone(cntxt, task, first, first + task->blk->cnt);
			first += task->blk->cnt;
			MOSskip_zone(task);
			break;
		case MOSAIC_NONE:
			MOSleftfetchjoin_none(cntxt, task, first, first + task->blk->cnt);
			first += task->blk->cnt;
			MOSskip_none(task);
			break;
		default:
			assert(0);
		}

	/* adminstrative wrapup of the leftfetchjoin */
	BBPreleaseref(*lid);
	BBPreleaseref(*rid);

	BATsetcount(bn,cnt);
	bn->hdense = 1;
	bn->hseqbase = 0;
	bn->hkey = 1;
	bn->T->nil = 0;
	bn->T->nonil = 1;
	bn->tsorted = 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = 1;
	BATderiveProps(bn,0);
	BBPkeepref(*ret = bn->batCacheid);
	return msg;
}


str
MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret,*lid,*rid;
	BAT  *bl = NULL, *br = NULL, *bln = NULL, *brn= NULL;
	BUN cnt = 0, first= 0;
	int swapped = 0;
	str msg = MAL_SUCCEED;
	MOStask task;

	(void) cntxt;
	(void) mb;
	ret = (int*) getArgReference(stk,pci,0);
	lid = (int*) getArgReference(stk,pci,2);
	rid = (int*) getArgReference(stk,pci,3);

	if( !isCompressed(*lid) && !isCompressed(*rid))
		return ALGjoin(ret,lid,rid);

	bl = BATdescriptor(*(int*) getArgReference(stk,pci,2));
	if( bl == NULL)
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
	br = BATdescriptor(*(int*) getArgReference(stk,pci,3));
	if( br == NULL){
		BBPreleaseref(bl->batCacheid);
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
	}

	// we assume on compressed argument
	if (bl->T->heap.compressed && br->T->heap.compressed ){
		BBPreleaseref(bl->batCacheid);
		BBPreleaseref(br->batCacheid);
		throw(MAL,"mosaic.join","Join over generator pairs not supported");
    }

	// result set preparation
	bln = BATnew(TYPE_void,TYPE_oid, cnt, TRANSIENT);
	brn = BATnew(TYPE_void,TYPE_oid, cnt, TRANSIENT);
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( bln == NULL || brn == NULL || task == NULL){
		if( bln) BBPreleaseref(bln->batCacheid);
		if( brn) BBPreleaseref(brn->batCacheid);
		BBPreleaseref(bl->batCacheid);
		BBPreleaseref(br->batCacheid);
		throw(MAL,"mosaic.join",MAL_MALLOC_FAIL);
	}

	if ( bl->T->heap.compressed){
		MOSinit(task,bl);
		task->elm = BATcount(br);
		task->src= Tloc(br,BUNfirst(br));
	} else {
		MOSinit(task,br);
		task->elm = BATcount(bl);
		task->src= Tloc(bl,BUNfirst(bl));
		swapped=1;
	}
	task->lbat = bln;
	task->rbat = brn;
	first = MOSfindChunk(cntxt,task,0);
	(void)first;

	// loop thru all the chunks and collect the results
	while(task->blk )
		switch(task->blk->tag){
		case MOSAIC_RLE:
			MOSjoin_rle(cntxt, task, first, first + task->blk->cnt);
			first += (BUN) task->blk->cnt;
			MOSskip_rle(task);
			break;
		case MOSAIC_DICT:
			MOSjoin_dict(cntxt, task, first, first + task->blk->cnt);
			first += (BUN) task->blk->cnt;
			MOSskip_dict(task);
			break;
		case MOSAIC_DELTA:
			MOSjoin_delta(cntxt, task, first, first + task->blk->cnt);
			first += (BUN) task->blk->cnt;
			MOSskip_delta(task);
			break;
		case MOSAIC_LINEAR:
			MOSjoin_linear(cntxt, task, first, first + task->blk->cnt);
			first += (BUN) task->blk->cnt;
			MOSskip_linear(task);
			break;
		case MOSAIC_ZONE:
			MOSjoin_zone(cntxt, task, first, first + task->blk->cnt);
			first += (BUN) task->blk->cnt;
			MOSskip_zone(task);
			break;
		case MOSAIC_NONE:
			MOSjoin_none(cntxt, task, first, first + task->blk->cnt);
			first += (BUN) task->blk->cnt;
			MOSskip_none(task);
			break;
		default:
			assert(0);
		}

    bln->hdense = 1;
    bln->hseqbase = 0;
    bln->hkey = 1;
    bln->tsorted = cnt <= 1;
    bln->trevsorted = cnt <= 1;
    BATderiveProps(bln,0);

    brn->hdense = 1;
    brn->hseqbase = 0;
    brn->hkey = 1;
    brn->tsorted = cnt<= 1;
    brn->trevsorted = cnt <= 1;
    BATderiveProps(brn,0);
    if( swapped){
        BBPkeepref(*(int*)getArgReference(stk,pci,0)= brn->batCacheid);
        BBPkeepref(*(int*)getArgReference(stk,pci,1)= bln->batCacheid);
    } else {
        BBPkeepref(*(int*)getArgReference(stk,pci,0)= bln->batCacheid);
        BBPkeepref(*(int*)getArgReference(stk,pci,1)= brn->batCacheid);
    }
    return msg;
}

// The analyse routine runs through the BAT dictionary and assess
// all possible compression options.

static void
MOSanalyseInternal(Client cntxt, BUN threshold, int bid)
{
	BAT *b;
	int ret;
	str type;

	b = BATdescriptor(bid);
	if( b == NULL ){
		mnstr_printf(cntxt->fdout,"#nonaccessible %d %s\n",bid, BBP_logical(bid));
		return;
	}
	if( BATcount(b) < threshold){
		BBPreleaseref(bid);
		//mnstr_printf(cntxt->fdout,"#too small %d %s\n",bid, BBP_logical(bid));
		return;
	}
	if( isVIEW(b)){
		mnstr_printf(cntxt->fdout,"#ignore view %d %s\n",bid, BBP_logical(bid));
		BBPreleaseref(bid);
		return;
	}
	type = getTypeName(b->ttype);
	switch( b->ttype){
	case TYPE_bit:
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
	case TYPE_oid:
	case TYPE_flt:
	case TYPE_dbl:
		mnstr_printf(cntxt->fdout,"#%d\t%s\t%s\t"BUNFMT"\t%10d ", bid, BBP_logical(bid), type, BATcount(b), ATOMsize(b->ttype) *(int) BATcount(b));
		MOScompressInternal(cntxt, &ret, &bid, 0);
		break;
	default:
		if( b->ttype == TYPE_timestamp){
			mnstr_printf(cntxt->fdout,"#%d\t%s\t%s\t"BUNFMT"\t%10d ", bid, BBP_logical(bid), type, BATcount(b), ATOMsize(b->ttype) *(int) BATcount(b));
			MOScompressInternal(cntxt, &ret, &bid, 0);
		}
	}
	GDKfree(type);
	BBPreleaseref(bid);
}

str
MOSanalyse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,bid;
	BUN threshold= 1000;
	(void) mb;
	

	if( pci->argc > 1)
		threshold = *(BUN*) getArgReference(stk,pci,1);

	if( pci->argc >2 ){
		bid = *(int*) getArgReference(stk,pci,2);
		MOSanalyseInternal(cntxt, threshold,bid);
	} else
    for (i = 1; i < getBBPsize(); i++)
		if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i)) ) 
			MOSanalyseInternal(cntxt, threshold, i);
	return MAL_SUCCEED;
}
