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
*/

#include "monetdb_config.h"
#include "mosaic.h"
#include "mtime.h"
#include "math.h"
#include "opt_prelude.h"
#include "algebra.h"

//#define _DEBUG_MOSAIC_

#define MOSAIC_VERSION 20140808

/* do not invest in compressing BATs smaller than this */
#define MIN_INPUT_COUNT 1

/* The compressor kinds currently hardwired */
#define MOSAIC_METHODS	7
#define MOSAIC_NONE     0		// no compression at all
#define MOSAIC_RLE      1		// use run-length encoding
#define MOSAIC_FRONT    2		// use front compression for >=4 byte fields
#define MOSAIC_DELTA	3		// use delta encoding
#define MOSAIC_BITMAPS  4		// use limited set of bitmaps
#define MOSAIC_RANGE    5		// use linear model
#define MOSAIC_GUASSIAN 6		// use guassian model fitting
#define MOSAIC_EOL		7		// marker for the last block

#define MOSAIC_BITS 48			// maximum number of elements to compress

//Compression should have a significant reduction to apply.
#define COMPRESS_THRESHOLD 50   //percent

/*
 * The header is reserved for meta information, e.g. oid indices.
 * The block header encodes the information needed for the chunk decompressor
 */
#define MOSAICINDEX 4  //> 2 elements
typedef struct MOSAICHEADER{
	int version;
	int top;
	oid index[MOSAICINDEX];
	lng offset[MOSAICINDEX];
} * MosaicHdr;

typedef struct MOSAICBLOCK{
	lng tag:4,		// method applied in chunk
	cnt:MOSAIC_BITS;	// compression specific information
} *MosaicBlk;

#define MosaicHdrSize  sizeof(struct MOSAICHEADER)
#define MosaicBlkSize  sizeof(struct MOSAICBLOCK)

#define wordaligned(SZ) \
	 ((SZ) +  ((SZ) % sizeof(int)? sizeof(int) - ((SZ)%sizeof(int)) : 0))


typedef struct MOSTASK{
	int type;		// one of the permissible types
	MosaicHdr hdr;	// start of the destination heap
	MosaicBlk blk;	// current block header
	char *dst;		// write pointer into current compressed blocks
	BUN	elm;		// elements left to compress
	char *src;		// read pointer into source

	// collect compression statistics for the particular task
	lng time[MOSAIC_METHODS];
	lng wins[MOSAIC_METHODS];	
	int perc[MOSAIC_METHODS]; // compression size for the last batch 0..100 percent
} *MOStask;

/* we keep a condensed OID index anchored to the compressed blocks */

typedef struct MOSINDEX{
	lng offset;		// header location within compressed heap
	lng nullcnt;	// number of nulls encountered
	ValRecord low,hgh; // zone value markers for fix-length types
} *mosaicindex;

/* Run through a column to produce a compressed version */

/* simple include the details of the hardwired compressors */
#include "mosaic_hdr.c"
#include "mosaic_none.c"
#include "mosaic_rle.c"

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
MOSclose(MOStask task){
	if( task->blk->cnt == 0){
		task->dst -= MosaicBlkSize;
		return;
	}
}

static void
MOSdumpTask(Client cntxt,MOStask task)
{
	int i;
	mnstr_printf(cntxt->fdout,"#type %d todo "LLFMT"\n", task->type, (lng)task->elm);
	mnstr_printf(cntxt->fdout,"#wins ");
	for(i=0; i< MOSAIC_METHODS; i++)
		mnstr_printf(cntxt->fdout,LLFMT " ",task->wins[i]);
	mnstr_printf(cntxt->fdout,"\n#time ");
	for(i=0; i< MOSAIC_METHODS; i++)
		mnstr_printf(cntxt->fdout, LLFMT" ",task->time[i]);
	mnstr_printf(cntxt->fdout,"\n#perc ");
	for(i=0; i< MOSAIC_METHODS; i++)
		mnstr_printf(cntxt->fdout, "%d ",task->perc[i]);
	mnstr_printf(cntxt->fdout,"\n");
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
		default: assert(0);
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
MOScompressInternal(Client cntxt, int *ret, int *bid, int threshold)
{
	BAT *b, *bn;
	BUN cnt;
	str msg = MAL_SUCCEED;
	MOStask task;
	int cand;
	lng chunksize=0, ch;

	if( threshold == 0)
		threshold = COMPRESS_THRESHOLD;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mosaic.compress", INTERNAL_BAT_ACCESS);

	if ( b->ttype == TYPE_void){
		// void columns are already compressed
		BBPreleaseref(*ret = b->batCacheid);
		return msg;
	}

	if (b->T->heap.compressed) {
		BBPreleaseref(b->batCacheid);
		return msg;	// don't compress twice
	}

	cnt = BATcount(b);
	if ( isVIEWCOMBINE(b) || cnt < MIN_INPUT_COUNT ){
		/* no need to compress */
		BBPkeepref(*ret = b->batCacheid);
		return msg;
	}

#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#compress bat %d threshold %d\n",*bid,threshold);
#endif
	// allocate space for the compressed version.
	// It should always take less space then the orginal column.
	// But be prepared that a last block header may  be stored
	// use a small size overshoot
	bn = BATnew( TYPE_void, b->ttype, cnt+MosaicBlkSize, TRANSIENT);
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
		cand = MOSAIC_NONE;
		chunksize = 1;
		// collect the opportunities for compression
		ch = MOSestimate_rle(cntxt,task);
		// select candidate amongst those
		if ( ch > chunksize){
		// If it is smaller then the non-compressed version then do that.
			cand = MOSAIC_RLE;
			chunksize = ch;
		}

		// apply the compression to a chunk
		switch(cand){
		case MOSAIC_RLE:
			// close the non-compressed part
			if( task->blk->cnt ){
				MOSupdateHeader(cntxt,task);
				MOSskip_none(task);
				// always start with an EOL block
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				task->blk->tag = MOSAIC_EOL;
				task->blk->cnt = 0;
			}
			MOScompress_rle(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= task->blk->cnt;
			MOSadvance_rle(task);
			task->blk->tag = MOSAIC_EOL;
			task->blk->cnt = 0;
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		default :
			// continue to use the last block header.
			MOScompress_none(cntxt,task);
		}
	}
	if( task->blk->tag == MOSAIC_NONE){
		MOSclose(task);
		MOSupdateHeader(cntxt,task);
		task->blk = (MosaicBlk) task->dst;
		task->blk->tag = MOSAIC_EOL;
		task->blk->cnt = 0;
	}
//#ifdef _DEBUG_MOSAIC_
	MOSdumpTask(cntxt,task);
//#endif
	(void) threshold;

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
	GDKfree(task);
#ifdef _DEBUG_MOSAIC_
	MOSdumpInternal(cntxt,bn);
#endif
	return msg;
}

str
MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	int threshold = COMPRESS_THRESHOLD;
	(void) mb;
	if( pci->argc == 4)
		threshold = *(int*) getArgReference(stk,pci,3);
	return MOScompressInternal(cntxt, (int*) getArgReference(stk,pci,0), (int*) getArgReference(stk,pci,1), threshold);
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
		case MOSAIC_NONE:
			MOSdecompress_none(task);
			MOSskip_none(task);
			break;
		case MOSAIC_RLE:
			MOSdecompress_rle(task);
			MOSskip_rle(task);
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
#define calculate_range(TPE, TPE2)					\
	do {								\
	} while (0)

str
MOSsubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *li, *hi, *anti;
	void *low, *hgh;
	int *ret, *bid, *cid= 0;
	int i;
	oid *cl = 0;
	BAT *b, *bn= NULL, *cand = NULL;
	str msg = MAL_SUCCEED;
	MOStask task;

	(void) cntxt;
	ret = (int *) getArgReference(stk, pci, 0);
	bid = (int *) getArgReference(stk, pci, 1);

	if (pci->argc == 8) {	/* candidate list included */
		cid = (int *) getArgReference(stk, pci, 2);
		i = 3;
	} else
		i = 2;
	low = (void *) getArgReference(stk, pci, i + 2);
	hgh = (void *) getArgReference(stk, pci, i + 3);
	li = (bit *) getArgReference(stk, pci, i + 4);
	hi = (bit *) getArgReference(stk, pci, i + 5);
	anti = (bit *) getArgReference(stk, pci, i + 6);
	// use default implementation if possible
	if( !isCompressed(*bid))
		return ALGsubselect1(ret,bid,low,hgh,li,hi,anti);

	b= BATdescriptor(*bid);
	if( b == NULL)
			throw(MAL, "mosaic.subselect",RUNTIME_OBJECT_MISSING);

	if (cid) {
		cand = BATdescriptor(*cid);
		if (cand == NULL)
			throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
		cl = (oid *) Tloc(cand, BUNfirst(cand));
	}
	(void) cl;
	(void) hi;
	(void) li;
	(void) anti;
	(void) mb;
	// inject the de-decompression
	task= (MOStask) GDKzalloc(sizeof(*task));
	task->type = b->ttype;
	task->elm =  b->T->heap.count;
	task->dst = (void*) Tloc(b,BUNfirst(b));
	task->blk = (MosaicBlk) task->dst;
	task->dst += MosaicHdrSize;
	task->blk = (MosaicBlk) task->dst;
	MOSfindChunk(cntxt,task,0);

	// loop thru all the chunks and collect the results
	while(task->blk )
		switch(task->blk->tag){
		case MOSAIC_RLE:
			MOSskip_rle(task);
			break;
		case MOSAIC_NONE:
		default:
			MOSskip_none(task);
		}
	* (bat *) getArgReference(stk, pci, 0) = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return msg;
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


#define MOSthetasubselect_(TPE,ABS) {\
}


str MOSthetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, cndid =0, c= 0, anti =0,tpe, *ret, *bid;
	BAT *cand = 0, *bn = NULL;
	BUN cap= 0;
	oid *cl = 0;
	str msg= MAL_SUCCEED;
	char **oper;
	void *low;

	(void) cntxt;
	ret= (int*) getArgReference(stk,pci,0);
	bid= (int*) getArgReference(stk,pci,1);
	if( pci->argc == 5){ // candidate list included
		cndid = *(int*) getArgReference(stk,pci, 2);
		idx = 3;
	} else idx = 2;
	low= (void*) getArgReference(stk,pci,idx+1);
	oper= (char**) getArgReference(stk,pci,idx+2);

	if( !isCompressed(*bid))
		return ALGthetasubselect1(ret,bid,low, (const char **)oper);

	(void) cl;
	(void) anti;
	(void) cap;
	if(cndid){
		cand = BATdescriptor(cndid);
		if( cand == NULL)
			throw(MAL,"mosaic.subselect",RUNTIME_OBJECT_MISSING);
		cl = (oid*) Tloc(cand,BUNfirst(cand));\
	}

	// check the step direction
	
	switch( tpe =getArgType(mb,pci,idx)){
	case TYPE_bte: MOSthetasubselect_(bte,abs);break;
	case TYPE_int: MOSthetasubselect_(int,abs);break;
	case TYPE_sht: MOSthetasubselect_(sht,abs);break;
	case TYPE_lng: MOSthetasubselect_(lng,llabs);break;
	case TYPE_flt: MOSthetasubselect_(flt,fabsf);break;
	case TYPE_dbl: MOSthetasubselect_(dbl,fabs);break;
	break;
	default:
		if ( tpe == TYPE_timestamp){
		} else
			throw(MAL,"mosaic.thetasubselect","Illegal generator arguments");
	}

	if( cndid)
		BBPreleaseref(cndid);
	if( bn){
		BATsetcount(bn,c);
		bn->hdense = 1;
		bn->hseqbase = 0;
		bn->hkey = 1;
		BATderiveProps(bn,0);
		BBPkeepref(*(int*)getArgReference(stk,pci,0)= bn->batCacheid);
	}
	return msg;
}

#define MOSleftfetchjoin_(TPE) {\
}

str MOSleftfetchjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret, *bid =0, *cid=0, c= 0, tpe;
	BAT *b, *bn = NULL;
	BUN cnt;
	oid *ol =0, o = 0;
	str msg= MAL_SUCCEED;

	(void) cntxt;

	ret = (int*) getArgReference(stk,pci,0);
	bid = (int*) getArgReference(stk,pci,1);
	cid = (int*) getArgReference(stk,pci,2);

	if( !isCompressed(*cid))
		return ALGleftfetchjoin(ret,bid,cid);

	b = BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL,"mosaic.leftfetchjoin",RUNTIME_OBJECT_MISSING);
	cnt = BATcount(b);
	if ( b->ttype == TYPE_void)
		o = b->tseqbase;
	else
		ol = (oid*) Tloc(b,BUNfirst(b));

	(void) cnt;
	(void) ol;
	(void) o;
	/* the actual code to perform a leftfetchjoin over generators */
	switch( tpe = getArgType(mb,pci,1)){
	case TYPE_bte:  MOSleftfetchjoin_(bte); break;
	case TYPE_sht:  MOSleftfetchjoin_(sht); break;
	case TYPE_int:  MOSleftfetchjoin_(int); break;
	case TYPE_lng:  MOSleftfetchjoin_(lng); break;
	case TYPE_flt:  MOSleftfetchjoin_(flt); break;
	case TYPE_dbl:  MOSleftfetchjoin_(dbl); break;
	default:
		if ( tpe == TYPE_timestamp){
		}
	}

	/* adminstrative wrapup of the leftfetchjoin */
	BBPreleaseref(*bid);
	if( bn){
		BATsetcount(bn,c);
		bn->hdense = 1;
		bn->hseqbase = 0;
		bn->hkey = 1;
		BATderiveProps(bn,0);
		BBPkeepref(*ret = bn->batCacheid);
	}
	return msg;
}

/* The operands of a join operation can either be defined on a generator */
#define MOSjoin_(TPE, ABS) {\
	}

str MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret,*bid,*cid;
	BAT  *b, *bl = NULL, *br = NULL, *bln = NULL, *brn= NULL;
	BUN cnt,c =0;
	oid o= 0, *ol, *or;
	int tpe, incr=0;
	InstrPtr p = NULL, q = NULL;
	str msg = MAL_SUCCEED;

	ret = (int*) getArgReference(stk,pci,0);
	bid = (int*) getArgReference(stk,pci,1);
	cid = (int*) getArgReference(stk,pci,2);

	if( !isCompressed(*bid) && !isCompressed(*cid))
		return ALGjoin(ret,bid,cid);
	(void) cntxt;
	(void) mb;
	// we assume at most one of the arguments to refer to the generator
		bl = BATdescriptor(*(int*) getArgReference(stk,pci,2));
		if( bl == NULL)
			throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
		br = BATdescriptor(*(int*) getArgReference(stk,pci,3));
		if( br == NULL){
			BBPreleaseref(bl->batCacheid);
			throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
		}

	// in case of both generators  || getModuleId(q) == generatorRef)materialize the 'smallest' one first
	// or implement more knowledge, postponed
	assert(!( p && q));
	assert(p || q);

	// switch roles to have a single target bat[:oid,:any] designated
	// by b and reference instruction p for the generator
	b = q? bl : br;
	p = q? q : p;
	cnt = BATcount(b);
	tpe = b->ttype;
	o= b->hseqbase;
	
	bln = BATnew(TYPE_void,TYPE_oid, cnt, TRANSIENT);
	brn = BATnew(TYPE_void,TYPE_oid, cnt, TRANSIENT);
	if( bln == NULL || brn == NULL){
		if(bln) BBPreleaseref(bln->batCacheid);
		if(brn) BBPreleaseref(brn->batCacheid);
		if(bl) BBPreleaseref(bl->batCacheid);
		if(br) BBPreleaseref(br->batCacheid);
		throw(MAL,"mosaic.join",MAL_MALLOC_FAIL);
	}
	ol = (oid*) Tloc(bln,BUNfirst(bln));
	or = (oid*) Tloc(brn,BUNfirst(brn));

	/* The actual join code for generators be injected here */
	switch(tpe){
	case TYPE_bte: //MOSjoin_(bte,abs); break;
	{ bte f,l,s; bte *v; BUN w;
	f = *(bte*) getArgReference(stk,p, 1);
	l = *(bte*) getArgReference(stk,p, 2);
	s = *(bte*) getArgReference(stk,p, 3);
	incr = s > 0;
	if ( s == 0 || (f> l && s>0) || (f<l && s < 0))
		throw(MAL,"mosaic.join","Illegal range");
	v = (bte*) Tloc(b,BUNfirst(b));
	for( ; cnt >0; cnt--,o++,v++){
		w = (BUN) floor(abs(*v -f)/abs(s));
		if ( f + (bte)( w * s) == *v ){
			*ol++ = (oid) w;
			*or++ = o;
			c++;
		}
	} }
	break;
	case TYPE_sht: MOSjoin_(sht,abs); break;
	case TYPE_int: MOSjoin_(int,abs); break;
	case TYPE_lng: MOSjoin_(lng,llabs); break;
	case TYPE_flt: MOSjoin_(flt,fabsf); break;
	case TYPE_dbl: MOSjoin_(dbl,fabs); break;
	default:
		if( tpe == TYPE_timestamp){
			// it is easier to produce the timestamp series
			// then to estimate the possible index
			}
		throw(MAL,"mosaic.join","Illegal type");
	}
    BATsetcount(bln,c);
    bln->hdense = 1;
    bln->hseqbase = 0;
    bln->hkey = 1;
    bln->tsorted = incr || c <= 1;              \
    bln->trevsorted = !incr || c <= 1;          \
    BATderiveProps(bln,0);

    BATsetcount(brn,c);
    brn->hdense = 1;
    brn->hseqbase = 0;
    brn->hkey = 1;
    brn->tsorted = incr || c <= 1;              \
    brn->trevsorted = !incr || c <= 1;          \
    BATderiveProps(brn,0);
    if( q){
        BBPkeepref(*(int*)getArgReference(stk,pci,0)= brn->batCacheid);
        BBPkeepref(*(int*)getArgReference(stk,pci,1)= bln->batCacheid);
    } else {
        BBPkeepref(*(int*)getArgReference(stk,pci,0)= bln->batCacheid);
        BBPkeepref(*(int*)getArgReference(stk,pci,1)= brn->batCacheid);
    }
    return msg;
}
