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
	char *base;
	if( isVIEW(b))
		b= BATdescriptor(VIEWtparent(b));
	assert(b);
	base = Tloc(b,BUNfirst(b));
	assert(base);
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
	dbl perc = task->size/100.0;

	mnstr_printf(cntxt->fdout,"# ");
	mnstr_printf(cntxt->fdout,"clk " LLFMT"\tsizes "LLFMT"\t"LLFMT "\t%3.0f%%\t%10.2fx\t", 
		task->timer,task->size,task->xsize, task->xsize/perc, task->xsize ==0 ? 0:(flt)task->size/task->xsize);
	for ( i=0; i < MOSAIC_METHODS; i++)
	if( task->blks[i])
		mnstr_printf(cntxt->fdout, "%s\t"LLFMT "\t"LLFMT " " LLFMT"\t" , filtername[i], task->blks[i], task->elms[i], task->elms[i]/task->blks[i]);
}

// dump a compressed BAT
static void
MOSdumpInternal(Client cntxt, BAT *b){
	MOStask task=0;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		return;
	MOSinit(task,b);
	MOSinitializeScan(cntxt,task,0,task->hdr->top);
	while(task->start< task->stop){
		switch(MOStag(task->blk)){
		case MOSAIC_NONE:
			MOSdump_none(cntxt,task);
			MOSadvance_none(cntxt,task);
			break;
		case MOSAIC_RLE:
			MOSdump_rle(cntxt,task);
			MOSadvance_rle(cntxt,task);
			break;
		case MOSAIC_DICT:
			MOSdump_dict(cntxt,task);
			MOSadvance_dict(cntxt,task);
			break;
		case MOSAIC_DELTA:
			MOSdump_delta(cntxt,task);
			MOSadvance_delta(cntxt,task);
			break;
		case MOSAIC_ZONE:
			MOSdump_zone(cntxt,task);
			MOSadvance_zone(cntxt,task);
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
	BUN cnt, cutoff =0;
	int i;
	char *c;
	str msg = MAL_SUCCEED;
	MOStask task;
	int cand;
	float factor= 1.0, fac= 1.0;
	int filter[MOSAIC_METHODS];
	
	if( properties && !strstr(properties,"compressed"))
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

	switch(ATOMstorage(b->ttype)){
	case TYPE_bit:
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_oid:
	case TYPE_wrd:
	case TYPE_flt:
	case TYPE_dbl:
		break;
	default:
		// don't compress them
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
	bn = BATnew( TYPE_void, b->ttype, cnt + 3 *  MosaicBlkSize + MosaicHdrSize, b->batPersistence);
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
	task->size = b->T->heap.free;
	task->timer = GDKusec();

	// prepare a compressed heap
	MOSinit(task,bn);
	MOSinitHeader(task);

	// always start with an EOL block
	*task->blk = MOSeol;

	cutoff = task->elm > 1000? task->elm - 1000: task->elm;
	while(task->elm > 0){
		// default is to extend the non-compressed block
		//mnstr_printf(cntxt->fdout,"#elements "BUNFMT"\n",task->elm);
		cand = MOSAIC_NONE;
		fac = 1.0;
		factor = 1.0;

		// cutoff the filters, especially dictionary tests are expensive
		if( cutoff && cutoff > task->elm){
			if( task->blks[MOSAIC_DICT] == 0)
				filter[MOSAIC_DICT] = 0;
			cutoff = 0;
		}
		
		// select candidate amongst those
		if ( filter[MOSAIC_RLE]){
			fac = MOSestimate_rle(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_RLE;
				factor = fac;
			}
		}
		if ( filter[MOSAIC_DICT]){
			fac = MOSestimate_dict(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_DICT;
				factor = fac;
			}
		}
		if ( filter[MOSAIC_ZONE]){
			fac = MOSestimate_zone(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_ZONE;
				factor = fac;
			}
		}
		if ( filter[MOSAIC_DELTA]){
			fac = MOSestimate_delta(cntxt,task);
			if ( fac > factor ){
				cand = MOSAIC_DELTA;
				factor = fac;
			}
		}
		if ( filter[MOSAIC_LINEAR]){
			fac = MOSestimate_linear(cntxt,task);
			if ( fac >factor){
				cand = MOSAIC_LINEAR;
				factor = fac;
			}
		}

		// wrapup previous block
		switch(cand){
		case MOSAIC_RLE:
		case MOSAIC_DICT:
		case MOSAIC_DELTA:
		case MOSAIC_LINEAR:
			// close the non-compressed part
			if( (MOStag(task->blk) == MOSAIC_NONE || MOStag(task->blk) == MOSAIC_ZONE) && MOScnt(task->blk) ){
				MOSupdateHeader(cntxt,task);
				if( MOStag(task->blk) == MOSAIC_NONE)
					MOSskip_none(cntxt,task);
				else
					MOSskip_zone(cntxt,task);
				// always start with an EOL block
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				*task->blk = MOSeol;
			}
			break;
		case MOSAIC_NONE:
		case MOSAIC_ZONE:
			if ( MOScnt(task->blk) == MOSlimit()){
				MOSupdateHeader(cntxt,task);
				if( MOStag(task->blk) == MOSAIC_NONE)
					MOSskip_none(cntxt,task);
				else
					MOSskip_zone(cntxt,task);
				// always start with an EOL block
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				*task->blk = MOSeol;
			}
		}
		// apply the compression to a chunk
		switch(cand){
		case MOSAIC_DICT:
			MOScompress_dict(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOScnt(task->blk);
			MOSadvance_dict(cntxt,task);
			*task->blk = MOSeol;
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_DELTA:
			MOScompress_delta(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOScnt(task->blk);
			MOSadvance_delta(cntxt,task);
			*task->blk = MOSeol;
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_LINEAR:
			MOScompress_linear(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOScnt(task->blk);
			MOSadvance_linear(cntxt,task);
			*task->blk= MOSeol;
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_RLE:
			MOScompress_rle(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOScnt(task->blk);
			MOSadvance_rle(cntxt,task);
			*task->blk = MOSeol;
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_ZONE:
			if( MOScnt(task->blk) == 0)
				task->dst += 2 * MosaicBlkSize;
			if ( MOScnt(task->blk) > MAXZONESIZE){
				MOSupdateHeader(cntxt,task);
				MOSadvance_zone(cntxt,task);
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				*task->blk = MOSeol;
			}
			MOScompress_zone(cntxt,task);
			break;
		default :
			// continue to use the last block header.
			MOScompress_none(cntxt,task);
		}
	}
	if( (MOStag(task->blk) == MOSAIC_NONE || MOStag(task->blk) == MOSAIC_ZONE) && MOScnt(task->blk)){
		MOSupdateHeader(cntxt,task);
		if( MOStag(task->blk) == MOSAIC_NONE ){
			MOSadvance_none(cntxt,task);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
		} else{
			MOSadvance_zone(cntxt,task);
			task->dst = ((char*) task->blk)+ 3 * MosaicBlkSize;
		}
		*task->blk = MOSeol;
	}
	task->xsize = ((lng)task->dst - (lng)task->hdr) + (lng)MosaicHdrSize;
	task->timer = GDKusec() - task->timer;
//#ifdef _DEBUG_MOSAIC_
	MOSdumpTask(cntxt,task);
	mnstr_printf(cntxt->fdout,"\n");
//#endif
	// if we couldnt compress well enough, ignore the result
/*
	if( task->xsize && task->size / task->xsize < 1){
		GDKfree(task);
		BBPreleaseref(bn->batCacheid);
		BBPkeepref(*ret = b->batCacheid);
		return MAL_SUCCEED;
	}
*/

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
	task->timer = GDKusec();
	while(task->blk){
		switch(MOStag(task->blk)){
		case MOSAIC_DICT:
			MOSdecompress_dict(cntxt,task);
			MOSskip_dict(cntxt,task);
			break;
		case MOSAIC_DELTA:
			MOSdecompress_delta(cntxt,task);
			MOSskip_delta(cntxt,task);
			break;
		case MOSAIC_LINEAR:
			MOSdecompress_linear(cntxt,task);
			MOSskip_linear(cntxt,task);
			break;
		case MOSAIC_NONE:
			MOSdecompress_none(cntxt,task);
			MOSskip_none(cntxt,task);
			break;
		case MOSAIC_RLE:
			MOSdecompress_rle(cntxt,task);
			MOSskip_rle(cntxt,task);
			break;
		case MOSAIC_ZONE:
			MOSdecompress_zone(cntxt,task);
			MOSskip_zone(cntxt,task);
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
	task->timer = GDKusec()- task->timer;
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

// locate the corresponding bind operation to determine the partition
static void
MOSgetPartition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int varid, int *part, int *nrofparts )
{
	int i;
	InstrPtr p;

	*part = 0;
	*nrofparts = 1;
	for( i = 1; i< mb->stop; i++){
		p= getInstrPtr(mb,i);
		if( getModuleId(p)== sqlRef && getFunctionId(p) == bindRef && getArg(p,0) == varid){
			*part = getVarConstant(mb,getArg(p,6)).val.ival;
			*nrofparts = getVarConstant(mb,getArg(p,7)).val.ival;
		} else
		if( p->token == ASSIGNsymbol && getArg(p,1) == varid)
			MOSgetPartition(cntxt,mb,stk,getArg(p,0),part,nrofparts);
	}
}

/* The algebra operators should fall back to their default
 * when we know that the heap is not compressed
 * The actual decompression should wait until we know that
 * the administration thru SQL layers works properly.
 *
 * The oid-range can be reduced due to partitioning.
 */
str
MOSsubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *li, *hi, *anti;
	void *low, *hgh;
	int *ret, *bid, *cid= 0;
	int i;
	int startblk,stopblk; // block range to scan
	BUN cnt = 0;
	BAT *b, *bn, *cand = NULL;
	str msg = MAL_SUCCEED;
	MOStask task;
	int part,nrofparts;

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
	if( !isCompressed(*bid)){
		if(cid)
			return ALGsubselect2(ret,bid,cid,low,hgh,li,hi,anti);
		else
			return ALGsubselect1(ret,bid,low,hgh,li,hi,anti);
	}

	b= BATdescriptor(*bid);
	if( b == NULL)
			throw(MAL, "mosaic.subselect",RUNTIME_OBJECT_MISSING);

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
	}

	// accumulator for the oids
	bn = BATnew(TYPE_void, TYPE_oid, BATcount(b), TRANSIENT);
	if( bn == NULL){
		GDKfree(task);
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
			GDKfree(task);
			throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
		}
		task->cl = (oid*) Tloc(cand, BUNfirst(cand));
		task->n = BATcount(cand);
	}

	// determine block range to scan for partitioned columns
	MOSgetPartition(cntxt, mb, stk, getArg(pci,1), &part, &nrofparts );
	if ( nrofparts > 1){
		// don't use more parallelism then entries in the header
		if( nrofparts > task->hdr->top)
			nrofparts = task->hdr->top;
		if( part > nrofparts){
			* (bat *) getArgReference(stk, pci, 0) = bn->batCacheid;
			BBPkeepref(bn->batCacheid);
			GDKfree(task);
			return MAL_SUCCEED;
		}
		startblk = task->hdr->top/nrofparts * part;
		if( part == nrofparts -1)
			stopblk  =  task->hdr->top;
		else
			stopblk  =  startblk + task->hdr->top/nrofparts;
	} else{
		startblk =0;
		stopblk = task->hdr->top;
	}
	// position the scan on the first mosaic block to consider
	MOSinitializeScan(cntxt,task,startblk,stopblk);

	while(task->start < task->stop ){
		switch(MOStag(task->blk)){
		case MOSAIC_RLE:
			MOSsubselect_rle(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DICT:
			MOSsubselect_dict(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DELTA:
			MOSsubselect_delta(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_LINEAR:
			MOSsubselect_linear(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_ZONE:
			MOSsubselect_zone(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_NONE:
		default:
			MOSsubselect_none(cntxt,task,low,hgh,li,hi,anti);
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
	GDKfree(task);
	BBPkeepref(bn->batCacheid);
	return msg;
}


str MOSthetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx, *cid =0,  *ret, *bid;
	int startblk, stopblk; // block range to scan
	int part,nrofparts;
	BAT *b = 0, *cand = 0, *bn = NULL;
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
		cid = (int*) getArgReference(stk,pci, 2);
		idx = 3;
	} else idx = 2;
	low= (void*) getArgReference(stk,pci,idx);
	oper= (char**) getArgReference(stk,pci,idx+1);

	if( !isCompressed(*bid)){
		if( cid)
			return ALGthetasubselect2(ret,bid,cid,low, (const char **)oper);
		else
			return ALGthetasubselect1(ret,bid,low, (const char **)oper);
	}
	
	b = BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	// determine the elements in the compressed structure

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	}

	// accumulator for the oids
	bn = BATnew(TYPE_void, TYPE_oid, b->T->heap.count, TRANSIENT);
	if( bn == NULL){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	}
	BATseqbase(bn,0);
	task->lb = (oid*) Tloc(bn,BUNfirst(bn));

	MOSinit(task,b);
	// drag along the candidate list into the task descriptor
	if (cid) {
		cand = BATdescriptor(*cid);
		if (cand == NULL){
			BBPreleaseref(b->batCacheid);
			BBPreleaseref(bn->batCacheid);
			GDKfree(task);
			throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
		}
		task->cl = (oid*) Tloc(cand, BUNfirst(cand));
		task->n = BATcount(cand);
	}

	// determine block range to scan for partitioned columns
	MOSgetPartition(cntxt, mb, stk, getArg(pci,1), &part, &nrofparts );
	if ( nrofparts > 1){
		// don't use more parallelism then entries in the header
		if( nrofparts > task->hdr->top)
			nrofparts = task->hdr->top;
		if( part > nrofparts){
			BBPkeepref(*(int*)getArgReference(stk,pci,0)= bn->batCacheid);
			GDKfree(task);
			return MAL_SUCCEED;
		}
		startblk = task->hdr->top/nrofparts * part;
		if( part == nrofparts -1)
			stopblk  =  task->hdr->top;
		else
			stopblk  =  startblk + task->hdr->top/nrofparts;
	} else{
		startblk =0;
		stopblk = task->hdr->top;
	}
	// position the scan on the first mosaic block to consider
	MOSinitializeScan(cntxt,task,startblk,stopblk);

	while(task->start < task->stop ){
		switch(MOStag(task->blk)){
		case MOSAIC_RLE:
			MOSthetasubselect_rle(cntxt,task,low,*oper);
			break;
		case MOSAIC_DELTA:
			MOSthetasubselect_delta(cntxt,task,low,*oper);
			break;
		case MOSAIC_LINEAR:
			MOSthetasubselect_linear(cntxt,task,low,*oper);
			break;
		case MOSAIC_DICT:
			MOSthetasubselect_dict(cntxt,task,low,*oper);
			break;
		case MOSAIC_ZONE:
			MOSthetasubselect_zone(cntxt,task,low,*oper);
			break;
		case MOSAIC_NONE:
		default:
			MOSthetasubselect_none(cntxt,task,low,*oper);
		}
	}
	// derive the filling
	cnt = (BUN)( task->lb - (oid*) Tloc(bn,BUNfirst(bn)));
	
	if( cid)
		BBPreleaseref(*cid);
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
	GDKfree(task);
	return msg;
}

str MOSleftfetchjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret, *lid =0, *rid=0;
	int part, nrofparts;
	int startblk,stopblk;
	BAT *bl = NULL, *br = NULL, *bn;
	BUN cnt;
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
	if (isVIEWCOMBINE(br)){
		BBPreleaseref(*rid);
		throw(MAL,"mosaic.leftfetchjoin","compressed view");
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
		GDKfree(task);
		throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
	}
	MOSinit(task,br);
	task->src = (char*) Tloc(bn,BUNfirst(bn));

	task->cl = ol;
	task->n = cnt;

	// determine block range to scan for partitioned columns
	MOSgetPartition(cntxt, mb, stk, getArg(pci,1), &part, &nrofparts );
	if ( nrofparts > 1){
		// don't use more parallelism then entries in the header
		if( nrofparts > task->hdr->top)
			nrofparts = task->hdr->top;
		if( part >= nrofparts){
			BBPreleaseref(*lid);
			BBPreleaseref(*rid);
			BBPkeepref(*ret = bn->batCacheid);
			GDKfree(task);
			return MAL_SUCCEED;
		}
		startblk = task->hdr->top/nrofparts * part;
		if( part < nrofparts )
			stopblk  =  startblk + task->hdr->top/nrofparts;
		else
			stopblk  =  task->hdr->top;
	} else{
		startblk =0;
		stopblk = task->hdr->top;
	}
	assert(startblk < stopblk);
	// position the scan on the first mosaic block to consider
	MOSinitializeScan(cntxt,task,startblk,stopblk);

	// loop thru all the chunks and fetch all results
	while(task->start<task->stop )
		switch(MOStag(task->blk)){
		case MOSAIC_RLE:
			MOSleftfetchjoin_rle(cntxt, task);
			break;
		case MOSAIC_DICT:
			MOSleftfetchjoin_dict(cntxt, task);
			break;
		case MOSAIC_DELTA:
			MOSleftfetchjoin_delta(cntxt, task);
			break;
		case MOSAIC_LINEAR:
			MOSleftfetchjoin_linear(cntxt, task);
			break;
		case MOSAIC_ZONE:
			MOSleftfetchjoin_zone(cntxt, task);
			break;
		case MOSAIC_NONE:
			MOSleftfetchjoin_none(cntxt, task);
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
	GDKfree(task);
	return msg;
}


str
MOSjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret,*lid,*rid;
	int part, nrofparts;
	int startblk,stopblk;
	BAT  *bl = NULL, *br = NULL, *bln = NULL, *brn= NULL;
	BUN cnt = 0;
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
	// determine block range to scan for partitioned columns
	MOSgetPartition(cntxt, mb, stk, getArg(pci,1), &part, &nrofparts );
	if ( nrofparts > 1){
		startblk = task->hdr->top/nrofparts * part;
		if( part == nrofparts -1)
			stopblk  =  task->hdr->top;
		else
			stopblk  =  startblk + task->hdr->top/nrofparts;
	} else{
		startblk =0;
		stopblk = task->hdr->top;
	}
	// position the scan on the first mosaic block to consider
	MOSinitializeScan(cntxt,task,startblk,stopblk);

	// loop thru all the chunks and collect the results
	while(task->blk )
		switch(MOStag(task->blk)){
		case MOSAIC_RLE:
			MOSjoin_rle(cntxt, task);
			break;
		case MOSAIC_DICT:
			MOSjoin_dict(cntxt, task);
			break;
		case MOSAIC_DELTA:
			MOSjoin_delta(cntxt, task);
			break;
		case MOSAIC_LINEAR:
			MOSjoin_linear(cntxt, task);
			break;
		case MOSAIC_ZONE:
			MOSjoin_zone(cntxt, task);
			break;
		case MOSAIC_NONE:
			MOSjoin_none(cntxt, task);
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
	GDKfree(task);
    return msg;
}

// The analyse routine runs through the BAT dictionary and assess
// all possible compression options.

static void
MOSanalyseInternal(Client cntxt, BUN threshold, str properties, int bid)
{
	BAT *b;
	int ret;
	str type;

	b = BATdescriptor(bid);
	if( b == NULL ){
		mnstr_printf(cntxt->fdout,"#nonaccessible %d\n",bid);
		return;
	}
	if( b->ttype == TYPE_void ||  BATcount(b) < threshold){
		BBPreleaseref(bid);
		//mnstr_printf(cntxt->fdout,"#too small %d %s\n",bid, BBP_logical(bid));
		return;
	}
	if ( isVIEW(b) || isVIEWCOMBINE(b) || VIEWtparent(b)) {
		mnstr_printf(cntxt->fdout,"#ignore view %d %s\n",bid, BBP_logical(bid));
		BBPreleaseref(bid);
		return;
	}
	if ( BATcount(b) < MIN_INPUT_COUNT ){
		mnstr_printf(cntxt->fdout,"#ignore small %d %s\n",bid, BBP_logical(bid));
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
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_wrd:
	case TYPE_oid:
	case TYPE_flt:
	case TYPE_dbl:
		mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t", bid, BBP_physical(bid), type, BATcount(b));
		MOScompressInternal(cntxt, &ret, &bid, properties);
		if( ret != bid)
			BBPdecref(ret,TRUE);
	case TYPE_str:
		break;
	default:
		if( b->ttype == TYPE_timestamp || b->ttype == TYPE_date || b->ttype == TYPE_daytime){
			mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t", bid, BBP_physical(bid), type, BATcount(b));
			MOScompressInternal(cntxt, &ret, &bid, properties);
			if( ret != bid)
				BBPdecref(ret,TRUE);
		} else
			mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t illegal compression type %s\n", bid, BBP_logical(bid), type, BATcount(b), getTypeName(b->ttype));
	}
	GDKfree(type);
	BBPreleaseref(bid);
}

str
MOSanalyse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,bid;
	BUN threshold= 1000;
	str properties = 0;
	(void) mb;
	

	if( pci->argc > 1){
		if( getArgType(mb,pci,1) == TYPE_lng)
			threshold = *(BUN*) getArgReference(stk,pci,1);
		if( getArgType(mb,pci,1) == TYPE_str)
			properties = *(str*) getArgReference(stk,pci,1);
	}

	if( pci->argc >2 ){
		bid = *(int*) getArgReference(stk,pci,2);
		MOSanalyseInternal(cntxt, threshold, properties, bid);
	} else
    for (i = 1; i < getBBPsize(); i++)
		if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i)) ) 
			MOSanalyseInternal(cntxt, threshold, properties, i);
	return MAL_SUCCEED;
}
