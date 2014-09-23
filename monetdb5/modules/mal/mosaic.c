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
#include "mosaic_literal.h"
#include "mosaic_runlength.h"
#include "mosaic_dictionary.h"
#include "mosaic_zone.h"
#include "mosaic_delta.h"
#include "mosaic_linear.h"
#include "mosaic_variance.h"
#include "mosaic_prefix.h"

static char *filtername[]={"literal","runlength","dictionary","delta","linear","variance","prefix","index","zone","EOL"};

static void
MOSinit(MOStask task, BAT *b){
	char *base;
	if( isVIEW(b))
		b= BATdescriptor(VIEWtparent(b));
	assert(b);
	base = Tloc(b,BUNfirst(b));
	assert(base);
	task->type = b->ttype;
	task->b = b;
	task->hdr = (MosaicHdr) base;
	base += MosaicHdrSize;
	task->blk = (MosaicBlk)  base;
	task->dst = base + MosaicBlkSize;
}

void MOSblk(MosaicBlk blk)
{
	printf("Block tag %d cnt "BUNFMT"\n", MOSgetTag(blk),MOSgetCnt(blk));
}

static void
MOSdumpTask(Client cntxt,MOStask task)
{
	int i;
	dbl perc = task->size/100.0;

	mnstr_printf(cntxt->fdout,"# ");
	mnstr_printf(cntxt->fdout,"clk " LLFMT"\tsizes %10lld\t%10lld\t%3.0f%%\t%10.2fx\t", 
		task->timer,task->size,task->xsize, task->xsize/perc, task->xsize ==0 ? 0:(flt)task->size/task->xsize);
	for ( i=0; i < MOSAIC_METHODS; i++)
	if( task->blks[i])
		mnstr_printf(cntxt->fdout, "%s\t"LLFMT "\t"LLFMT " " LLFMT"\t" , filtername[i], task->blks[i], task->elms[i], task->elms[i]/task->blks[i]);
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
	MOSinitializeScan(cntxt,task,0,task->hdr->top);
	while(task->start< task->stop){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_NONE:
			MOSdump_literal(cntxt,task);
			MOSadvance_literal(cntxt,task);
			break;
		case MOSAIC_RLE:
			MOSdump_runlength(cntxt,task);
			MOSadvance_runlength(cntxt,task);
			break;
		case MOSAIC_DICT:
			MOSdump_dictionary(cntxt,task);
			MOSadvance_dictionary(cntxt,task);
			break;
		case MOSAIC_DELTA:
			MOSdump_delta(cntxt,task);
			MOSadvance_delta(cntxt,task);
			break;
		case MOSAIC_PREFIX:
			MOSdump_prefix(cntxt,task);
			MOSadvance_prefix(cntxt,task);
			break;
		case MOSAIC_VARIANCE:
			MOSdump_variance(cntxt,task);
			MOSadvance_variance(cntxt,task);
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

/*
static BAT*
inheritCOL( BAT *bn, COLrec *cn, BAT *b, COLrec *c, bat p )
{
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
*/

/*
 * Compression is focussed on a single column.
 * Multiple compression techniques are applied at the same time.
 */

str
MOScompressInternal(Client cntxt, int *ret, int *bid, str properties, int inplace, int debug)
{
	BAT *bsrc, *bcompress;
	BUN cutoff =0;
	int i, flg=0;
	str msg = MAL_SUCCEED;
	MOStask task;
	int cand;
	float factor= 1.0, fac= 1.0;
	int filter[MOSAIC_METHODS];
	
	if( properties && !strstr(properties,"compressed"))
		for( i = 0; i< MOSAIC_METHODS; i++){
			filter[i]= strstr(properties,filtername[i]) != 0;
			flg += filter[i];
		}
	else{
		for( i = 0; i< MOSAIC_METHODS; i++)
			filter[i]= 1;
		flg=1;
	}

	if ((bcompress = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mosaic.compress", INTERNAL_BAT_ACCESS);

	switch(ATOMstorage(bcompress->ttype)){
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
	case TYPE_str:
		break;
	default:
		// don't compress them
		BBPkeepref(*ret = bcompress->batCacheid);
		return msg;
	}
	if (flg == 0){
		BBPkeepref(*ret = bcompress->batCacheid);
		return msg;	// don't compress at all
	}

	if (bcompress->T->heap.compressed) {
		BBPkeepref(*ret = bcompress->batCacheid);
		return msg;	// don't compress twice
	}

	if ( isVIEWCOMBINE(bcompress) || BATcount(bcompress) < MIN_INPUT_COUNT ){
		/* no need to compress */
		BBPkeepref(*ret = bcompress->batCacheid);
		return msg;
	}

#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#compress bat %d properties %s\n",*bid,properties?properties:"");
#endif
	// allocate space for a compressed version.
	// It should always take less space then the orginal column.
	// But be prepared that a header and last block header may  be stored
	// use a size overshoot. Also be aware of possible dictionary headers
	//if (inplace)
		bsrc = BATcopy(bcompress, bcompress->htype, bcompress->ttype, TRUE,TRANSIENT);
	if( !inplace)
		bsrc = BATextend(bsrc, BATgrows(bsrc)+MosaicHdrSize);
	else
		bcompress = BATextend(bcompress, BATgrows(bcompress)+MosaicHdrSize);

	if (bsrc == NULL) {
		BBPreleaseref(bcompress->batCacheid);
		throw(MAL,"mosaic.compress", MAL_MALLOC_FAIL);
	}
	BATseqbase(bsrc,bcompress->hseqbase);

	// actual compression mosaic task
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(bsrc->batCacheid);
		BBPreleaseref(bcompress->batCacheid);
		throw(MAL, "mosaic.compress", MAL_MALLOC_FAIL);
	}

	if( inplace){
		// initialize in place compression
		bcompress = BATextend(bcompress, BATgrows(bcompress)+MosaicHdrSize);
		if( bcompress == NULL){
			BBPreleaseref(bsrc->batCacheid);
			throw(MAL,"mosaic.compress", MAL_MALLOC_FAIL);
		}
		// initialize the non-compressed read pointer
		task->src = Tloc(bsrc, BUNfirst(bsrc));
		task->elm = BATcount(bsrc);
		task->size = bsrc->T->heap.free;
		task->timer = GDKusec();

		MOSinit(task,bcompress);
		MOSinitHeader(task);

		// claim the server for exclusive use
		msg = MCstartMaintenance(cntxt,1,0);
		if( msg != MAL_SUCCEED){
			GDKfree(task);
			BBPreleaseref(bsrc->batCacheid);
			BBPreleaseref(bcompress->batCacheid);
			throw(MAL, "mosaic.compress", "Can not claim server");
		}
	} else {
		// initialize local compressed copy
		task->src = Tloc(bcompress, BUNfirst(bcompress));
		task->elm = BATcount(bcompress);
		task->size = bcompress->T->heap.free;
		task->timer = GDKusec();

		MOSinit(task,bsrc);
		MOSinitHeader(task);
	}
	MOScreatedictionary(cntxt,task);
	// always start with an EOL block
	MOSsetTag(task->blk,MOSAIC_EOL);

	cutoff = task->elm > 1000? task->elm - 1000: task->elm;
	while(task->elm > 0){
		// default is to extend the non-compressed block
		cand = MOSAIC_NONE;
		fac = 1.0;
		factor = 1.0;

		// cutoff the filters, especially dictionary tests are expensive
		if( cutoff && cutoff > task->elm){
			if( task->blks[MOSAIC_PREFIX] == 0)
				filter[MOSAIC_PREFIX] = 0;
			if( task->blks[MOSAIC_DICT] == 0)
				filter[MOSAIC_DICT] = 0;
			cutoff = 0;
		}
		
		// select candidate amongst those
		if ( filter[MOSAIC_RLE]){
			fac = MOSestimate_runlength(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_RLE;
				factor = fac;
			}
		}
		if ( filter[MOSAIC_DICT]){
			fac = MOSestimate_dictionary(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_DICT;
				factor = fac;
			}
		}
/*
		if ( filter[MOSAIC_VARIANCE]){
			fac = MOSestimate_variance(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_VARIANCE;
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
*/
		if ( filter[MOSAIC_DELTA]){
			fac = MOSestimate_delta(cntxt,task);
			if ( fac > factor ){
				cand = MOSAIC_DELTA;
				factor = fac;
			}
		}
		if ( filter[MOSAIC_PREFIX]){
			fac = MOSestimate_prefix(cntxt,task);
			if ( fac > factor ){
				cand = MOSAIC_PREFIX;
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
		case MOSAIC_PREFIX:
			// close the non-compressed part
			if( (MOSgetTag(task->blk) == MOSAIC_NONE || MOSgetTag(task->blk) == MOSAIC_ZONE) && MOSgetCnt(task->blk) ){
				MOSupdateHeader(cntxt,task);
				if( MOSgetTag(task->blk) == MOSAIC_NONE)
					MOSadvance_literal(cntxt,task);
				else
					MOSadvance_zone(cntxt,task);
				// always start with an EOL block
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				MOSsetTag(task->blk,MOSAIC_EOL);
			}
			break;
		case MOSAIC_NONE:
		case MOSAIC_ZONE:
			if ( MOSgetCnt(task->blk) == MOSlimit()){
				MOSupdateHeader(cntxt,task);
				if( MOSgetTag(task->blk) == MOSAIC_NONE)
					MOSadvance_literal(cntxt,task);
				else
					MOSadvance_zone(cntxt,task);
				// always start with an EOL block
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				MOSsetTag(task->blk,MOSAIC_EOL);
			}
		}
		// apply the compression to a chunk
		switch(cand){
		case MOSAIC_DICT:
			MOScompress_dictionary(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOSgetCnt(task->blk);
			MOSadvance_dictionary(cntxt,task);
			MOSsetTag(task->blk,MOSAIC_EOL);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_VARIANCE:
			MOScompress_variance(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOSgetCnt(task->blk);
			MOSadvance_variance(cntxt,task);
			MOSsetTag(task->blk,MOSAIC_EOL);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_DELTA:
			MOScompress_delta(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOSgetCnt(task->blk);
			MOSadvance_delta(cntxt,task);
			MOSsetTag(task->blk,MOSAIC_EOL);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_PREFIX:
			MOScompress_prefix(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOSgetCnt(task->blk);
			MOSadvance_prefix(cntxt,task);
			MOSsetTag(task->blk,MOSAIC_EOL);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_LINEAR:
			MOScompress_linear(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOSgetCnt(task->blk);
			MOSadvance_linear(cntxt,task);
			MOSsetTag(task->blk,MOSAIC_EOL);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_RLE:
			MOScompress_runlength(cntxt,task);
			MOSupdateHeader(cntxt,task);
			//prepare new block header
			task->elm -= MOSgetCnt(task->blk);
			MOSadvance_runlength(cntxt,task);
			MOSsetTag(task->blk,MOSAIC_EOL);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
			break;
		case MOSAIC_ZONE:
			if( MOSgetCnt(task->blk) == 0)
				task->dst += 2 * MosaicBlkSize;
			if ( MOSgetCnt(task->blk) > MAXZONESIZE){
				MOSupdateHeader(cntxt,task);
				MOSadvance_zone(cntxt,task);
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				MOSsetTag(task->blk,MOSAIC_EOL);
			}
			MOScompress_zone(cntxt,task);
			break;
		default :
			// continue to use the last block header.
			MOScompress_literal(cntxt,task);
		}
	}
	if( (MOSgetTag(task->blk) == MOSAIC_NONE || MOSgetTag(task->blk) == MOSAIC_ZONE) && MOSgetCnt(task->blk)){
		MOSupdateHeader(cntxt,task);
		if( MOSgetTag(task->blk) == MOSAIC_NONE ){
			MOSadvance_literal(cntxt,task);
			task->dst = ((char*) task->blk)+ MosaicBlkSize;
		} else{
			MOSadvance_zone(cntxt,task);
			task->dst = ((char*) task->blk)+ 3 * MosaicBlkSize;
		}
		MOSsetTag(task->blk,MOSAIC_EOL);
	} else
		task->dst = ((char*) task->blk)+ MosaicBlkSize;
	task->xsize = (task->dst - (char*)task->hdr) + MosaicHdrSize;
	task->timer = GDKusec() - task->timer;
	if(debug) MOSdumpTask(cntxt,task);
	// if we couldnt compress well enough, ignore the result
	// TODO

	if( inplace){
		bcompress->batDirty = 1;
		bcompress->T->heap.free = (size_t) (task->dst - Tloc(bcompress,BUNfirst(bcompress)) );
		bcompress->T->heap.compressed= 1;
		MCexitMaintenance(cntxt);
		BBPkeepref(*ret = bcompress->batCacheid);
		BBPreleaseref(bsrc->batCacheid);
	} else {
		BATsetcount(bsrc,BATcount(bcompress));
		bsrc->batDirty = 1;
		bsrc->T->heap.free = (size_t) (task->dst - Tloc(bsrc,BUNfirst(bsrc)) );
		bsrc->T->heap.compressed= 1;
		BBPkeepref(*ret = bsrc->batCacheid);
		BBPreleaseref(bcompress->batCacheid);
	}
	GDKfree(task);
#ifdef _DEBUG_MOSAIC_
	MOSdumpInternal(cntxt,bcompress);
#endif
	return msg;
}

str
MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	str prop = NULL;
#ifdef _DEBUG_MOSAIC_
	int flg = 1;
#else
	int flg = 0;
#endif
	(void) mb;
	if( pci->argc == 3)
		prop = *(str*) getArgReference(stk,pci,2);
	return MOScompressInternal(cntxt, (int*) getArgReference(stk,pci,0), (int*) getArgReference(stk,pci,1), prop, 0, flg);
}

str
MOScompressStorage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	str prop = NULL;
#ifdef _DEBUG_MOSAIC_
	int flg = 1;
#else
	int flg = 0;
#endif
	(void) mb;
	if( pci->argc == 3)
		prop = *(str*) getArgReference(stk,pci,2);
	return MOScompressInternal(cntxt, (int*) getArgReference(stk,pci,0), (int*) getArgReference(stk,pci,1), prop, 1, flg);
}

// bulk decompress a heap
str
MOSdecompressInternal(Client cntxt, int *ret, int *bid, int inplace)
{	
	BAT *bsrc, *b;
	MOStask task;
	str msg;
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

	bsrc = BATnew(b->htype,b->ttype,BATcount(b)+ MosaicHdrSize,TRANSIENT);
	if ( bsrc == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "mosaic.decompress", MAL_MALLOC_FAIL);
	}
	BATseqbase(bsrc,b->hseqbase);

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bsrc->batCacheid);
		throw(MAL, "mosaic.decompress", MAL_MALLOC_FAIL);
	}
	if(inplace) {
		//copy the compressed heap to its temporary location
		 memcpy(bsrc->T->heap.base, b->T->heap.base, b->T->heap.free);
	
		// claim the server for exclusive use
		msg = MCstartMaintenance(cntxt,1,0);
		if( msg != MAL_SUCCEED){
			GDKfree(msg);
			BBPreleaseref(b->batCacheid);
			BBPreleaseref(bsrc->batCacheid);
			throw(MAL, "mosaic.decompress", "Can not claim server");
		}

		// Now bsrc should be securely saved to avoid database loss 
		// during the compressions step. TOBEDONE
		
		MOSinit(task,bsrc);
		task->src = Tloc(b, BUNfirst(b));
		task->timer = GDKusec();
	} else { 
		// create a local decompressed copy
		MOSinit(task,b);
		task->src = Tloc(bsrc, BUNfirst(bsrc));
		task->timer = GDKusec();
	} 

	while(task->blk){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_DICT:
			MOSdecompress_dictionary(cntxt,task);
			MOSskip_dictionary(cntxt,task);
			break;
		case MOSAIC_VARIANCE:
			MOSdecompress_variance(cntxt,task);
			MOSskip_variance(cntxt,task);
			break;
		case MOSAIC_DELTA:
			MOSdecompress_delta(cntxt,task);
			MOSskip_delta(cntxt,task);
			break;
		case MOSAIC_PREFIX:
			MOSdecompress_prefix(cntxt,task);
			MOSskip_prefix(cntxt,task);
			break;
		case MOSAIC_LINEAR:
			MOSdecompress_linear(cntxt,task);
			MOSskip_linear(cntxt,task);
			break;
		case MOSAIC_NONE:
			MOSdecompress_literal(cntxt,task);
			MOSskip_literal(cntxt,task);
			break;
		case MOSAIC_RLE:
			MOSdecompress_runlength(cntxt,task);
			MOSskip_runlength(cntxt,task);
			break;
		case MOSAIC_ZONE:
			MOSdecompress_zone(cntxt,task);
			MOSskip_zone(cntxt,task);
			break;
		default: assert(0);
		}
	}

	// continue with all work
	if(inplace) {
		b->batDirty = 1;
		b->T->heap.free = (size_t) (BATcount(b) * b->T->width);
		b->T->heap.compressed= 0;

		MCexitMaintenance(cntxt);
		BBPreleaseref(bsrc->batCacheid);
		BBPkeepref( *ret = b->batCacheid);
	} else {
		BATsetcount(bsrc,BATcount(b));
		bsrc->batDirty = 1;
		bsrc->T->heap.free = (size_t) (BATcount(b) * b->T->width);
		bsrc->T->heap.compressed= 0;
        bsrc->hdense = 1;
        bsrc->hseqbase = 0;
        bsrc->hkey = 1;
        BATderiveProps(bsrc,0);

		BBPreleaseref(b->batCacheid);
		BBPkeepref( *ret = bsrc->batCacheid);
	}
	GDKfree(task);

	//if (!b->T->heap.compressed && b->ttype != TYPE_void) {
		/* inherit original uncompressed tail as view */
		//bn = inheritCOL( bn, bn->T, b, b->T, VIEWtparent(b) );
	//}
	return MAL_SUCCEED;
}

str
MOSdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) mb;
	return MOSdecompressInternal(cntxt, (int*) getArgReference(stk,pci,0), (int*) getArgReference(stk,pci,1),0);
}

str
MOSdecompressStorage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) mb;
	return MOSdecompressInternal(cntxt, (int*) getArgReference(stk,pci,0), (int*) getArgReference(stk,pci,1),1);
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
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			MOSsubselect_runlength(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DICT:
			MOSsubselect_dictionary(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_VARIANCE:
			MOSsubselect_variance(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DELTA:
			MOSsubselect_delta(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_PREFIX:
			MOSsubselect_prefix(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_LINEAR:
			MOSsubselect_linear(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_ZONE:
			MOSsubselect_zone(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_NONE:
		default:
			MOSsubselect_literal(cntxt,task,low,hgh,li,hi,anti);
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
	bn = BATnew(TYPE_void, TYPE_oid, BATcount(b), TRANSIENT);
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
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			MOSthetasubselect_runlength(cntxt,task,low,*oper);
			break;
		case MOSAIC_DELTA:
			MOSthetasubselect_delta(cntxt,task,low,*oper);
			break;
		case MOSAIC_PREFIX:
			MOSthetasubselect_prefix(cntxt,task,low,*oper);
			break;
		case MOSAIC_LINEAR:
			MOSthetasubselect_linear(cntxt,task,low,*oper);
			break;
		case MOSAIC_DICT:
			MOSthetasubselect_dictionary(cntxt,task,low,*oper);
			break;
		case MOSAIC_VARIANCE:
			MOSthetasubselect_variance(cntxt,task,low,*oper);
			break;
		case MOSAIC_ZONE:
			MOSthetasubselect_zone(cntxt,task,low,*oper);
			break;
		case MOSAIC_NONE:
		default:
			MOSthetasubselect_literal(cntxt,task,low,*oper);
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
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			MOSleftfetchjoin_runlength(cntxt, task);
			break;
		case MOSAIC_DICT:
			MOSleftfetchjoin_dictionary(cntxt, task);
			break;
		case MOSAIC_VARIANCE:
			MOSleftfetchjoin_variance(cntxt, task);
			break;
		case MOSAIC_DELTA:
			MOSleftfetchjoin_delta(cntxt, task);
			break;
		case MOSAIC_PREFIX:
			MOSleftfetchjoin_prefix(cntxt, task);
			break;
		case MOSAIC_LINEAR:
			MOSleftfetchjoin_linear(cntxt, task);
			break;
		case MOSAIC_ZONE:
			MOSleftfetchjoin_zone(cntxt, task);
			break;
		case MOSAIC_NONE:
			MOSleftfetchjoin_literal(cntxt, task);
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
	int *ret, *ret2,*lid,*rid;
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
	ret2 = (int*) getArgReference(stk,pci,1);
	lid = (int*) getArgReference(stk,pci,2);
	rid = (int*) getArgReference(stk,pci,3);

	if( !isCompressed(*lid) && !isCompressed(*rid))
		return ALGjoin2(ret,ret2,lid,rid);

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
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			MOSjoin_runlength(cntxt, task);
			break;
		case MOSAIC_DICT:
			MOSjoin_dictionary(cntxt, task);
			break;
		case MOSAIC_VARIANCE:
			MOSjoin_variance(cntxt, task);
			break;
		case MOSAIC_DELTA:
			MOSjoin_delta(cntxt, task);
			break;
		case MOSAIC_PREFIX:
			MOSjoin_prefix(cntxt, task);
			break;
		case MOSAIC_LINEAR:
			MOSjoin_linear(cntxt, task);
			break;
		case MOSAIC_ZONE:
			MOSjoin_zone(cntxt, task);
			break;
		case MOSAIC_NONE:
			MOSjoin_literal(cntxt, task);
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
MOSanalyseInternal(Client cntxt, int threshold, str properties, int bid)
{
	BAT *b,*bn, *br=0;
	int ret = 0, bid2 = 0;
	str type;
	(void) br;

	b = BATdescriptor(bid);
	if( b == NULL ){
		mnstr_printf(cntxt->fdout,"#nonaccessible %d\n",bid);
		return;
	}
	if( b->ttype == TYPE_void ||  BATcount(b) < (BUN) threshold){
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
	bn= BATcopy(b,b->htype,b->ttype,TRUE,TRANSIENT);
	if( bn == NULL)
		return;
	bid2= bn->batCacheid;
	type = getTypeName(b->ttype);
	switch( b->ttype){
	case TYPE_bit:
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
	case TYPE_wrd:
	case TYPE_oid:
	case TYPE_flt:
	case TYPE_dbl:
	case TYPE_str:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
		mnstr_printf(cntxt->fdout,"#%-15s%d\t%-8s\t%s\t"BUNFMT"\t", properties, bid, BBP_physical(bid), type, BATcount(b));
		MOScompressInternal(cntxt, &ret, &bid2, properties,0,1);
		br = BATdescriptor(ret);
		if(br) BBPreclaim(br);
		break;
	default:
		if( b->ttype == TYPE_timestamp || b->ttype == TYPE_date || b->ttype == TYPE_daytime){
			mnstr_printf(cntxt->fdout,"#%-15s%d\t%-8s\t%s\t"BUNFMT"\t", properties, bid, BBP_physical(bid), type, BATcount(b));
			MOScompressInternal(cntxt, &ret, &bid2, properties,0,1);
			br = BATdescriptor(ret);
			if(br) BBPreclaim(br);
		} else
			mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t illegal compression type %s\n", bid, BBP_logical(bid), type, BATcount(b), getTypeName(b->ttype));
	}
	GDKfree(type);
	BBPreleaseref(bid);
	BBPreleaseref(bid2);
}

str
MOSanalyse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,j,limit,bid;
	int threshold= 1000;
	str properties[32] ={0};
	int top=0;
	char *c;
	(void) mb;
	

	if( pci->argc > 1){
		if( getArgType(mb,pci,1) == TYPE_lng)
			threshold = *(int*) getArgReference(stk,pci,1);
		if( getArgType(mb,pci,1) == TYPE_str){
			c= properties[0] = *(str*) getArgReference(stk,pci,1);
			top++;
			while ( (c=strchr(c,';'))  && top <32){
				*c++ = 0;
				properties[top++] = c;
			}
		}
	}
	if( top == 0) { properties[0]="compressed"; top++;}

	if( pci->argc >2 ){
		bid = *(int*) getArgReference(stk,pci,2);
		for( j = 0; j < top; j++)
			MOSanalyseInternal(cntxt, threshold, properties[j], bid);
	} else
    for (limit= getBBPsize(),i = 1; i < limit; i++){
		if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i)) ) 
			for( j = 0; j < top; j++)
				MOSanalyseInternal(cntxt, threshold, properties[j], i);
		if( top > 1)
			mnstr_printf(cntxt->fdout,"#\n");
	}
	return MAL_SUCCEED;
}
