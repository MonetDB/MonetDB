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
#include "mosaic_delta.h"
#include "mosaic_linear.h"
#include "mosaic_frame.h"
#include "mosaic_prefix.h"

char *MOSfiltername[]={"literal","runlength","dictionary","delta","linear","frame","prefix","EOL"};
BUN MOSblocklimit = 100000;

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
	mnstr_printf(cntxt->fdout,"clk " LLFMT"\tsizes %10lld\t%10lld\t%3.0f%%\t%10.3fx\t", 
		task->timer,task->size,task->xsize, task->xsize/perc, task->xsize ==0 ? 0:(flt)task->size/task->xsize);
	for ( i=0; i < MOSAIC_METHODS; i++)
	if( task->hdr->blks[i])
		mnstr_printf(cntxt->fdout, "%s\t"LLFMT "\t"LLFMT " " LLFMT"\t" , MOSfiltername[i], task->hdr->blks[i], task->hdr->elms[i], task->hdr->elms[i]/task->hdr->blks[i]);
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
		case MOSAIC_FRAME:
			MOSdump_frame(cntxt,task);
			MOSadvance_frame(cntxt,task);
		}
	}
}

str
MOSdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	bat bid = *getArgReference_bat(stk,pci,1);
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

#define MOSnewBlk(TASK)\
			MOSsetTag(TASK->blk,MOSAIC_EOL);\
			TASK->dst = ((char*) TASK->blk)+ MosaicBlkSize;

str
MOScompressInternal(Client cntxt, bat *ret, bat *bid, MOStask task, int inplace, int debug)
{
	BAT *bsrc;		// the source BAT
	BAT *bcompress; // the BAT that will contain the compressed version
	BUN cutoff =0;
	str msg = MAL_SUCCEED;
	int cand;
	float factor= 1.0, fac= 1.0;
	
	*ret = 0;

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
	bsrc = BATcopy(bcompress, bcompress->htype, bcompress->ttype, TRUE,TRANSIENT);

	if (bsrc == NULL) {
		BBPreleaseref(bcompress->batCacheid);
		throw(MAL,"mosaic.compress", MAL_MALLOC_FAIL);
	}
	BATseqbase(bsrc,bcompress->hseqbase);

	if( inplace){
		// It should always take less space than the orginal column.
		// But be prepared that a header and last block header may  be stored
		// use a size overshoot. Also be aware of possible dictionary headers
		if (BATsetaccess(bcompress, BAT_WRITE) == NULL) {
			BBPreleaseref(bsrc->batCacheid);
			BBPreleaseref(bcompress->batCacheid);
			throw(MAL, "mosaic.compress", GDK_EXCEPTION);
		}
		bcompress = BATextend(bcompress, BATgrows(bcompress)+MosaicHdrSize);
		if( bcompress == NULL){
			BBPreleaseref(bsrc->batCacheid);
			throw(MAL,"mosaic.compress", MAL_MALLOC_FAIL);
		}
		// initialize the non-compressed read pointer
		task->src = Tloc(bsrc, BUNfirst(bsrc));
		task->elm = BATcount(bsrc);
		task->start = 0;
		task->stop = BATcount(bsrc);
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
		// It should always take less space than the orginal column.
		// But be prepared that a header and last block header may  be stored
		// use a size overshoot. Also be aware of possible dictionary headers
		// Initialize local compressed copy
		bsrc = BATextend(bsrc, BATgrows(bsrc)+MosaicHdrSize);
		if( bsrc == NULL){
			BBPreleaseref(bcompress->batCacheid);
			throw(MAL,"mosaic.compress", MAL_MALLOC_FAIL);
		}
		task->src = Tloc(bcompress, BUNfirst(bcompress));
		task->elm = BATcount(bcompress);
		task->start = 0;
		task->stop = BATcount(bsrc);
		task->size = bcompress->T->heap.free;
		task->timer = GDKusec();

		MOSinit(task,bsrc);
		MOSinitHeader(task);
	}
	MOScreateframe(cntxt,task);
	MOScreatedictionary(cntxt,task);
	// always start with an EOL block
	MOSsetTag(task->blk,MOSAIC_EOL);

	cutoff = task->elm > 1000? task->elm - 1000: task->elm;
	while(task->start < task->stop ){
		// default is to extend the non-compressed block
		cand = MOSAIC_NONE;
		fac = 1.0;
		factor = 1.0;

		// cutoff the filters, especially dictionary tests are expensive
		if( cutoff && cutoff < task->start){
			if( task->hdr->blks[MOSAIC_PREFIX] == 0)
				task->filter[MOSAIC_PREFIX] = 0;
			if( task->hdr->blks[MOSAIC_DICT] == 0)
				task->filter[MOSAIC_DICT] = 0;
			cutoff = 0;
		}
		
		// select candidate amongst those
		if ( task->filter[MOSAIC_RLE]){
			fac = MOSestimate_runlength(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_RLE;
				factor = fac;
			}
		}
		if ( task->filter[MOSAIC_DICT]){
			fac = MOSestimate_dictionary(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_DICT;
				factor = fac;
			}
		}
		if ( task->filter[MOSAIC_FRAME]){
			fac = MOSestimate_frame(cntxt,task);
			if (fac > factor){
				cand = MOSAIC_FRAME;
				factor = fac;
			}
		}
		if ( task->filter[MOSAIC_DELTA]){
			fac = MOSestimate_delta(cntxt,task);
			if ( fac > factor ){
				cand = MOSAIC_DELTA;
				factor = fac;
			}
		}
		if ( task->filter[MOSAIC_PREFIX]){
			fac = MOSestimate_prefix(cntxt,task);
			if ( fac > factor ){
				cand = MOSAIC_PREFIX;
				factor = fac;
			}
			if ( fac  < 0.0)
					task->filter[MOSAIC_PREFIX] = 0;
		}
		if ( task->filter[MOSAIC_LINEAR]){
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
			if( MOSgetTag(task->blk) == MOSAIC_NONE && MOSgetCnt(task->blk) ){
				task->start -= MOSgetCnt(task->blk);
				MOSupdateHeader(cntxt,task);
				MOSadvance_literal(cntxt,task);
				// always start with an EOL block
				task->dst = ((char*) task->blk)+ MosaicBlkSize;
				MOSsetTag(task->blk,MOSAIC_EOL);
			}
			break;
		case MOSAIC_NONE:
			if ( MOSgetCnt(task->blk) == MOSlimit()){
				task->start -= MOSgetCnt(task->blk);
				MOSupdateHeader(cntxt,task);
				MOSadvance_literal(cntxt,task);
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
			MOSadvance_dictionary(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_FRAME:
			MOScompress_frame(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_frame(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_DELTA:
			MOScompress_delta(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_delta(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_PREFIX:
			MOScompress_prefix(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_prefix(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_LINEAR:
			MOScompress_linear(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_linear(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_RLE:
			MOScompress_runlength(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_runlength(cntxt,task);
			MOSnewBlk(task);
			break;
		default :
			// continue to use the last block header.
			MOScompress_literal(cntxt,task);
			task->start++;
		}
	}
	if( MOSgetTag(task->blk) == MOSAIC_NONE && MOSgetCnt(task->blk)){
		MOSupdateHeader(cntxt,task);
		MOSadvance_literal(cntxt,task);
		task->dst = ((char*) task->blk)+ MosaicBlkSize;
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
		bcompress->T->heap.dirty = 1;
		bcompress->T->heap.free = (size_t) (task->dst - Tloc(bcompress,BUNfirst(bcompress)) );
		bcompress->T->heap.compressed= 1;
		MCexitMaintenance(cntxt);
		BATsetaccess(bcompress, BAT_READ);
		BBPkeepref(*ret = bcompress->batCacheid);
		BBPreleaseref(bsrc->batCacheid);
	} else {
		BATsetcount(bsrc,BATcount(bcompress));
		// retain the stringwidth
		bsrc->T->width = bcompress->T->width ;
		bsrc->T->shift = bcompress->T->shift ;
		bsrc->T->heap.dirty = 1;
		bsrc->T->heap.free = (size_t) (task->dst - Tloc(bsrc,BUNfirst(bsrc)) );
		bsrc->T->heap.compressed= 1;
		BBPkeepref(*ret = bsrc->batCacheid);
		BBPreleaseref(bcompress->batCacheid);
	}
	task->factor = task->hdr->factor = (task->xsize ==0 ? 0:(flt)task->size/task->xsize);
#ifdef _DEBUG_MOSAIC_
	MOSdumpInternal(cntxt,bcompress);
#endif
	return msg;
}

str
MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	str prop = NULL;
	int i;
	MOStask task;

#ifdef _DEBUG_MOSAIC_
	int flg = 1;
#else
	int flg = 0;
#endif
	(void) mb;
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		throw(MAL, "mosaic.compress", MAL_MALLOC_FAIL);

	if( pci->argc == 3)
		prop = *getArgReference_str(stk,pci,2);
	if( prop && !strstr(prop,"mosaic"))
		for( i = 0; i< MOSAIC_METHODS; i++)
			task->filter[i]= strstr(prop,MOSfiltername[i]) != 0;
	else
		for( i = 0; i< MOSAIC_METHODS; i++)
			task->filter[i]= 1;

	prop= MOScompressInternal(cntxt, getArgReference_bat(stk,pci,0), getArgReference_bat(stk,pci,1), task, 0, flg);
	GDKfree(task);
	return prop;
}

// bulk decompress a heap
str
MOSdecompressInternal(Client cntxt, bat *ret, bat *bid, int inplace)
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

	// use a copy to ensure you get the vheap as well
	bsrc = BATcopy(b,b->htype,b->ttype,TRUE, TRANSIENT);
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
		// claim the server for exclusive use
		msg = MCstartMaintenance(cntxt,1,0);
		if( msg != MAL_SUCCEED){
			GDKfree(msg);
			BBPreleaseref(b->batCacheid);
			BBPreleaseref(bsrc->batCacheid);
			throw(MAL, "mosaic.decompress", "Can not claim server");
		}

		//copy the compressed heap to its temporary location
		 memcpy(bsrc->T->heap.base, b->T->heap.base, b->T->heap.free);
		// Now bsrc should be securely saved to avoid database loss 
		// during the compressions step. TOBEDON
	
/*	
{	// Make a backup copy of the heap
		bat bid = abs(bsrc->batCacheid);
		const char *nme, *bnme, *srcdir;
		char filename[IDLENGTH];


		nme = BBP_physical(bid);
		if ((bnme = strrchr(nme, DIR_SEP)) == NULL)
			bnme = nme;
		else
			bnme++; 
		sprintf(filename, "BACKUP%c%s", DIR_SEP, bnme);
		srcdir = GDKfilepath(bsrc->H->heap.farmid, BATDIR, nme, NULL);
		*strrchr(srcdir, DIR_SEP) = 0;

		if (GDKmove(bsrc->T->heap.farmid, srcdir, bnme, "theap", BAKDIR, bnme, "theap") != 0)
			GDKfatal("mosaic.compress: cannot make backup of %s.%s\n", nme, "theap");

		// create a new heap
        bsrc->T->heap.filename = GDKfilepath(-1, NULL, nme, "tail");
        if (bsrc->T->heap.filename == NULL)
            GDKfatal("mosaic.compress: GDKmalloc failed\n");
        if (HEAPalloc(&bsrc->T->heap, bsrc->batCapacity, ATOMsize(bsrc->ttype)) < 0)
            GDKfatal("mosaic.compress: allocating new tail heap for BAT %d failed\n", bid);
        bsrc->T->heap.dirty = TRUE;

		GDKfree(srcdir);
}
*/
		
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
		case MOSAIC_FRAME:
			MOSdecompress_frame(cntxt,task);
			MOSskip_frame(cntxt,task);
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
		bsrc->T->width = b->T->width;
		bsrc->T->shift = b->T->shift;
		bsrc->T->heap.free = (size_t) (BATcount(b) * b->T->width);
		bsrc->T->heap.compressed= 0;
        bsrc->hdense = 1;
        bsrc->hseqbase = 0;
        bsrc->hkey = 1;
        BATderiveProps(bsrc,0);

		BBPreleaseref(b->batCacheid);
		BBPkeepref( *ret = bsrc->batCacheid);
	}
	if( task->hdr->checksum.sumlng != task->hdr->checksum2.sumlng)
		mnstr_printf(cntxt->fdout,"#incompatible compression\n");
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
	return MOSdecompressInternal(cntxt, getArgReference_bat(stk,pci,0), getArgReference_bat(stk,pci,1),0);
}

str
MOSdecompressStorage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) mb;
	return MOSdecompressInternal(cntxt, getArgReference_bat(stk,pci,0), getArgReference_bat(stk,pci,1),1);
}

// The remainders is cloned from the generator code base
// overload the algebra functions to check for compressed heaps.
static int
isCompressed(bat bid)
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
	bat *ret, *bid, *cid= 0;
	int i;
	int startblk,stopblk; // block range to scan
	BUN cnt = 0;
	BAT *b, *bn, *cand = NULL;
	str msg = MAL_SUCCEED;
	MOStask task;
	int part,nrofparts;

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
		// don't use more parallelism than entries in the header
		if( nrofparts > task->hdr->top)
			nrofparts = task->hdr->top;
		if( part > nrofparts){
			*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
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
		case MOSAIC_FRAME:
			MOSsubselect_frame(cntxt,task,low,hgh,li,hi,anti);
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
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	GDKfree(task);
	BBPkeepref(bn->batCacheid);
	return msg;
}


str MOSthetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx;
	bat *cid =0,  *ret, *bid;
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
		// don't use more parallelism than entries in the header
		if( nrofparts > task->hdr->top)
			nrofparts = task->hdr->top;
		if( part > nrofparts){
			BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
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
		case MOSAIC_FRAME:
			MOSthetasubselect_frame(cntxt,task,low,*oper);
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
		BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
	}
	GDKfree(task);
	return msg;
}

str MOSleftfetchjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret, *lid =0, *rid=0;
	int part, nrofparts;
	int startblk,stopblk;
	BAT *bl = NULL, *br = NULL, *bn;
	BUN cnt;
	oid *ol =0, o = 0;
	str msg= MAL_SUCCEED;
	MOStask task;

	(void) mb;
	(void) cntxt;

	ret = getArgReference_bat(stk,pci,0);
	lid = getArgReference_bat(stk,pci,1);
	rid = getArgReference_bat(stk,pci,2);

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
		// don't use more parallelism than entries in the header
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
		case MOSAIC_FRAME:
			MOSleftfetchjoin_frame(cntxt, task);
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
	bat *ret, *ret2,*lid,*rid;
	int part, nrofparts;
	int startblk,stopblk;
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

	if( !isCompressed(*lid) && !isCompressed(*rid))
		return ALGjoin2(ret,ret2,lid,rid);

	bl = BATdescriptor(*lid);
	if( bl == NULL)
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
	br = BATdescriptor(*rid);
	if( br == NULL){
		BBPreleaseref(bl->batCacheid);
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
	}

	// we assume one compressed argument
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
		case MOSAIC_FRAME:
			MOSjoin_frame(cntxt, task);
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

static int
MOSanalyseInternal(Client cntxt, int threshold, MOStask task, bat bid)
{
	BAT *b;
	int ret = 0;
	str type;

	b = BATdescriptor(bid);
	if( b == NULL ){
		mnstr_printf(cntxt->fdout,"#nonaccessible %d\n",bid);
		return 0;
	}
	if( b->ttype == TYPE_void ||  BATcount(b) < (BUN) threshold){
		BBPreleaseref(bid);
		//mnstr_printf(cntxt->fdout,"#too small %d %s\n",bid, BBP_logical(bid));
		return 0;
	}
	if ( isVIEW(b) || isVIEWCOMBINE(b) || VIEWtparent(b)) {
		mnstr_printf(cntxt->fdout,"#ignore view %d %s\n",bid, BBP_logical(bid));
		BBPreleaseref(bid);
		return 0;
	}
	if ( BATcount(b) < MIN_INPUT_COUNT ){
		mnstr_printf(cntxt->fdout,"#ignore small %d %s\n",bid, BBP_logical(bid));
		BBPreleaseref(bid);
		return 0;
	}
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
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_str:
		mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t", bid, BBP_physical(bid), type, BATcount(b));
		MOScompressInternal(cntxt, &ret, &bid, task,0,TRUE);
		if( ret != b->batCacheid) 
			BBPdecref(ret, TRUE);
		break;
	default:
		if( b->ttype == TYPE_timestamp || b->ttype == TYPE_date || b->ttype == TYPE_daytime){
			mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t", bid, BBP_physical(bid), type, BATcount(b));
			MOScompressInternal(cntxt, &ret, &bid, task,0,TRUE);
			if( ret != b->batCacheid)
				BBPdecref(ret, TRUE);
		} else
			mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t illegal compression type %s\n", bid, BBP_logical(bid), type, BATcount(b), getTypeName(b->ttype));
		;
	}
	GDKfree(type);
	BBPreleaseref(bid);
	return 1;

}
/* slice a fixed size atom into thin columns*/
static str
MOSsliceInternal(Client cntxt, bat *slices, BUN size, BAT *b)
{
	BUN i;
	BUN cnt= BATcount(b);
	BAT *bats[8];
	bte *thin[8];
	assert(size < 8);
	(void) cntxt;

	for( i = 0; i< size; i++){
		bats[i] = BATnew(TYPE_void,TYPE_bte, cnt, TRANSIENT);
		if ( bats[i] == NULL){
			for( ;i>0; i--)
				BBPreleaseref(bats[--i]->batCacheid);
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
	return MOSsliceInternal(cntxt, slices, s,b);
}

str
MOSanalyse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,j,k,limit, cand=1;
	bat bid;
	int threshold= 1000;
	str properties[32] ={0};
	float xf[32];
	int top=0,x=0,mx;
	char *c;
	MOStask task;
	(void) mb;
	
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		throw(MAL, "mosaic.compress", MAL_MALLOC_FAIL);

	if(pci->argc > 1 && getArgType(mb,pci,1) == TYPE_str){
		c= properties[0] = *getArgReference_str(stk,pci,1);
		top++;
		while ( (c=strchr(c,';'))  && top <32){
			*c++ = 0;
			properties[top++] = c;
		}
		cand++;
	}
	if( top == 0) { properties[0]="mosaic"; top++;}


	if( pci->argc > cand ){
		bid = *getArgReference_bat(stk,pci,cand);
		mx = 0;
		for( j = 0; j < top; j++){
			if( properties[j] && !strstr(properties[j],"mosaic"))
				for( k = 0; k< MOSAIC_METHODS; k++)
					task->filter[k]= strstr(properties[j],MOSfiltername[k]) != 0;
			else
				for( k = 0; k< MOSAIC_METHODS; k++)
					task->filter[k]= 1;
			x+= MOSanalyseInternal(cntxt, threshold, task, bid);
			xf[j]= task->hdr? task->factor: 0;
			if(xf[mx] < xf[j]) mx =j;
		}
		if(x >1){
			mnstr_printf(cntxt->fdout,"#all %d ",bid);
			for(j=0;j< top; j++)
				mnstr_printf(cntxt->fdout,"%-15s %9.5f ",properties[j], xf[j]);
			mnstr_printf(cntxt->fdout," BEST %-15s %9.5f\n",properties[mx], xf[mx] );
		}
		x=0;
	} else
    for (limit= getBBPsize(),i = 1; i < limit; i++){
		if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i)) ) 
			for( j = 0; j < top; j++){
				if( properties[j] && !strstr(properties[j],"mosaic"))
					for( k = 0; k< MOSAIC_METHODS; k++)
						task->filter[k]= strstr(properties[j],MOSfiltername[k]) != 0;
				else
					for( k = 0; k< MOSAIC_METHODS; k++)
						task->filter[k]= 1;
				x+= MOSanalyseInternal(cntxt, threshold, task, i);
			xf[j]= task->hdr? task->factor: 0;
		}
		if( x >1){
			mnstr_printf(cntxt->fdout,"#all %d ",i);
			mx =0;
			for(j=0;j< top; j++){
				mnstr_printf(cntxt->fdout,"%-15s %9.5f ",properties[j], xf[j]);
				if(xf[mx] < xf[j]) mx =j;
			}
			mnstr_printf(cntxt->fdout," BEST %-15s %9.5f\n",properties[mx], xf[mx] );
		}
		x=0;
	}
	GDKfree(task);
	return MAL_SUCCEED;
}

/*
 * Start searching for a proper compression scheme.
 */

#define STEP MOSAIC_METHODS
static int
MOSsetmix(float *xf, int idx, MOStask task){
	int i, j=0;
	for( i = 1; i < STEP; i++)
		task->filter[i] = 0;
	while(idx > 0) {
		if( idx % STEP ) { 
			if ( xf[idx % STEP] > 1.0  || xf[idx % STEP] == 0){
				if( idx % STEP < STEP -1) { /* ignore EOL */
					task->filter[idx % STEP] = 1;
					j++;
				}
			} else 
				return -1;
		}
		idx /= STEP;
	}
	return j ? 0: -1;
}

static int getPattern(MOStask task)
{	int i,p=0;
	for( i = 0; i< MOSAIC_METHODS-1; i++)
		p += 2 * p + task->filter[i];
	return p;
}

str
MOSoptimize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int ply = 1;
	MOStask task;
	int cases;
	int i, j, ret, idx =0, p;
	bat bid;
	int pattern[1024];
	float mx, xf[1024];

	(void) mb;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		throw(MAL, "mosaic.mosaic", MAL_MALLOC_FAIL);

	bid = *getArgReference_int(stk,pci,1);
	if( pci->argc > 2)
		ply = *getArgReference_int(stk,pci,2);
	MOSblocklimit = 100000;
	if( pci->argc > 3)
		MOSblocklimit  = *getArgReference_int(stk,pci,3) * 1000;

	cases = STEP;
	for ( i=ply-1; i > 0 && cases * STEP < 1024; i--)
		cases *= STEP;

	for( i = 0; i < 1024; i++)
		xf[i]=0;

	mx = 0;
	for( i = 1; i< cases; i++){
		if( MOSsetmix(xf,i,task) < 0)
			continue;
		p= getPattern(task);
		for( j=0; j < i; j++)
			if (pattern[j] == p) break;
		if( j<i ) continue;
		pattern[j]= p;
#define _DEBUG_MOSAIC_
#ifdef _DEBUG_MOSAIC_
		mnstr_printf(cntxt->fdout,"# %d\t%-8s\t", bid, BBP_physical(bid));
		for( j = 0; j < MOSAIC_METHODS -1; j++)
			mnstr_printf(cntxt->fdout,"%d",task->filter[j]?1:0);
		mnstr_printf(cntxt->fdout,"\t");
#else
		(void) cntxt;
#endif
		MOScompressInternal(cntxt, &ret, &bid, task, 0, TRUE);
		xf[i] = task->factor;
		if( ret != bid)
			BBPdecref(ret, TRUE);
		if( mx < task->factor){
			mx = task->factor;
			idx = i;
		}
		
	}
	mnstr_printf(cntxt->fdout,"#best strategy %d ",idx);
	(void) MOSsetmix(xf,idx,task);
	for( j = 0; j < MOSAIC_METHODS -1; j++)
	if( task->filter[j])
		mnstr_printf(cntxt->fdout,"%s ",MOSfiltername[j]);
	mnstr_printf(cntxt->fdout,"\t%9.5f\n",mx);
	GDKfree(task);
	
	return MAL_SUCCEED;
}

