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

str
MOSlayout_hdr(MOStask* task, MosaicLayout* layout) {
	str msg = MAL_SUCCEED;
	unsigned i;
	char buf[BUFSIZ];
	lng nil= lng_nil;
	const lng zero= 0;

	lng bsn = 0;
	str tech = "header";
	lng count; // TODO
	lng input; // TODO
	lng output; // TODO

	str properties = "{\"blk summary\":<blk summary TODO>}, \"elm summary\":<elm summary TODO>}";

	// safe the general properties
	snprintf(buf,BUFSIZ,"%g", (task->hdr)->ratio);
	if( (msg = layout_insert_record(
			layout,
			&bsn,
			&tech,
			&count, 
			&input,
			&output,
			&properties)) != MAL_SUCCEED)
			 return msg;

	/*
	for(i=0; i < MOSAIC_METHODS; i++){
		lng nil = 0;
		snprintf(buf,BUFSIZ,"%s blocks", MOSmethods[i].name);
		if( BUNappend(bbsn, &zero, false) != GDK_SUCCEED ||
			BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &(task->hdr)->blks[i], false) != GDK_SUCCEED ||
			BUNappend(binput, &(task->hdr)->elms[i], false) != GDK_SUCCEED ||
			BUNappend(boutput, &nil , false) != GDK_SUCCEED ||
			BUNappend(bproperties, "", false) != GDK_SUCCEED)
				throw(MAL,"mosaic.layout", MAL_MALLOC_FAIL);
	}
	*/


	if( (task->hdr)->blks[MOSAIC_DICT256] && (msg = MOSlayoutDictionary_ID(dict256)(task,layout)) != MAL_SUCCEED)
		return msg;

	if( (task->hdr)->blks[MOSAIC_DICT] && (msg = MOSlayoutDictionary_ID(dict)(task,layout)) != MAL_SUCCEED)
		return msg;

	return MAL_SUCCEED;
}
